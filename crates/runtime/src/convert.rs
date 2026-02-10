use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock, RwLock,
};

use image::{DynamicImage, GrayAlphaImage, GrayImage, ImageBuffer, Luma, LumaA, Rgb, RgbImage, Rgba, RgbaImage};

/// Runtime conversion registry for common CPU-side types.
///
/// This mirrors the `GpuSendable` story: users can register additional conversions
/// at runtime if needed, while we ship a set of defaults (numeric widenings, image casts).
type AnyArc = Arc<dyn Any + Send + Sync>;
type ConvertFn = Arc<dyn Fn(&AnyArc) -> Option<AnyArc> + Send + Sync>;

pub struct ConversionRegistry {
    inner: HashMap<(TypeId, TypeId), ConvertFn>,
}

impl ConversionRegistry {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn register<S: Any + Send + Sync + 'static, T: Any + Send + Sync + 'static>(
        &mut self,
        f: fn(&S) -> Option<T>,
    ) -> &mut Self {
        let key = (TypeId::of::<S>(), TypeId::of::<T>());
        self.inner.insert(
            key,
            Arc::new(move |a: &AnyArc| {
                a.downcast_ref::<S>().and_then(|s| {
                    f(s).map(|t| {
                        let out: AnyArc = Arc::new(t);
                        out
                    })
                })
            }),
        );
        self
    }

    fn convert_direct(&self, a: &AnyArc, to: TypeId) -> Option<AnyArc> {
        let from = a.as_ref().type_id();
        self.inner.get(&(from, to)).and_then(|f| f(a))
    }

    fn resolve_program(&self, from: TypeId, to: TypeId) -> Option<Arc<[ConvertFn]>> {
        // BFS over available conversions (unweighted; good enough for now).
        let mut queue: std::collections::VecDeque<TypeId> = std::collections::VecDeque::new();
        // prev[dst] = (src, converter_used_for_src_to_dst)
        let mut prev: HashMap<TypeId, (TypeId, ConvertFn)> = HashMap::new();
        queue.push_back(from);
        // Seed with a sentinel; converter unused for the root.
        prev.insert(from, (from, Arc::new(|_| None)));

        while let Some(cur) = queue.pop_front() {
            if cur == to {
                break;
            }
            for (&(src, dst), conv) in &self.inner {
                if src != cur {
                    continue;
                }
                if prev.contains_key(&dst) {
                    continue;
                }
                prev.insert(dst, (cur, conv.clone()));
                queue.push_back(dst);
            }
        }

        if !prev.contains_key(&to) {
            return None;
        }

        let mut steps: Vec<ConvertFn> = Vec::new();
        let mut cur = to;
        while cur != from {
            let (p, conv) = prev.get(&cur).cloned()?;
            steps.push(conv);
            cur = p;
        }
        steps.reverse();

        let program: Arc<[ConvertFn]> = Arc::from(steps);
        Some(program)
    }

    fn convert_arc_any(
        &self,
        a: &AnyArc,
        to: TypeId,
        program_cache: &mut ProgramCache,
    ) -> Option<AnyArc> {
        let from = a.as_ref().type_id();
        if from == to {
            return Some(a.clone());
        }

        // Fast path: direct conversion.
        if let Some(out) = self.convert_direct(a, to) {
            return Some(out);
        }

        // Cached conversion chain (thread-local).
        let program = program_cache.program_for(self, from, to)?;
        let mut cur: AnyArc = a.clone();
        for step in program.iter() {
            cur = step(&cur)?;
        }
        Some(cur)
    }

    pub fn convert_to<T: Any + Clone + Send + Sync + 'static>(&self, a: &AnyArc) -> Option<T> {
        let to = TypeId::of::<T>();

        if let Some(v) = a.downcast_ref::<T>() {
            return Some(v.clone());
        }

        // Local registry instance: resolve programs without TLS/versioning.
        let mut cache = ProgramCache::default();
        self.convert_arc_any(a, to, &mut cache)
            .and_then(|out| out.downcast_ref::<T>().cloned())
    }
}

impl Default for ConversionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

struct RegistryState {
    version: AtomicU64,
    reg: RwLock<ConversionRegistry>,
}

/// Global registry with common conversions.
fn default_registry() -> &'static RegistryState {
    static REG: OnceLock<RegistryState> = OnceLock::new();
    REG.get_or_init(|| {
        let mut reg = ConversionRegistry::new();
        // Numeric widenings (lossless).
        reg.register::<i8, i16>(|v| Some(*v as i16))
            .register::<i8, i32>(|v| Some(*v as i32))
            .register::<i8, i64>(|v| Some(*v as i64))
            .register::<i8, f64>(|v| Some(*v as f64))
            .register::<i16, i32>(|v| Some(*v as i32))
            .register::<i16, i64>(|v| Some(*v as i64))
            .register::<i16, f64>(|v| Some(*v as f64))
            .register::<i32, i64>(|v| Some(*v as i64))
            .register::<i32, f64>(|v| Some(*v as f64))
            .register::<i64, f64>(|v| Some(*v as f64))
            .register::<u8, u16>(|v| Some(*v as u16))
            .register::<u8, u32>(|v| Some(*v as u32))
            .register::<u8, u64>(|v| Some(*v as u64))
            .register::<u8, f64>(|v| Some(*v as f64))
            .register::<u16, u32>(|v| Some(*v as u32))
            .register::<u16, u64>(|v| Some(*v as u64))
            .register::<u16, f64>(|v| Some(*v as f64))
            .register::<u32, u64>(|v| Some(*v as u64))
            .register::<u32, f64>(|v| Some(*v as f64))
            .register::<f32, f64>(|v| Some(*v as f64))
            .register::<bool, u8>(|v| Some(if *v { 1 } else { 0 }))
            .register::<u8, bool>(|v| Some(*v != 0));

        // Common image -> DynamicImage conversions.
        reg.register::<DynamicImage, DynamicImage>(|img| Some(img.clone()))
            .register::<RgbaImage, DynamicImage>(|img| Some(DynamicImage::ImageRgba8(img.clone())))
            .register::<RgbImage, DynamicImage>(|img| Some(DynamicImage::ImageRgb8(img.clone())))
            .register::<GrayImage, DynamicImage>(|img| Some(DynamicImage::ImageLuma8(img.clone())))
            .register::<GrayAlphaImage, DynamicImage>(|img| {
                Some(DynamicImage::ImageLumaA8(img.clone()))
            })
            .register::<ImageBuffer<Rgba<u8>, Vec<u8>>, DynamicImage>(|img| {
                Some(DynamicImage::ImageRgba8(img.clone()))
            })
            .register::<ImageBuffer<Rgb<u8>, Vec<u8>>, DynamicImage>(|img| {
                Some(DynamicImage::ImageRgb8(img.clone()))
            })
            .register::<ImageBuffer<Luma<u8>, Vec<u8>>, DynamicImage>(|img| {
                Some(DynamicImage::ImageLuma8(img.clone()))
            })
            .register::<ImageBuffer<LumaA<u8>, Vec<u8>>, DynamicImage>(|img| {
                Some(DynamicImage::ImageLumaA8(img.clone()))
            });
        RegistryState {
            version: AtomicU64::new(1),
            reg: RwLock::new(reg),
        }
    })
}

#[derive(Default)]
struct ProgramCache {
    version: u64,
    programs: HashMap<(TypeId, TypeId), Option<Arc<[ConvertFn]>>>,
}

impl ProgramCache {
    fn sync_version(&mut self, version: u64) {
        if self.version != version {
            self.version = version;
            self.programs.clear();
        }
    }

    fn program_for(
        &mut self,
        reg: &ConversionRegistry,
        from: TypeId,
        to: TypeId,
    ) -> Option<Arc<[ConvertFn]>> {
        if let Some(cached) = self.programs.get(&(from, to)) {
            return cached.clone();
        }
        if from == to {
            let program: Arc<[ConvertFn]> = Arc::from([]);
            self.programs.insert((from, to), Some(program.clone()));
            return Some(program);
        }
        let resolved = reg.resolve_program(from, to);
        self.programs.insert((from, to), resolved.clone());
        resolved
    }
}

thread_local! {
    static TLS_PROGRAM_CACHE: std::cell::RefCell<ProgramCache> = std::cell::RefCell::new(ProgramCache::default());
}

fn with_tls_program_cache<R>(f: impl FnOnce(&mut ProgramCache) -> R) -> R {
    TLS_PROGRAM_CACHE.with(|cell| {
        let mut guard = cell.borrow_mut();
        f(&mut guard)
    })
}

/// Attempt to convert an `Arc<dyn Any>` into `T` using the default registry.
pub fn convert_arc<T: Any + Clone + Send + Sync + 'static>(
    a: &Arc<dyn Any + Send + Sync>,
) -> Option<T> {
    let state = default_registry();
    let version = state.version.load(Ordering::Relaxed);
    let reg_guard = state.reg.read().ok()?;
    with_tls_program_cache(|cache| {
        cache.sync_version(version);
        reg_guard
            .convert_arc_any(a, TypeId::of::<T>(), cache)
            .and_then(|out| out.downcast_ref::<T>().cloned())
    })
}

/// Convert an `Arc<dyn Any>` to a target type id, returning an `Arc<dyn Any>`.
///
/// This is the "zero-copy when possible" conversion entry point: it preserves sharing by
/// returning an `Arc`, enabling fanout dedupe via caches at the executor/IO layer.
pub fn convert_any_arc(
    a: &Arc<dyn Any + Send + Sync>,
    to: TypeId,
) -> Option<Arc<dyn Any + Send + Sync>> {
    let state = default_registry();
    let version = state.version.load(Ordering::Relaxed);
    let reg_guard = state.reg.read().ok()?;
    with_tls_program_cache(|cache| {
        cache.sync_version(version);
        reg_guard.convert_arc_any(a, to, cache)
    })
}

/// Convert an `Arc<dyn Any>` to `Arc<T>` using the default registry.
pub fn convert_to_arc<T: Any + Send + Sync + 'static>(
    a: &Arc<dyn Any + Send + Sync>,
) -> Option<Arc<T>> {
    if let Ok(arc) = Arc::downcast::<T>(a.clone()) {
        return Some(arc);
    }
    let out = convert_any_arc(a, TypeId::of::<T>())?;
    Arc::downcast::<T>(out).ok()
}

/// Allow callers (including plugins) to extend the global conversion registry.
#[allow(dead_code)]
pub fn register_conversion<S, T>(f: fn(&S) -> Option<T>)
where
    S: Any + Send + Sync + 'static,
    T: Any + Send + Sync + 'static,
{
    let state = default_registry();
    if let Ok(mut reg) = state.reg.write() {
        reg.register::<S, T>(f);
        state.version.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct A(u8);
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct B(u16);
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct C(u32);

    #[test]
    fn chained_conversion_resolves() {
        register_conversion::<A, B>(|a| Some(B(a.0 as u16)));
        register_conversion::<B, C>(|b| Some(C(b.0 as u32)));

        let a: Arc<dyn Any + Send + Sync> = Arc::new(A(7));
        let out: Option<C> = convert_arc::<C>(&a);
        assert_eq!(out, Some(C(7)));
    }
}
