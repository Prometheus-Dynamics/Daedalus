use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{
    Arc, OnceLock, RwLock,
    atomic::{AtomicU64, Ordering},
};

use image::{
    DynamicImage, GrayAlphaImage, GrayImage, ImageBuffer, Luma, LumaA, Rgb, RgbImage, Rgba,
    RgbaImage,
};

/// Runtime conversion registry for common CPU-side types.
///
/// This mirrors the `DeviceBridge` story: users can register additional conversions
/// at runtime if needed, while we ship a set of defaults (numeric widenings, image casts).
type AnyArc = Arc<dyn Any + Send + Sync>;
type ConvertFn = Arc<dyn Fn(&AnyArc) -> Option<AnyArc> + Send + Sync>;
type ProgramKey = (TypeId, TypeId);
type Program = Arc<[ConvertEdge]>;
type ProgramCache = HashMap<ProgramKey, Option<Program>>;

#[derive(Clone)]
struct ConvertEdge {
    convert: ConvertFn,
    from_rust: &'static str,
    to_rust: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConversionStep {
    pub from_rust: String,
    pub to_rust: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeConversionResolution {
    pub steps: Vec<RuntimeConversionStep>,
}

pub struct ConversionRegistry {
    inner: HashMap<(TypeId, TypeId), ConvertEdge>,
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
            ConvertEdge {
                convert: Arc::new(move |a: &AnyArc| {
                    a.downcast_ref::<S>().and_then(|s| {
                        f(s).map(|t| {
                            let out: AnyArc = Arc::new(t);
                            out
                        })
                    })
                }),
                from_rust: std::any::type_name::<S>(),
                to_rust: std::any::type_name::<T>(),
            },
        );
        self
    }

    fn convert_direct(&self, a: &AnyArc, to: TypeId) -> Option<AnyArc> {
        let from = a.as_ref().type_id();
        self.inner
            .get(&(from, to))
            .and_then(|edge| (edge.convert)(a))
    }

    fn resolve_program(&self, from: TypeId, to: TypeId) -> Option<Program> {
        // BFS over available conversions (unweighted; good enough for now).
        let mut queue: std::collections::VecDeque<TypeId> = std::collections::VecDeque::new();
        // prev[dst] = (src, converter_used_for_src_to_dst)
        let mut prev: HashMap<TypeId, (TypeId, ConvertEdge)> = HashMap::new();
        queue.push_back(from);
        // Seed with a sentinel; converter unused for the root.
        prev.insert(
            from,
            (
                from,
                ConvertEdge {
                    convert: Arc::new(|_| None),
                    from_rust: "",
                    to_rust: "",
                },
            ),
        );

        while let Some(cur) = queue.pop_front() {
            if cur == to {
                break;
            }
            for (&(src, dst), edge) in &self.inner {
                if src != cur {
                    continue;
                }
                if prev.contains_key(&dst) {
                    continue;
                }
                prev.insert(dst, (cur, edge.clone()));
                queue.push_back(dst);
            }
        }

        if !prev.contains_key(&to) {
            return None;
        }

        let mut steps: Vec<ConvertEdge> = Vec::new();
        let mut cur = to;
        while cur != from {
            let (p, edge) = prev.get(&cur).cloned()?;
            steps.push(edge);
            cur = p;
        }
        steps.reverse();

        Some(Arc::from(steps))
    }

    fn convert_arc_any(&self, a: &AnyArc, to: TypeId) -> Option<AnyArc> {
        let from = a.as_ref().type_id();
        if from == to {
            return Some(a.clone());
        }

        // Fast path: direct conversion.
        if let Some(out) = self.convert_direct(a, to) {
            return Some(out);
        }

        // Local conversion chain resolution (no global cache; this is not the hot path).
        let program = self.resolve_program(from, to)?;
        let mut cur: AnyArc = a.clone();
        for step in program.iter() {
            cur = (step.convert)(&cur)?;
        }
        Some(cur)
    }

    pub fn convert_to<T: Any + Clone + Send + Sync + 'static>(&self, a: &AnyArc) -> Option<T> {
        let to = TypeId::of::<T>();

        if let Some(v) = a.downcast_ref::<T>() {
            return Some(v.clone());
        }

        self.convert_arc_any(a, to)
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
    programs: RwLock<ProgramCache>,
    programs_version: AtomicU64,
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
            programs: RwLock::new(ProgramCache::new()),
            programs_version: AtomicU64::new(1),
        }
    })
}

fn with_program_cached<R>(from: TypeId, to: TypeId, f: impl FnOnce(&Program) -> R) -> Option<R> {
    let state = default_registry();
    let reg_version = state.version.load(Ordering::Relaxed);
    let prog_version = state.programs_version.load(Ordering::Relaxed);
    if reg_version != prog_version {
        if let Ok(mut guard) = state.programs.write() {
            guard.clear();
        }
        state.programs_version.store(reg_version, Ordering::Relaxed);
    }

    if let Ok(guard) = state.programs.read()
        && let Some(hit) = guard.get(&(from, to))
    {
        return hit.as_ref().map(f);
    }

    let reg_guard = state.reg.read().ok()?;
    let resolved: Option<Program> = if from == to {
        Some(Arc::from(Vec::<ConvertEdge>::new()))
    } else {
        reg_guard.resolve_program(from, to)
    };

    if let Ok(mut guard) = state.programs.write() {
        guard.insert((from, to), resolved.clone());
    }
    resolved.as_ref().map(f)
}

fn resolution_from_program(program: &Program) -> RuntimeConversionResolution {
    RuntimeConversionResolution {
        steps: program
            .iter()
            .map(|step| RuntimeConversionStep {
                from_rust: step.from_rust.to_string(),
                to_rust: step.to_rust.to_string(),
            })
            .collect(),
    }
}

#[cfg(feature = "gpu")]
pub fn explain_conversion_from_types<S, T>() -> Option<RuntimeConversionResolution>
where
    S: Any + Send + Sync + 'static,
    T: Any + Send + Sync + 'static,
{
    explain_conversion_by_ids(TypeId::of::<S>(), TypeId::of::<T>())
}

pub fn explain_conversion_to<T>(
    a: &Arc<dyn Any + Send + Sync>,
) -> Option<RuntimeConversionResolution>
where
    T: Any + Send + Sync + 'static,
{
    explain_conversion_by_ids(a.as_ref().type_id(), TypeId::of::<T>())
}

pub fn explain_conversion_by_ids(from: TypeId, to: TypeId) -> Option<RuntimeConversionResolution> {
    with_program_cached(from, to, resolution_from_program)
}

/// Attempt to convert an `Arc<dyn Any>` into `T` using the default registry.
pub fn convert_arc<T: Any + Clone + Send + Sync + 'static>(
    a: &Arc<dyn Any + Send + Sync>,
) -> Option<T> {
    let to = TypeId::of::<T>();
    if let Some(v) = a.downcast_ref::<T>() {
        return Some(v.clone());
    }
    let from = a.as_ref().type_id();
    with_program_cached(from, to, |program| {
        let mut cur: Arc<dyn Any + Send + Sync> = a.clone();
        for step in program.iter() {
            cur = (step.convert)(&cur)?;
        }
        cur.downcast_ref::<T>().cloned()
    })?
}

/// Convert an `Arc<dyn Any>` to a target type id, returning an `Arc<dyn Any>`.
///
/// This is the "zero-copy when possible" conversion entry point: it preserves sharing by
/// returning an `Arc`, enabling fanout dedupe via caches at the executor/IO layer.
pub fn convert_any_arc(
    a: &Arc<dyn Any + Send + Sync>,
    to: TypeId,
) -> Option<Arc<dyn Any + Send + Sync>> {
    let from = a.as_ref().type_id();
    if from == to {
        return Some(a.clone());
    }
    // Fast path: direct conversion.
    let state = default_registry();
    if let Ok(reg_guard) = state.reg.read()
        && let Some(out) = reg_guard.convert_direct(a, to)
    {
        return Some(out);
    }

    with_program_cached(from, to, |program| {
        let mut cur: Arc<dyn Any + Send + Sync> = a.clone();
        for step in program.iter() {
            cur = (step.convert)(&cur)?;
        }
        Some(cur)
    })?
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
        // Clear the global program cache on next use.
        state.programs_version.fetch_add(1, Ordering::Relaxed);
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

    #[test]
    fn explain_conversion_reports_runtime_steps() {
        register_conversion::<A, B>(|a| Some(B(a.0 as u16)));
        register_conversion::<B, C>(|b| Some(C(b.0 as u32)));

        let a: Arc<dyn Any + Send + Sync> = Arc::new(A(7));
        let resolution = explain_conversion_to::<C>(&a).expect("runtime conversion path");
        assert_eq!(resolution.steps.len(), 2);
        assert_eq!(resolution.steps[0].from_rust, std::any::type_name::<A>());
        assert_eq!(resolution.steps[0].to_rust, std::any::type_name::<B>());
        assert_eq!(resolution.steps[1].from_rust, std::any::type_name::<B>());
        assert_eq!(resolution.steps[1].to_rust, std::any::type_name::<C>());
    }
}
