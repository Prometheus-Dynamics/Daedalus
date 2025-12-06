use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use image::{DynamicImage, GrayAlphaImage, GrayImage, ImageBuffer, Luma, LumaA, Rgb, RgbImage, Rgba, RgbaImage};

/// Runtime conversion registry for common CPU-side types.
///
/// This mirrors the `GpuSendable` story: users can register additional conversions
/// at runtime if needed, while we ship a set of defaults (numeric widenings, image casts).
type ConvertFn =
    Box<dyn Fn(&Arc<dyn Any + Send + Sync>) -> Option<Box<dyn Any + Send + Sync>> + Send + Sync>;

pub struct ConversionRegistry {
    inner: HashMap<(TypeId, TypeId), ConvertFn>,
    resolved_paths: HashMap<(TypeId, TypeId), Option<Vec<TypeId>>>,
}

impl ConversionRegistry {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            resolved_paths: HashMap::new(),
        }
    }

    pub fn register<S: Any + Send + Sync + 'static, T: Any + Send + Sync + 'static>(
        &mut self,
        f: fn(&S) -> Option<T>,
    ) -> &mut Self {
        let key = (TypeId::of::<S>(), TypeId::of::<T>());
        self.inner.insert(
            key,
            Box::new(move |a| {
                a.downcast_ref::<S>()
                    .and_then(|s| f(s).map(|t| Box::new(t) as Box<dyn Any + Send + Sync>))
            }),
        );
        self.resolved_paths.clear();
        self
    }

    fn convert_boxed(
        &self,
        a: &Arc<dyn Any + Send + Sync>,
        to: TypeId,
    ) -> Option<Box<dyn Any + Send + Sync>> {
        let from = a.as_ref().type_id();
        self.inner.get(&(from, to)).and_then(|f| f(a))
    }

    fn resolve_path(&mut self, from: TypeId, to: TypeId) -> Option<Vec<TypeId>> {
        if let Some(cached) = self.resolved_paths.get(&(from, to)) {
            return cached.clone();
        }

        if from == to {
            self.resolved_paths.insert((from, to), Some(vec![from]));
            return Some(vec![from]);
        }

        // BFS over available conversions (unweighted; good enough for now).
        let mut queue: std::collections::VecDeque<TypeId> = std::collections::VecDeque::new();
        let mut prev: HashMap<TypeId, TypeId> = HashMap::new();
        queue.push_back(from);
        prev.insert(from, from);

        while let Some(cur) = queue.pop_front() {
            if cur == to {
                break;
            }
            for &(src, dst) in self.inner.keys() {
                if src != cur {
                    continue;
                }
                if prev.contains_key(&dst) {
                    continue;
                }
                prev.insert(dst, cur);
                queue.push_back(dst);
            }
        }

        if !prev.contains_key(&to) {
            self.resolved_paths.insert((from, to), None);
            return None;
        }

        let mut path: Vec<TypeId> = Vec::new();
        let mut cur = to;
        loop {
            path.push(cur);
            let p = *prev.get(&cur).unwrap();
            if p == cur {
                break;
            }
            cur = p;
        }
        path.reverse();
        self.resolved_paths.insert((from, to), Some(path.clone()));
        Some(path)
    }

    pub fn convert_to<T: Any + Clone + Send + Sync + 'static>(
        &mut self,
        a: &Arc<dyn Any + Send + Sync>,
    ) -> Option<T> {
        let from = a.as_ref().type_id();
        let to = TypeId::of::<T>();

        // Fast path: direct conversion.
        if let Some(b) = self
            .convert_boxed(a, to)
            .and_then(|b| b.downcast::<T>().ok())
        {
            return Some((*b).clone());
        }

        // Chained conversion.
        let path = self.resolve_path(from, to)?;
        if path.len() < 2 {
            return None;
        }

        let mut cur: Arc<dyn Any + Send + Sync> = a.clone();
        for win in path.windows(2) {
            let from = win[0];
            let to = win[1];
            if cur.as_ref().type_id() != from {
                return None;
            }
            let boxed = self.convert_boxed(&cur, to)?;
            cur = Arc::from(boxed);
        }

        cur.downcast_ref::<T>().cloned()
    }
}

impl Default for ConversionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global registry with common conversions.
fn default_registry() -> &'static Mutex<ConversionRegistry> {
    static REG: OnceLock<Mutex<ConversionRegistry>> = OnceLock::new();
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

        Mutex::new(reg)
    })
}

/// Attempt to convert an `Arc<dyn Any>` into `T` using the default registry.
pub fn convert_arc<T: Any + Clone + Send + Sync + 'static>(
    a: &Arc<dyn Any + Send + Sync>,
) -> Option<T> {
    default_registry()
        .lock()
        .ok()
        .and_then(|mut r| r.convert_to::<T>(a))
}

/// Allow callers (including plugins) to extend the global conversion registry.
#[allow(dead_code)]
pub fn register_conversion<S, T>(f: fn(&S) -> Option<T>)
where
    S: Any + Send + Sync + 'static,
    T: Any + Send + Sync + 'static,
{
    if let Ok(mut reg) = default_registry().lock() {
        reg.register::<S, T>(f);
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
