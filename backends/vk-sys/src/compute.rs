//! Compute pipelines made and bound with Vulkan itself, for [`Chain`](crate::chain::Chain),
//! which records many dispatches every tick and can't afford what a general encoder costs:
//! nothing here allocates once a pipeline and its sets exist.
//!
//! A [`ComputePipeline`] is built from WGSL, which is checked and translated here, so its
//! binding table is read from the shader and can't disagree with it. `Sets` is a small ring of
//! descriptor sets for one user of a pipeline, each remembered by the buffers it binds: binding
//! the same buffers again finds the set, and binding others rewrites the one used longest ago.

use std::ffi::CString;

use ash::vk;
use naga::back::spv;
use naga::proc::BoundsCheckPolicy;

use crate::Error;
use crate::device::Raw;

/// The most bindings a pipeline here has.
pub const MAX_BINDINGS: usize = 16;

/// How a binding's buffer is declared in the shader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Binding {
    /// `var<storage>`, read or written.
    Storage,
    /// `var<uniform>`.
    Uniform,
}

impl Binding {
    fn descriptor(self) -> vk::DescriptorType {
        match self {
            Binding::Storage => vk::DescriptorType::STORAGE_BUFFER,
            Binding::Uniform => vk::DescriptorType::UNIFORM_BUFFER,
        }
    }
}

/// One `@group(0) @binding(number)` of a pipeline's shader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    pub number: u32,
    pub kind: Binding,
    /// The fewest bytes that may be bound: the shader's type, with an array of no fixed length
    /// counted as one element.
    pub min_size: u64,
}

/// A compute pipeline: one entry point of a shader and the buffers it binds, all in group 0.
pub struct ComputePipeline {
    device: ash::Device,
    module: vk::ShaderModule,
    pub(crate) set_layout: vk::DescriptorSetLayout,
    layout: vk::PipelineLayout,
    pipeline: vk::Pipeline,
    slots: Box<[Slot]>,
    /// The most workgroups the device dispatches at once, in each dimension.
    pub(crate) max_counts: [u32; 3],
    /// Everything above is this device's.
    pub(crate) keep: wgpu::Device,
}

/// Translates `source` for its compute entry point `entry`, and reads its bindings. Anything
/// but storage and uniform buffers in group 0 is [`Error::Unsupported`].
fn translate(source: &str, entry: &str) -> Result<(Vec<u32>, Vec<Slot>), Error> {
    let module = naga::front::wgsl::parse_str(source).map_err(|_| Error::Unsupported)?;
    // No optional capability: a shader that needs one is left to wgpu, which knows the device.
    let info = naga::valid::Validator::new(
        naga::valid::ValidationFlags::all(),
        naga::valid::Capabilities::empty(),
    )
    .validate(&module)
    .map_err(|_| Error::Unsupported)?;
    let found = module
        .entry_points
        .iter()
        .any(|point| point.name == entry && point.stage == naga::ShaderStage::Compute);
    if !found {
        return Err(Error::Unsupported);
    }
    let mut slots = Vec::new();
    for (_, global) in module.global_variables.iter() {
        let Some(binding) = &global.binding else {
            continue;
        };
        let kind = match global.space {
            naga::AddressSpace::Storage { .. } => Binding::Storage,
            naga::AddressSpace::Uniform => Binding::Uniform,
            _ => return Err(Error::Unsupported),
        };
        if binding.group != 0 {
            return Err(Error::Unsupported);
        }
        let min_size = module.types[global.ty].inner.size(module.to_ctx());
        slots.push(Slot {
            number: binding.binding,
            kind,
            min_size: u64::from(min_size),
        });
    }
    slots.sort_by_key(|slot| slot.number);
    if slots.len() > MAX_BINDINGS {
        return Err(Error::Unsupported);
    }
    // As wgpu translates for Vulkan: nothing reads or writes outside what is bound.
    let options = spv::Options {
        bounds_check_policies: naga::proc::BoundsCheckPolicies {
            index: BoundsCheckPolicy::Restrict,
            buffer: BoundsCheckPolicy::Restrict,
            image_load: BoundsCheckPolicy::Restrict,
            binding_array: BoundsCheckPolicy::Restrict,
        },
        zero_initialize_workgroup_memory: spv::ZeroInitializeWorkgroupMemoryMode::Polyfill,
        // A binding keeps the group and number the shader gives it.
        fake_missing_bindings: true,
        ..spv::Options::default()
    };
    let pipeline = spv::PipelineOptions {
        shader_stage: naga::ShaderStage::Compute,
        entry_point: entry.into(),
    };
    let words = spv::write_vec(&module, &info, &options, Some(&pipeline))
        .map_err(|_| Error::Unsupported)?;
    Ok((words, slots))
}

impl ComputePipeline {
    /// Builds the pipeline for the compute entry point `entry` of the WGSL `source`. The
    /// shader may bind storage and uniform buffers in group 0 and nothing else, and may use no
    /// optional feature: otherwise this is [`Error::Unsupported`].
    pub fn from_wgsl(
        device: &wgpu::Device,
        source: &str,
        entry: &str,
    ) -> Result<ComputePipeline, Error> {
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        let (code, slots) = translate(source, entry)?;
        let name = CString::new(entry).map_err(|_| Error::Unsupported)?;
        let vk_device = raw.device;
        // SAFETY: a property query on the device's own physical device.
        let limits = unsafe { raw.instance.get_physical_device_properties(raw.physical) }.limits;
        let mut declared = [vk::DescriptorSetLayoutBinding::default(); MAX_BINDINGS];
        for (out, slot) in declared.iter_mut().zip(&slots) {
            *out = vk::DescriptorSetLayoutBinding::default()
                .binding(slot.number)
                .descriptor_type(slot.kind.descriptor())
                .descriptor_count(1)
                .stage_flags(vk::ShaderStageFlags::COMPUTE);
        }
        let declared = &declared[..slots.len()];
        // SAFETY: valid create infos on a live device, and SPIR-V naga wrote from a module it
        // validated. Whatever was made before a failure is destroyed before returning, and on
        // success everything is destroyed in `Drop`.
        unsafe {
            let module = vk_device
                .create_shader_module(&vk::ShaderModuleCreateInfo::default().code(&code), None)?;
            let set_layout = match vk_device.create_descriptor_set_layout(
                &vk::DescriptorSetLayoutCreateInfo::default().bindings(declared),
                None,
            ) {
                Ok(made) => made,
                Err(error) => {
                    vk_device.destroy_shader_module(module, None);
                    return Err(error.into());
                }
            };
            let set_layouts = [set_layout];
            let layout = match vk_device.create_pipeline_layout(
                &vk::PipelineLayoutCreateInfo::default().set_layouts(&set_layouts),
                None,
            ) {
                Ok(made) => made,
                Err(error) => {
                    vk_device.destroy_descriptor_set_layout(set_layout, None);
                    vk_device.destroy_shader_module(module, None);
                    return Err(error.into());
                }
            };
            let stage = vk::PipelineShaderStageCreateInfo::default()
                .stage(vk::ShaderStageFlags::COMPUTE)
                .module(module)
                .name(&name);
            let made = vk_device.create_compute_pipelines(
                vk::PipelineCache::null(),
                &[vk::ComputePipelineCreateInfo::default()
                    .stage(stage)
                    .layout(layout)],
                None,
            );
            let pipeline = match made {
                Ok(pipelines) => pipelines[0],
                Err((_, error)) => {
                    vk_device.destroy_pipeline_layout(layout, None);
                    vk_device.destroy_descriptor_set_layout(set_layout, None);
                    vk_device.destroy_shader_module(module, None);
                    return Err(error.into());
                }
            };
            Ok(ComputePipeline {
                device: vk_device,
                module,
                set_layout,
                layout,
                pipeline,
                slots: slots.into(),
                max_counts: limits.max_compute_work_group_count,
                keep: device.clone(),
            })
        }
    }

    /// The most workgroups the device dispatches at once, in each dimension.
    pub fn max_counts(&self) -> [u32; 3] {
        self.max_counts
    }

    /// The shader's bindings, by number.
    pub fn slots(&self) -> &[Slot] {
        &self.slots
    }

    /// Records one dispatch: the pipeline, `set`, and `counts` workgroups.
    ///
    /// # Safety
    ///
    /// `commands` is recording on this pipeline's device. `set` is from a `Sets` of this
    /// pipeline, and it, its buffers and this pipeline stay alive until the submit is done.
    /// `counts` are within the device's limit.
    // SAFETY: the contract is in the doc comment above.
    pub(crate) unsafe fn record(
        &self,
        commands: vk::CommandBuffer,
        set: vk::DescriptorSet,
        counts: [u32; 3],
    ) {
        // SAFETY: the caller's contract: a recording command buffer and live objects.
        unsafe {
            self.device
                .cmd_bind_pipeline(commands, vk::PipelineBindPoint::COMPUTE, self.pipeline);
            self.device.cmd_bind_descriptor_sets(
                commands,
                vk::PipelineBindPoint::COMPUTE,
                self.layout,
                0,
                &[set],
                &[],
            );
            self.device
                .cmd_dispatch(commands, counts[0], counts[1], counts[2]);
        }
    }
}

impl Drop for ComputePipeline {
    fn drop(&mut self) {
        // SAFETY: whoever recorded the pipeline waited for the submit, by the contract of
        // `record`. These were made in `from_wgsl` and are destroyed once.
        unsafe {
            self.device.destroy_pipeline(self.pipeline, None);
            self.device.destroy_pipeline_layout(self.layout, None);
            self.device
                .destroy_descriptor_set_layout(self.set_layout, None);
            self.device.destroy_shader_module(self.module, None);
        }
    }
}

struct Kept {
    set: vk::DescriptorSet,
    /// The buffers the set binds now, with the bytes of each that are bound. Holding them
    /// keeps what the set names alive.
    bound: [Option<(wgpu::Buffer, u64)>; MAX_BINDINGS],
    /// When the set was last asked for, for picking the oldest.
    used: u64,
}

/// Descriptor sets for one user of a pipeline: a few, each remembered by the buffers it binds.
pub(crate) struct Sets {
    device: ash::Device,
    pool: vk::DescriptorPool,
    kept: Box<[Kept]>,
    slots: Box<[Slot]>,
    clock: u64,
    _keep: wgpu::Device,
}

impl Sets {
    /// `count` sets of `pipeline`, at least one, none bound to anything yet.
    pub(crate) fn new(pipeline: &ComputePipeline, count: usize) -> Result<Sets, Error> {
        let count = count.max(1);
        let device = pipeline.device.clone();
        let of = |kind: Binding| {
            let each = pipeline
                .slots
                .iter()
                .filter(|slot| slot.kind == kind)
                .count();
            vk::DescriptorPoolSize::default()
                .ty(kind.descriptor())
                // A pool size of zero isn't allowed.
                .descriptor_count((each * count).max(1) as u32)
        };
        let sizes = [of(Binding::Storage), of(Binding::Uniform)];
        // SAFETY: valid create infos on a live device. The pool is destroyed on failure here
        // and otherwise in `Drop`, which frees its sets with it.
        unsafe {
            let pool = device.create_descriptor_pool(
                &vk::DescriptorPoolCreateInfo::default()
                    .max_sets(count as u32)
                    .pool_sizes(&sizes),
                None,
            )?;
            let mut kept = Vec::with_capacity(count);
            let layouts = [pipeline.set_layout];
            for _ in 0..count {
                let made = device.allocate_descriptor_sets(
                    &vk::DescriptorSetAllocateInfo::default()
                        .descriptor_pool(pool)
                        .set_layouts(&layouts),
                );
                match made {
                    Ok(sets) => kept.push(Kept {
                        set: sets[0],
                        bound: [const { None }; MAX_BINDINGS],
                        used: 0,
                    }),
                    Err(error) => {
                        device.destroy_descriptor_pool(pool, None);
                        return Err(error.into());
                    }
                }
            }
            Ok(Sets {
                device,
                pool,
                kept: kept.into(),
                slots: pipeline.slots.clone(),
                clock: 0,
                _keep: pipeline.keep.clone(),
            })
        }
    }

    /// The set that binds `buffers`, one for each binding by number and none of them `None`,
    /// each with the bytes of it to bind from its start. Found if exactly these were bound before, and otherwise the set
    /// asked for longest ago is rewritten. Finding one allocates nothing.
    ///
    /// # Safety
    ///
    /// Each buffer is this device's, has the Vulkan usage its binding needs and at least the
    /// bytes given. No submit that isn't done uses a set of this `Sets`: one that is rewritten
    /// must not be in use.
    // SAFETY: the contract is in the doc comment above.
    pub(crate) unsafe fn bind(
        &mut self,
        buffers: &[Option<(&wgpu::Buffer, u64)>],
    ) -> Result<vk::DescriptorSet, Error> {
        if buffers.len() != self.slots.len() || buffers.iter().any(Option::is_none) {
            return Err(Error::Range);
        }
        self.clock += 1;
        let same = |kept: &Kept| {
            kept.bound.iter().zip(buffers).all(|(have, want)| {
                matches!((have, want), (Some((buffer, size)), Some(want))
                    if buffer == want.0 && *size == want.1)
            })
        };
        if let Some(found) = self.kept.iter_mut().find(|kept| same(kept)) {
            found.used = self.clock;
            return Ok(found.set);
        }
        let oldest = self
            .kept
            .iter_mut()
            .min_by_key(|kept| kept.used)
            .ok_or(Error::Range)?;
        let mut infos = [vk::DescriptorBufferInfo::default(); MAX_BINDINGS];
        for (info, (buffer, size)) in infos.iter_mut().zip(buffers.iter().flatten()) {
            let handle = crate::memory::handle(buffer).ok_or(Error::NotVulkan)?;
            *info = vk::DescriptorBufferInfo::default()
                .buffer(handle)
                .range(*size);
        }
        let mut writes = [vk::WriteDescriptorSet::default(); MAX_BINDINGS];
        for (at, (write, slot)) in writes.iter_mut().zip(&self.slots).enumerate() {
            *write = vk::WriteDescriptorSet::default()
                .dst_set(oldest.set)
                .dst_binding(slot.number)
                .descriptor_type(slot.kind.descriptor())
                .buffer_info(std::slice::from_ref(&infos[at]));
        }
        // SAFETY: the caller's contract: live buffers of this device, and a set no pending
        // submit uses.
        unsafe {
            self.device
                .update_descriptor_sets(&writes[..buffers.len()], &[]);
        }
        for (have, (buffer, size)) in oldest.bound.iter_mut().zip(buffers.iter().flatten()) {
            *have = Some(((*buffer).clone(), *size));
        }
        oldest.used = self.clock;
        Ok(oldest.set)
    }
}

impl Drop for Sets {
    fn drop(&mut self) {
        // SAFETY: the owner waited for every submit that used a set, by the contract of `bind`.
        // Destroying the pool frees its sets.
        unsafe { self.device.destroy_descriptor_pool(self.pool, None) };
    }
}
