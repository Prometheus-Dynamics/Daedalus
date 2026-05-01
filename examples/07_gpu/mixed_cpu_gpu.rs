use daedalus::transport::{Cpu, Gpu};

fn cpu_stage(frame: Cpu<Vec<u8>>) -> Cpu<Vec<u8>> {
    frame
}

fn gpu_stage(frame: Gpu<Vec<u8>>) -> Gpu<Vec<u8>> {
    frame
}

fn main() {
    let cpu = Cpu::new(vec![1_u8, 2, 3]);
    let gpu = Gpu::new(cpu_stage(cpu).into_inner());
    let cpu = Cpu::new(gpu_stage(gpu).into_inner());
    println!("mixed cpu/gpu bytes: {}", cpu.len());
}
