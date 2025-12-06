#![allow(clippy::field_reassign_with_default)]

use daedalus_engine::{Engine, EngineConfig, GpuBackend, RuntimeMode};

#[test]
fn pool_size_zero_is_rejected() {
    let mut cfg = EngineConfig::default();
    cfg.runtime.pool_size = Some(0);
    let err = Engine::new(cfg).err().expect("expected config error");
    let msg = format!("{}", err);
    assert!(msg.contains("pool_size must be > 0"), "{msg}");
}

#[test]
fn gpu_requires_feature() {
    let mut cfg = EngineConfig::default();
    cfg.gpu = GpuBackend::Device;
    let res = Engine::new(cfg);
    #[cfg(not(feature = "gpu"))]
    {
        let err = res.err().expect("expected config error");
        let msg = format!("{}", err);
        assert!(msg.contains("feature 'gpu' is disabled"));
    }
    #[cfg(feature = "gpu")]
    {
        assert!(
            res.is_ok(),
            "expected GPU config to succeed when feature `gpu` is enabled"
        );
    }
}

#[test]
fn lockfree_requires_feature() {
    let mut cfg = EngineConfig::default();
    cfg.runtime.mode = RuntimeMode::Parallel;
    cfg.runtime.lockfree_queues = true;
    let res = Engine::new(cfg);
    #[cfg(not(feature = "lockfree-queues"))]
    {
        let err = res.err().expect("expected config error");
        let msg = format!("{}", err);
        assert!(
            msg.contains("feature 'lockfree-queues' is disabled"),
            "{msg}"
        );
    }
    #[cfg(feature = "lockfree-queues")]
    {
        assert!(res.is_ok());
    }
}
