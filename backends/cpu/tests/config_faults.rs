//! `Registry::config_faults`: one call that checks every node's config against itself.

use daedalus_derive::{NodeConfig, Type};
use daedalus_macros::node;
use daedalus_registry::{NodeRecord, Registry};

#[derive(Clone, Type, NodeConfig)]
#[type_key("f:good")]
struct Good {
    #[port(default = 5, min = 1, max = 9)]
    level: u32,
}

impl Default for Good {
    fn default() -> Good {
        Good { level: 5 }
    }
}

/// Its `Default` says 0.2 where the declaration says 0.1: two copies that drifted.
#[derive(Clone, Type, NodeConfig)]
#[type_key("f:drifted")]
struct Drifted {
    #[port(default = 0.1, min = 0.000001)]
    tag_size_m: f64,
}

impl Default for Drifted {
    fn default() -> Drifted {
        Drifted { tag_size_m: 0.2 }
    }
}

#[node(id = "f.good", inputs(x, config = Good), outputs(out))]
fn good(x: i64, config: &Good) -> i64 {
    x + i64::from(config.level)
}

#[node(id = "f.drifted", inputs(x, config = Drifted), outputs(out))]
fn drifted(x: i64, config: &Drifted) -> i64 {
    x + config.tag_size_m as i64
}

#[node(id = "f.plain", inputs(x), outputs(out))]
fn plain(x: i64) -> i64 {
    x
}

fn registry(nodes: &[&'static NodeRecord]) -> Registry {
    let mut registry = Registry::new();
    registry.register_all(nodes).unwrap();
    registry
}

#[test]
fn a_registry_of_consistent_configs_has_no_faults() {
    assert_eq!(
        registry(&[&GOOD_NODE, &PLAIN_NODE]).config_faults(),
        Vec::<String>::new()
    );
}

#[test]
fn a_drifted_default_is_named_by_node_config_and_field_with_both_values() {
    assert_eq!(
        registry(&[&GOOD_NODE, &DRIFTED_NODE, &PLAIN_NODE]).config_faults(),
        [
            "node `f.drifted` config `config` field `tag_size_m`: declares the default 0.1, but \
          `Default` gives 0.2"
        ]
    );
}
