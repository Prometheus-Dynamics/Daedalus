use serde_json::Value;

const CHECKED_IN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../docs/schema/daedalus.graph.v1.schema.json"
);

fn drift(path: &str, wanted: &Value, found: &Value, out: &mut Vec<String>) {
    match (wanted, found) {
        (Value::Object(a), Value::Object(b)) => {
            let names = a
                .keys()
                .chain(b.keys().filter(|name| !a.contains_key(*name)));
            for name in names {
                let at = format!("{path}/{name}");
                match (a.get(name), b.get(name)) {
                    (Some(x), Some(y)) => drift(&at, x, y, out),
                    _ => out.push(at),
                }
            }
        }
        _ if wanted != found => out.push(path.to_string()),
        _ => {}
    }
}

fn checked_in() -> Value {
    serde_json::from_str(&std::fs::read_to_string(CHECKED_IN).unwrap()).unwrap()
}

#[test]
fn the_checked_in_schema_matches_the_generator() {
    let mut found = Vec::new();
    drift(
        "",
        &daedalus_graph::json_schema(),
        &checked_in(),
        &mut found,
    );
    assert!(
        found.is_empty(),
        "run `cargo run -p daedalus-graph --bin graph_schema`: {found:?}"
    );
}

#[test]
fn schema_drift_catches_an_unlisted_field() {
    let generated = daedalus_graph::json_schema();
    let mut fixture = checked_in();
    let extra = serde_json::json!({ "type": "string" });
    fixture["$defs"]["node"]["properties"]["unlisted"] = extra;
    let mut found = Vec::new();
    drift("", &generated, &fixture, &mut found);
    assert_eq!(found, ["/$defs/node/properties/unlisted"]);
}
