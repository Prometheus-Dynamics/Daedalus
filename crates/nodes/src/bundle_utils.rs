use daedalus_runtime::NodeError;

use crate::NodeDecl;
use crate::node;

#[node(id = "utils.identity", inputs("value"), outputs("value"))]
fn identity(value: i32) -> Result<i32, NodeError> {
    Ok(value)
}

#[node(id = "utils.add", inputs("a", "b"), outputs("out"))]
fn add(a: i32, b: i32) -> Result<i32, NodeError> {
    checked_sum(a, b)
}

/// The sum; an overflowing sum is an error (`InvalidInput`), not a wrap or a panic.
fn checked_sum(a: i32, b: i32) -> Result<i32, NodeError> {
    a.checked_add(b)
        .ok_or_else(|| NodeError::InvalidInput(format!("utils.add: {a} + {b} overflows i32")))
}

pub fn nodes() -> Vec<NodeDecl> {
    vec![
        IdentityNode::node_decl().expect("utils.identity node declaration"),
        AddNode::node_decl().expect("utils.add node declaration"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_reports_overflow_as_an_error() {
        assert!(matches!(checked_sum(2, 3), Ok(5)));
        assert!(matches!(checked_sum(i32::MAX, 0), Ok(i32::MAX)));
        assert!(matches!(
            checked_sum(i32::MAX, 1),
            Err(NodeError::InvalidInput(message)) if message.contains("overflows")
        ));
        assert!(matches!(
            checked_sum(i32::MIN, -1),
            Err(NodeError::InvalidInput(_))
        ));
    }
}
