use super::*;
use crate::schema_package::{boundary_contract_fixture_spec, fixture_languages};
use daedalus_data::model::{EnumValue, StructFieldValue, TypeExpr, Value};
use daedalus_transport::{AccessMode, Layout, Payload, Residency, TypeKey};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;

mod fixtures;
mod invoke_protocol;
mod package;
mod wire_value;
