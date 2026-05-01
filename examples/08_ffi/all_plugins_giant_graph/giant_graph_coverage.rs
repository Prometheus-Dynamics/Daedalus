#![allow(dead_code)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GiantGraphCoverageSummary {
    pub plugin_packages: usize,
    pub node_count: usize,
    pub edge_count: usize,
    pub languages: BTreeMap<String, GiantGraphLanguageCoverage>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GiantGraphLanguageCoverage {
    pub node_count: usize,
    pub adapter_nodes: u64,
    pub gpu_nodes: u64,
    pub stateful_nodes: u64,
    pub zero_copy_nodes: u64,
    pub shared_reference_nodes: u64,
    pub cow_nodes: u64,
    pub mutable_nodes: u64,
    pub owned_nodes: u64,
    pub typed_error_nodes: u64,
    pub raw_events: u64,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GiantGraphCoverageError {
    #[error("missing coverage for language `{0}`")]
    MissingLanguage(String),
    #[error("language `{language}` reported {actual} nodes, expected at least {expected}")]
    LanguageNodeCount {
        language: String,
        expected: usize,
        actual: usize,
    },
    #[error("overall graph reported {actual} nodes, expected at least {expected}")]
    OverallNodeCount { expected: usize, actual: usize },
    #[error("overall graph reported {actual} edges, expected at least {expected}")]
    OverallEdgeCount { expected: usize, actual: usize },
    #[error("language `{language}` is missing required feature coverage `{feature}`")]
    MissingFeature { language: String, feature: String },
}

impl GiantGraphCoverageSummary {
    pub fn with_structure(
        mut self,
        plugin_packages: usize,
        node_count: usize,
        edge_count: usize,
    ) -> Self {
        self.plugin_packages = plugin_packages;
        self.node_count = node_count;
        self.edge_count = edge_count;
        self
    }

    pub fn record_language(
        &mut self,
        language: impl Into<String>,
        coverage: GiantGraphLanguageCoverage,
    ) {
        self.languages.insert(language.into(), coverage);
    }

    pub fn total_gpu_nodes(&self) -> u64 {
        self.languages
            .values()
            .map(|language| language.gpu_nodes)
            .sum()
    }

    pub fn total_payload_mode_nodes(&self) -> u64 {
        self.languages
            .values()
            .map(|language| {
                language.zero_copy_nodes
                    + language.shared_reference_nodes
                    + language.cow_nodes
                    + language.mutable_nodes
                    + language.owned_nodes
            })
            .sum()
    }

    pub fn validate(
        &self,
        expected_languages: &[&str],
        expected_nodes_per_language: usize,
        expected_edges: usize,
    ) -> Result<(), GiantGraphCoverageError> {
        let expected_total_nodes = expected_languages.len() * expected_nodes_per_language;
        if self.node_count < expected_total_nodes {
            return Err(GiantGraphCoverageError::OverallNodeCount {
                expected: expected_total_nodes,
                actual: self.node_count,
            });
        }
        if self.edge_count < expected_edges {
            return Err(GiantGraphCoverageError::OverallEdgeCount {
                expected: expected_edges,
                actual: self.edge_count,
            });
        }
        for language in expected_languages {
            let coverage = self
                .languages
                .get(*language)
                .ok_or_else(|| GiantGraphCoverageError::MissingLanguage((*language).into()))?;
            if coverage.node_count < expected_nodes_per_language {
                return Err(GiantGraphCoverageError::LanguageNodeCount {
                    language: (*language).into(),
                    expected: expected_nodes_per_language,
                    actual: coverage.node_count,
                });
            }
            coverage.validate_required_features(language)?;
        }
        Ok(())
    }
}

impl GiantGraphLanguageCoverage {
    pub fn with_node_count(node_count: usize) -> Self {
        Self {
            node_count,
            ..Self::default()
        }
    }

    fn validate_required_features(&self, language: &str) -> Result<(), GiantGraphCoverageError> {
        for (feature, count) in [
            ("adapter_nodes", self.adapter_nodes),
            ("gpu_nodes", self.gpu_nodes),
            ("stateful_nodes", self.stateful_nodes),
            ("zero_copy_nodes", self.zero_copy_nodes),
            ("shared_reference_nodes", self.shared_reference_nodes),
            ("cow_nodes", self.cow_nodes),
            ("mutable_nodes", self.mutable_nodes),
            ("owned_nodes", self.owned_nodes),
            ("typed_error_nodes", self.typed_error_nodes),
            ("raw_events", self.raw_events),
        ] {
            if count == 0 {
                return Err(GiantGraphCoverageError::MissingFeature {
                    language: language.into(),
                    feature: feature.into(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_coverage_validates_language_structure_and_features() {
        let coverage = full_coverage().with_structure(2, 40, 76);

        assert_eq!(coverage.validate(&["rust", "python"], 20, 76), Ok(()));
        assert_eq!(
            coverage.validate(&["rust", "node"], 20, 76),
            Err(GiantGraphCoverageError::MissingLanguage("node".into()))
        );
    }

    #[test]
    fn graph_coverage_reports_payload_and_gpu_totals() {
        let coverage = full_coverage().with_structure(2, 40, 76);

        assert_eq!(coverage.total_gpu_nodes(), 2);
        assert_eq!(coverage.total_payload_mode_nodes(), 10);
    }

    fn full_coverage() -> GiantGraphCoverageSummary {
        let mut coverage = GiantGraphCoverageSummary::default();
        coverage.record_language("rust", language_coverage());
        coverage.record_language("python", language_coverage());
        coverage
    }

    fn language_coverage() -> GiantGraphLanguageCoverage {
        GiantGraphLanguageCoverage {
            node_count: 20,
            adapter_nodes: 2,
            gpu_nodes: 1,
            stateful_nodes: 1,
            zero_copy_nodes: 1,
            shared_reference_nodes: 1,
            cow_nodes: 1,
            mutable_nodes: 1,
            owned_nodes: 1,
            typed_error_nodes: 1,
            raw_events: 1,
        }
    }
}
