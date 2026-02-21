use std::collections::HashSet;

use anyhow::{Result, anyhow};

use crate::runtime::descriptors::{JobPlan, OperatorDescriptor};

/// Minimal descriptor validator for TM-side operator instantiation in P4.
///
/// v1 only validates supported operator descriptor + known udf ids.
/// Actual runtime chain reconstruction stays in follow-up iterations.
#[derive(Debug, Clone, Default)]
pub struct OperatorFactory {
    known_udfs: HashSet<String>,
}

impl OperatorFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_udf(mut self, udf_id: impl Into<String>) -> Self {
        self.known_udfs.insert(udf_id.into());
        self
    }

    pub fn register_udf(&mut self, udf_id: impl Into<String>) {
        self.known_udfs.insert(udf_id.into());
    }

    pub fn validate_job_plan(&self, plan: &JobPlan) -> Result<()> {
        for op in &plan.operators {
            self.validate_operator(op)?;
        }
        Ok(())
    }

    fn validate_operator(&self, op: &OperatorDescriptor) -> Result<()> {
        match op {
            OperatorDescriptor::Source { .. }
            | OperatorDescriptor::KeyBy { .. }
            | OperatorDescriptor::Sink { .. } => Ok(()),
            OperatorDescriptor::Map { udf_id, .. } => self.validate_udf_id("Map", udf_id),
            OperatorDescriptor::Filter { udf_id, .. } => self.validate_udf_id("Filter", udf_id),
            OperatorDescriptor::FlatMap { udf_id, .. } => self.validate_udf_id("FlatMap", udf_id),
            OperatorDescriptor::Reduce { udf_id } => self.validate_udf_id("Reduce", udf_id),
            OperatorDescriptor::Window { function_id, .. } => {
                self.validate_udf_id("Window", function_id)
            }
        }
    }

    fn validate_udf_id(&self, operator_name: &str, udf_id: &str) -> Result<()> {
        if udf_id.starts_with("builtin::") || self.known_udfs.contains(udf_id) {
            return Ok(());
        }
        Err(anyhow!(
            "{operator_name} operator references unknown udf_id: {udf_id}"
        ))
    }
}
