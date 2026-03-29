use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, anyhow};

use crate::elastic::RescalePlan;

/// Rescale cutover phase aligned with P6 S0->S6 state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RescalePhase {
    S0Idle,
    S1Preparing,
    S2BarrierInjected,
    S3Aligned,
    S4Flushed,
    S5Switched,
    S6Activated,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RescaleSession {
    pub job_id: String,
    pub operator_id: u32,
    pub new_parallelism: usize,
    pub generation: u64,
    pub checkpoint_id: Option<u64>,
    pub phase: RescalePhase,
    pub prepared_task_ids: Vec<String>,
    pub failure_reason: Option<String>,
}

impl RescaleSession {
    fn new(plan: &RescalePlan, generation: u64) -> Self {
        Self {
            job_id: plan.job_id.clone(),
            operator_id: plan.operator_id,
            new_parallelism: plan.new_parallelism,
            generation,
            checkpoint_id: None,
            phase: RescalePhase::S1Preparing,
            prepared_task_ids: Vec::new(),
            failure_reason: None,
        }
    }
}

/// JM-side coordinator for rescale orchestration.
pub struct RescaleCoordinator {
    next_generation: AtomicU64,
    latest_generation_by_job: HashMap<String, u64>,
    sessions_by_job: HashMap<String, RescaleSession>,
}

impl Default for RescaleCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl RescaleCoordinator {
    pub fn new() -> Self {
        Self {
            next_generation: AtomicU64::new(1),
            latest_generation_by_job: HashMap::new(),
            sessions_by_job: HashMap::new(),
        }
    }

    /// Backward-compatible helper used by older call sites.
    ///
    /// Executes a synthetic full transition in-memory.
    pub fn execute_rescale(&mut self, plan: &RescalePlan) -> Result<u64> {
        let generation = self.begin_rescale(plan)?;
        self.mark_prepared(&plan.job_id, generation, Vec::new())?;
        self.mark_barrier_injected(&plan.job_id, generation, 0)?;
        self.mark_aligned(&plan.job_id, generation)?;
        self.mark_flushed(&plan.job_id, generation)?;
        self.mark_switched(&plan.job_id, generation)?;
        self.mark_activated(&plan.job_id, generation)?;
        Ok(generation)
    }

    pub fn begin_rescale(&mut self, plan: &RescalePlan) -> Result<u64> {
        if plan.new_parallelism == 0 {
            return Err(anyhow!("new_parallelism must be greater than 0"));
        }
        if let Some(existing) = self.sessions_by_job.get(&plan.job_id)
            && !matches!(
                existing.phase,
                RescalePhase::S6Activated | RescalePhase::Failed | RescalePhase::S0Idle
            )
        {
            return Err(anyhow!(
                "job {} already has inflight rescale in phase {:?}",
                plan.job_id,
                existing.phase
            ));
        }

        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        self.sessions_by_job
            .insert(plan.job_id.clone(), RescaleSession::new(plan, generation));
        Ok(generation)
    }

    pub fn mark_prepared(
        &mut self,
        job_id: &str,
        generation: u64,
        prepared_task_ids: Vec<String>,
    ) -> Result<()> {
        let session = self.session_mut(job_id, generation)?;
        ensure_phase(
            job_id,
            generation,
            session.phase,
            &[RescalePhase::S1Preparing],
            "prepare",
        )?;
        session.prepared_task_ids = prepared_task_ids;
        Ok(())
    }

    pub fn mark_barrier_injected(
        &mut self,
        job_id: &str,
        generation: u64,
        checkpoint_id: u64,
    ) -> Result<()> {
        let session = self.session_mut(job_id, generation)?;
        ensure_phase(
            job_id,
            generation,
            session.phase,
            &[RescalePhase::S1Preparing],
            "inject_barrier",
        )?;
        session.checkpoint_id = Some(checkpoint_id);
        session.phase = RescalePhase::S2BarrierInjected;
        Ok(())
    }

    pub fn mark_aligned(&mut self, job_id: &str, generation: u64) -> Result<()> {
        self.transition(
            job_id,
            generation,
            &[RescalePhase::S2BarrierInjected],
            RescalePhase::S3Aligned,
            "align",
        )
    }

    pub fn mark_flushed(&mut self, job_id: &str, generation: u64) -> Result<()> {
        self.transition(
            job_id,
            generation,
            &[RescalePhase::S3Aligned],
            RescalePhase::S4Flushed,
            "flush",
        )
    }

    pub fn mark_switched(&mut self, job_id: &str, generation: u64) -> Result<()> {
        self.transition(
            job_id,
            generation,
            &[RescalePhase::S4Flushed],
            RescalePhase::S5Switched,
            "switch",
        )
    }

    pub fn mark_activated(&mut self, job_id: &str, generation: u64) -> Result<()> {
        self.transition(
            job_id,
            generation,
            &[RescalePhase::S5Switched],
            RescalePhase::S6Activated,
            "activate",
        )?;
        self.latest_generation_by_job
            .insert(job_id.to_string(), generation);
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        job_id: &str,
        generation: u64,
        failure_reason: impl Into<String>,
    ) -> Result<()> {
        let session = self.session_mut(job_id, generation)?;
        if matches!(session.phase, RescalePhase::S6Activated) {
            return Err(anyhow!(
                "job {} generation {} is already activated",
                job_id,
                generation
            ));
        }
        session.phase = RescalePhase::Failed;
        session.failure_reason = Some(failure_reason.into());
        Ok(())
    }

    pub fn latest_generation(&self, job_id: &str) -> Option<u64> {
        self.latest_generation_by_job.get(job_id).copied()
    }

    pub fn session(&self, job_id: &str) -> Option<RescaleSession> {
        self.sessions_by_job.get(job_id).cloned()
    }

    pub fn rollback(&mut self, job_id: &str) {
        self.latest_generation_by_job.remove(job_id);
        self.sessions_by_job.remove(job_id);
    }

    fn session_mut(&mut self, job_id: &str, generation: u64) -> Result<&mut RescaleSession> {
        let session = self
            .sessions_by_job
            .get_mut(job_id)
            .ok_or_else(|| anyhow!("job {} has no active rescale session", job_id))?;
        if session.generation != generation {
            return Err(anyhow!(
                "job {} generation mismatch: expected {}, got {}",
                job_id,
                session.generation,
                generation
            ));
        }
        Ok(session)
    }

    fn transition(
        &mut self,
        job_id: &str,
        generation: u64,
        allowed_from: &[RescalePhase],
        next: RescalePhase,
        action: &str,
    ) -> Result<()> {
        let session = self.session_mut(job_id, generation)?;
        ensure_phase(job_id, generation, session.phase, allowed_from, action)?;
        session.phase = next;
        Ok(())
    }
}

fn ensure_phase(
    job_id: &str,
    generation: u64,
    current: RescalePhase,
    allowed_from: &[RescalePhase],
    action: &str,
) -> Result<()> {
    if allowed_from.contains(&current) {
        return Ok(());
    }
    Err(anyhow!(
        "job {} generation {} cannot {} from phase {:?}",
        job_id,
        generation,
        action,
        current
    ))
}

#[cfg(test)]
#[path = "tests/rescale_tests.rs"]
mod tests;
