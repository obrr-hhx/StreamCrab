use std::collections::HashMap;

use anyhow::{Result, anyhow};

use super::{TaskManagerInfo, TmId};

/// v1 scheduler: round-robin under slot capacity constraints.
#[derive(Debug, Default)]
pub struct RoundRobinScheduler {
    cursor: usize,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign task IDs to task managers.
    ///
    /// Returns `task_id -> tm_id`.
    pub fn schedule(
        &mut self,
        task_ids: &[String],
        task_managers: &HashMap<TmId, TaskManagerInfo>,
    ) -> Result<HashMap<String, TmId>> {
        if task_ids.is_empty() {
            return Ok(HashMap::new());
        }
        if task_managers.is_empty() {
            return Err(anyhow!("no task managers registered"));
        }

        let mut tm_ids: Vec<_> = task_managers.keys().cloned().collect();
        tm_ids.sort();

        let total_slots: usize = tm_ids
            .iter()
            .map(|id| task_managers[id].num_slots)
            .sum::<usize>();
        if task_ids.len() > total_slots {
            return Err(anyhow!(
                "insufficient slots: tasks={}, slots={}",
                task_ids.len(),
                total_slots
            ));
        }

        let mut used_slots: HashMap<TmId, usize> = HashMap::new();
        let mut assigned = HashMap::with_capacity(task_ids.len());

        for task_id in task_ids {
            let mut assigned_tm = None;

            for offset in 0..tm_ids.len() {
                let idx = (self.cursor + offset) % tm_ids.len();
                let tm_id = &tm_ids[idx];
                let used = *used_slots.get(tm_id).unwrap_or(&0);
                let cap = task_managers[tm_id].num_slots;

                if used < cap {
                    assigned_tm = Some(tm_id.clone());
                    self.cursor = (idx + 1) % tm_ids.len();
                    break;
                }
            }

            let tm_id = assigned_tm.ok_or_else(|| anyhow!("unable to assign task {}", task_id))?;
            *used_slots.entry(tm_id.clone()).or_insert(0) += 1;
            assigned.insert(task_id.clone(), tm_id);
        }

        Ok(assigned)
    }
}
