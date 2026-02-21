use super::*;

/// Barrier alignment result.
#[derive(Debug)]
pub enum BarrierAlignResult<T> {
    Forward(StreamElement<T>),
    Buffering,
    Aligned {
        barrier: Barrier,
        buffered: Vec<(ChannelIndex, StreamElement<T>)>,
    },
    Aborted {
        checkpoint_id: CheckpointId,
        drained: Vec<(ChannelIndex, StreamElement<T>)>,
    },
}

/// Barrier aligner state for multi-input tasks.
pub struct BarrierAligner<T> {
    pub num_inputs: usize,
    pub max_buffer_size: usize,
    current_barrier: Option<Barrier>,
    barriers_received: Vec<bool>,
    blocked_channels: Vec<bool>,
    buffered: VecDeque<(ChannelIndex, StreamElement<T>)>,
    last_cleared_checkpoint_id: Option<CheckpointId>,
}

impl<T> BarrierAligner<T> {
    pub fn new(num_inputs: usize) -> Self {
        Self {
            num_inputs,
            max_buffer_size: 10_000,
            current_barrier: None,
            barriers_received: vec![false; num_inputs],
            blocked_channels: vec![false; num_inputs],
            buffered: VecDeque::new(),
            last_cleared_checkpoint_id: None,
        }
    }

    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    pub fn process_element(
        &mut self,
        channel_idx: ChannelIndex,
        element: StreamElement<T>,
    ) -> Result<BarrierAlignResult<T>> {
        if channel_idx >= self.num_inputs {
            return Err(anyhow!("channel index {} out of bounds", channel_idx));
        }

        match element {
            StreamElement::CheckpointBarrier(barrier) => self.process_barrier(channel_idx, barrier),
            other => self.process_non_barrier(channel_idx, other),
        }
    }

    fn process_barrier(
        &mut self,
        channel_idx: ChannelIndex,
        barrier: Barrier,
    ) -> Result<BarrierAlignResult<T>> {
        if self
            .last_cleared_checkpoint_id
            .is_some_and(|cleared| barrier.checkpoint_id <= cleared)
        {
            // Late barrier from a checkpoint that has already completed/aborted.
            return Ok(BarrierAlignResult::Buffering);
        }

        if let Some(current) = self.current_barrier {
            if barrier.checkpoint_id != current.checkpoint_id {
                return Err(anyhow!(
                    "out-of-order barrier: current={}, incoming={}",
                    current.checkpoint_id,
                    barrier.checkpoint_id
                ));
            }
        } else {
            self.current_barrier = Some(barrier);
            self.barriers_received.fill(false);
            self.blocked_channels.fill(false);
            self.buffered.clear();
        }

        if self.barriers_received[channel_idx] {
            return Err(anyhow!(
                "duplicate barrier {} on channel {}",
                barrier.checkpoint_id,
                channel_idx
            ));
        }

        self.barriers_received[channel_idx] = true;
        self.blocked_channels[channel_idx] = true;

        if self.barriers_received.iter().all(|v| *v) {
            let aligned_barrier = self
                .current_barrier
                .take()
                .ok_or_else(|| anyhow!("internal aligner state is inconsistent"))?;
            let buffered = self.buffered.drain(..).collect();
            self.barriers_received.fill(false);
            self.blocked_channels.fill(false);
            self.mark_checkpoint_cleared(aligned_barrier.checkpoint_id);
            return Ok(BarrierAlignResult::Aligned {
                barrier: aligned_barrier,
                buffered,
            });
        }

        Ok(BarrierAlignResult::Buffering)
    }

    fn process_non_barrier(
        &mut self,
        channel_idx: ChannelIndex,
        element: StreamElement<T>,
    ) -> Result<BarrierAlignResult<T>> {
        if self.current_barrier.is_some() && self.blocked_channels[channel_idx] {
            if self.buffered.len() >= self.max_buffer_size {
                let checkpoint_id = self
                    .current_barrier
                    .take()
                    .ok_or_else(|| anyhow!("internal aligner state is inconsistent"))?
                    .checkpoint_id;
                let mut drained: Vec<(ChannelIndex, StreamElement<T>)> =
                    self.buffered.drain(..).collect();
                drained.push((channel_idx, element));
                self.barriers_received.fill(false);
                self.blocked_channels.fill(false);
                self.mark_checkpoint_cleared(checkpoint_id);
                return Ok(BarrierAlignResult::Aborted {
                    checkpoint_id,
                    drained,
                });
            }
            self.buffered.push_back((channel_idx, element));
            return Ok(BarrierAlignResult::Buffering);
        }

        Ok(BarrierAlignResult::Forward(element))
    }

    fn mark_checkpoint_cleared(&mut self, checkpoint_id: CheckpointId) {
        self.last_cleared_checkpoint_id = Some(
            self.last_cleared_checkpoint_id
                .map_or(checkpoint_id, |prev| prev.max(checkpoint_id)),
        );
    }
}
