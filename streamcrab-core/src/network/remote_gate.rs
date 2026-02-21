use std::collections::HashMap;

use anyhow::{Result, anyhow};

use crate::channel::LocalChannelSender;
use crate::types::StreamElement;

use super::frame::{Frame, FrameType};

fn frame_type_for_element(element: &StreamElement<Vec<u8>>) -> FrameType {
    match element {
        StreamElement::Record(_) => FrameType::Data,
        StreamElement::Watermark(_) => FrameType::Watermark,
        StreamElement::CheckpointBarrier(_) => FrameType::Barrier,
        StreamElement::End => FrameType::End,
    }
}

/// Encode a stream element into a transport frame.
pub fn encode_stream_element(channel_id: u32, element: StreamElement<Vec<u8>>) -> Result<Frame> {
    let frame_type = frame_type_for_element(&element);
    let payload = bincode::serialize(&element)?;
    Ok(Frame::new(frame_type, channel_id, payload))
}

/// Decode a transport frame into stream element and channel id.
pub fn decode_stream_element(frame: Frame) -> Result<(u32, StreamElement<Vec<u8>>)> {
    if frame.frame_type == FrameType::Control {
        return Err(anyhow!("control frames are not stream elements"));
    }

    let element: StreamElement<Vec<u8>> = bincode::deserialize(&frame.payload)?;
    let expected = frame_type_for_element(&element);
    if expected != frame.frame_type {
        return Err(anyhow!(
            "frame type mismatch: expected {:?}, got {:?}",
            expected,
            frame.frame_type
        ));
    }
    Ok((frame.channel_id, element))
}

/// Bridges incoming remote frames into local input channels for a task.
pub struct RemoteInputGate {
    channel_senders: HashMap<u32, LocalChannelSender<Vec<u8>>>,
}

impl RemoteInputGate {
    pub fn new(channel_senders: HashMap<u32, LocalChannelSender<Vec<u8>>>) -> Self {
        Self { channel_senders }
    }

    pub fn ingest_frame(&self, frame: Frame) -> Result<()> {
        let (channel_id, element) = decode_stream_element(frame)?;
        let sender = self
            .channel_senders
            .get(&channel_id)
            .ok_or_else(|| anyhow!("unknown input channel id {}", channel_id))?;
        sender.send(element)
    }

    pub fn channel_count(&self) -> usize {
        self.channel_senders.len()
    }
}

/// Encodes local task outputs into remote transport frames.
pub struct RemoteOutputGate {
    output_channel_ids: Vec<u32>,
}

impl RemoteOutputGate {
    pub fn new(output_channel_ids: Vec<u32>) -> Self {
        Self { output_channel_ids }
    }

    pub fn encode_for_output(
        &self,
        output_index: usize,
        element: StreamElement<Vec<u8>>,
    ) -> Result<Frame> {
        let channel_id = self
            .output_channel_ids
            .get(output_index)
            .copied()
            .ok_or_else(|| anyhow!("invalid output index {}", output_index))?;
        encode_stream_element(channel_id, element)
    }

    pub fn output_count(&self) -> usize {
        self.output_channel_ids.len()
    }
}

#[cfg(test)]
#[path = "tests/remote_gate_tests.rs"]
mod tests;
