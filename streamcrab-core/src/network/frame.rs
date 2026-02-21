use anyhow::{Result, anyhow};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Logical frame type for multiplexed TCP transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Data = 1,
    Barrier = 2,
    Watermark = 3,
    End = 4,
    Control = 5,
}

impl TryFrom<u8> for FrameType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(FrameType::Data),
            2 => Ok(FrameType::Barrier),
            3 => Ok(FrameType::Watermark),
            4 => Ok(FrameType::End),
            5 => Ok(FrameType::Control),
            other => Err(anyhow!("unknown frame type: {}", other)),
        }
    }
}

/// Wire frame: `[len:u32][type:u8][channel_id:u32][payload:bytes]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    pub channel_id: u32,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(frame_type: FrameType, channel_id: u32, payload: Vec<u8>) -> Self {
        Self {
            frame_type,
            channel_id,
            payload,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let body_len = 1 + 4 + self.payload.len();
        let mut out = Vec::with_capacity(4 + body_len);
        out.extend_from_slice(&(body_len as u32).to_be_bytes());
        out.push(self.frame_type as u8);
        out.extend_from_slice(&self.channel_id.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(body: &[u8]) -> Result<Self> {
        if body.len() < 5 {
            return Err(anyhow!("frame body too short: {}", body.len()));
        }
        let frame_type = FrameType::try_from(body[0])?;
        let channel_id = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
        Ok(Self {
            frame_type,
            channel_id,
            payload: body[5..].to_vec(),
        })
    }
}

/// Try decode one frame from a sticky buffer.
pub fn try_decode_from_buffer(buffer: &mut Vec<u8>) -> Result<Option<Frame>> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let body_len = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
    if buffer.len() < 4 + body_len {
        return Ok(None);
    }
    let body = buffer[4..4 + body_len].to_vec();
    buffer.drain(0..4 + body_len);
    Ok(Some(Frame::decode(&body)?))
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Frame>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let body_len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; body_len];
    reader.read_exact(&mut body).await?;
    Frame::decode(&body)
}

pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&frame.encode()).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/frame_tests.rs"]
mod tests;
