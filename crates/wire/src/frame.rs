//! Frame format for the exdb wire protocol.

use std::io::{Read, Write};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Result, WireError};

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u8 = 0x01;

/// Maximum frame payload accepted by default: 16 MiB.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Fixed binary frame header size.
pub const BINARY_HEADER_LEN: usize = 12;

const FLAG_COMPRESSED: u8 = 0x01;
const RESERVED_FLAGS: u8 = !FLAG_COMPRESSED;

/// Wire frame mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// JSON object terminated by `\n`.
    JsonText,
    /// 12-byte binary header followed by a payload.
    Binary,
}

/// Binary payload encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    Json = 0x01,
    Bson = 0x02,
    Protobuf = 0x03,
}

impl Encoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Encoding::Json => "json",
            Encoding::Bson => "bson",
            Encoding::Protobuf => "protobuf",
        }
    }
}

impl TryFrom<u8> for Encoding {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Encoding::Json),
            0x02 => Ok(Encoding::Bson),
            0x03 => Ok(Encoding::Protobuf),
            other => Err(WireError::UnsupportedEncoding(other)),
        }
    }
}

impl From<Encoding> for u8 {
    fn from(value: Encoding) -> Self {
        value as u8
    }
}

/// Decoded binary frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryFrameHeader {
    pub version: u8,
    pub flags: u8,
    pub encoding: Encoding,
    pub msg_type: u8,
    pub msg_id: u32,
    pub length: u32,
}

impl BinaryFrameHeader {
    /// Decode a 12-byte binary frame header.
    pub fn decode(bytes: [u8; BINARY_HEADER_LEN]) -> Result<Self> {
        let version = bytes[0];
        if version != PROTOCOL_VERSION {
            return Err(WireError::UnsupportedVersion(version));
        }

        let flags = bytes[1];
        if flags & RESERVED_FLAGS != 0 {
            return Err(WireError::UnsupportedFlags(flags));
        }
        Ok(Self {
            version,
            flags,
            encoding: Encoding::try_from(bytes[2])?,
            msg_type: bytes[3],
            msg_id: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            length: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        })
    }

    /// Encode this header into the on-wire 12-byte representation.
    pub fn encode(self) -> [u8; BINARY_HEADER_LEN] {
        let mut bytes = [0u8; BINARY_HEADER_LEN];
        bytes[0] = self.version;
        bytes[1] = self.flags;
        bytes[2] = self.encoding as u8;
        bytes[3] = self.msg_type;
        bytes[4..8].copy_from_slice(&self.msg_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.length.to_le_bytes());
        bytes
    }
}

/// A decoded wire frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub frame_type: FrameType,
    pub msg_id: u32,
    pub msg_type: u8,
    pub payload: Vec<u8>,
    pub encoding: Encoding,
    pub compressed: bool,
}

impl RawFrame {
    pub fn json_text(payload: Vec<u8>) -> Self {
        Self {
            frame_type: FrameType::JsonText,
            msg_id: 0,
            msg_type: 0,
            payload,
            encoding: Encoding::Json,
            compressed: false,
        }
    }

    pub fn binary(msg_id: u32, msg_type: u8, encoding: Encoding, payload: Vec<u8>) -> Self {
        Self {
            frame_type: FrameType::Binary,
            msg_id,
            msg_type,
            payload,
            encoding,
            compressed: false,
        }
    }

    pub fn binary_compressed(
        msg_id: u32,
        msg_type: u8,
        encoding: Encoding,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            frame_type: FrameType::Binary,
            msg_id,
            msg_type,
            payload,
            encoding,
            compressed: true,
        }
    }
}

/// Read one frame with the default 16 MiB payload limit.
pub async fn read_frame<R>(reader: &mut R) -> Result<RawFrame>
where
    R: AsyncRead + Unpin,
{
    read_frame_with_limit(reader, DEFAULT_MAX_MESSAGE_SIZE).await
}

/// Read one frame with an explicit payload limit.
pub async fn read_frame_with_limit<R>(reader: &mut R, max_message_size: usize) -> Result<RawFrame>
where
    R: AsyncRead + Unpin,
{
    let mut first = [0u8; 1];
    reader.read_exact(&mut first).await?;

    if first[0] == b'{' {
        let payload = read_json_line_after_first_byte(reader, first[0], max_message_size).await?;
        return Ok(RawFrame::json_text(payload));
    }

    let mut header_bytes = [0u8; BINARY_HEADER_LEN];
    header_bytes[0] = first[0];
    reader.read_exact(&mut header_bytes[1..]).await?;
    let header = BinaryFrameHeader::decode(header_bytes)?;

    let length = header.length as usize;
    if length > max_message_size {
        return Err(WireError::BinaryMessageTooLarge {
            msg_id: header.msg_id,
            msg_type: header.msg_type,
            encoding: header.encoding as u8,
            length,
            limit: max_message_size,
        });
    }

    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).await?;
    let payload = if header.flags & FLAG_COMPRESSED != 0 {
        decompress_payload(&payload, max_message_size)
            .map_err(|err| binary_payload_error_with_header(err, header))?
    } else {
        payload
    };
    Ok(RawFrame::binary(
        header.msg_id,
        header.msg_type,
        header.encoding,
        payload,
    ))
}

async fn read_json_line_after_first_byte<R>(
    reader: &mut R,
    first: u8,
    max_message_size: usize,
) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut payload = Vec::with_capacity(128);
    payload.push(first);

    loop {
        if payload.len() > max_message_size {
            return Err(WireError::MessageTooLarge {
                length: payload.len(),
                limit: max_message_size,
            });
        }

        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            if payload.last() == Some(&b'\r') {
                payload.pop();
            }
            return Ok(payload);
        }
        payload.push(byte[0]);
    }
}

/// Write one frame.
pub async fn write_frame<W>(writer: &mut W, frame: &RawFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_frame_with_limit(writer, frame, DEFAULT_MAX_MESSAGE_SIZE).await
}

/// Write one frame with an explicit payload limit.
pub async fn write_frame_with_limit<W>(
    writer: &mut W,
    frame: &RawFrame,
    max_message_size: usize,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match frame.frame_type {
        FrameType::JsonText => {
            if frame.payload.len() > max_message_size {
                return Err(WireError::MessageTooLarge {
                    length: frame.payload.len(),
                    limit: max_message_size,
                });
            }
            writer.write_all(&frame.payload).await?;
            writer.write_all(b"\n").await?;
        }
        FrameType::Binary => {
            writer
                .write_all(&encode_binary_frame_with_limit(frame, max_message_size)?)
                .await?;
        }
    }
    writer.flush().await?;
    Ok(())
}

/// Encode a binary frame as one complete header+payload byte buffer.
pub fn encode_binary_frame_with_limit(
    frame: &RawFrame,
    max_message_size: usize,
) -> Result<Vec<u8>> {
    if frame.frame_type != FrameType::Binary {
        return Err(WireError::MalformedFrame("expected binary frame"));
    }
    if frame.payload.len() > max_message_size {
        return Err(WireError::MessageTooLarge {
            length: frame.payload.len(),
            limit: max_message_size,
        });
    }

    let (flags, payload) = if frame.compressed {
        let compressed = compress_payload(&frame.payload)?;
        if compressed.len() > max_message_size {
            return Err(WireError::MessageTooLarge {
                length: compressed.len(),
                limit: max_message_size,
            });
        }
        (FLAG_COMPRESSED, compressed)
    } else {
        (0, frame.payload.clone())
    };
    let header = BinaryFrameHeader {
        version: PROTOCOL_VERSION,
        flags,
        encoding: frame.encoding,
        msg_type: frame.msg_type,
        msg_id: frame.msg_id,
        length: payload.len() as u32,
    };
    let mut bytes = Vec::with_capacity(BINARY_HEADER_LEN + payload.len());
    bytes.extend_from_slice(&header.encode());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// Decode one complete binary header+payload byte buffer.
pub fn decode_binary_frame_with_limit(bytes: &[u8], max_message_size: usize) -> Result<RawFrame> {
    if bytes.len() < BINARY_HEADER_LEN {
        return Err(WireError::MalformedFrame("binary frame header too short"));
    }
    let header = BinaryFrameHeader::decode(bytes[..BINARY_HEADER_LEN].try_into().unwrap())?;
    let payload = &bytes[BINARY_HEADER_LEN..];
    let length = header.length as usize;
    if length > max_message_size {
        return Err(WireError::BinaryMessageTooLarge {
            msg_id: header.msg_id,
            msg_type: header.msg_type,
            encoding: header.encoding as u8,
            length,
            limit: max_message_size,
        });
    }
    if payload.len() != length {
        return Err(WireError::MalformedFrame("binary frame length mismatch"));
    }
    let payload = if header.flags & FLAG_COMPRESSED != 0 {
        decompress_payload(payload, max_message_size)
            .map_err(|err| binary_payload_error_with_header(err, header))?
    } else {
        payload.to_vec()
    };
    Ok(RawFrame {
        frame_type: FrameType::Binary,
        msg_id: header.msg_id,
        msg_type: header.msg_type,
        payload,
        encoding: header.encoding,
        compressed: header.flags & FLAG_COMPRESSED != 0,
    })
}

fn compress_payload(payload: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder.write_all(payload)?;
    encoder
        .finish()
        .map_err(|error| WireError::Compression(error.to_string()))
}

fn decompress_payload(payload: &[u8], max_message_size: usize) -> Result<Vec<u8>> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(payload);
    let mut decoded = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let read = decoder.read(&mut chunk)?;
        if read == 0 {
            return Ok(decoded);
        }
        let next_len = decoded
            .len()
            .checked_add(read)
            .ok_or(WireError::MessageTooLarge {
                length: usize::MAX,
                limit: max_message_size,
            })?;
        if next_len > max_message_size {
            return Err(WireError::MessageTooLarge {
                length: next_len,
                limit: max_message_size,
            });
        }
        decoded.extend_from_slice(&chunk[..read]);
    }
}

fn binary_payload_error_with_header(err: WireError, header: BinaryFrameHeader) -> WireError {
    match err {
        WireError::MessageTooLarge { length, limit } => WireError::BinaryMessageTooLarge {
            msg_id: header.msg_id,
            msg_type: header.msg_type,
            encoding: header.encoding as u8,
            length,
            limit,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn binary_header_roundtrip() {
        let header = BinaryFrameHeader {
            version: PROTOCOL_VERSION,
            flags: 0,
            encoding: Encoding::Bson,
            msg_type: 0x20,
            msg_id: 42,
            length: 1234,
        };

        assert_eq!(BinaryFrameHeader::decode(header.encode()).unwrap(), header);
    }

    #[test]
    fn binary_header_rejects_reserved_flags() {
        let mut bytes = [0u8; BINARY_HEADER_LEN];
        bytes[0] = PROTOCOL_VERSION;
        bytes[1] = 0x80;
        bytes[2] = Encoding::Json as u8;

        assert!(matches!(
            BinaryFrameHeader::decode(bytes),
            Err(WireError::UnsupportedFlags(0x80))
        ));
    }

    #[test]
    fn binary_header_accepts_compression_flag() {
        let header = BinaryFrameHeader {
            version: PROTOCOL_VERSION,
            flags: FLAG_COMPRESSED,
            encoding: Encoding::Json,
            msg_type: 0x21,
            msg_id: 99,
            length: 128,
        };

        assert_eq!(BinaryFrameHeader::decode(header.encode()).unwrap(), header);
    }

    #[tokio::test]
    async fn json_text_frame_roundtrip() {
        let (mut client, mut server) = duplex(1024);
        let frame = RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec());

        write_frame(&mut client, &frame).await.unwrap();
        let decoded = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded.frame_type, FrameType::JsonText);
        assert_eq!(decoded.encoding, Encoding::Json);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[tokio::test]
    async fn binary_frame_roundtrip() {
        let (mut client, mut server) = duplex(1024);
        let frame = RawFrame::binary(7, 0x21, Encoding::Json, br#"{"doc_id":"01"}"#.to_vec());

        write_frame(&mut client, &frame).await.unwrap();
        let decoded = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn binary_compressed_frame_roundtrip() {
        let (mut client, mut server) = duplex(1024);
        let payload = br#"{"doc":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#.to_vec();
        let frame = RawFrame::binary_compressed(8, 0x21, Encoding::Json, payload.clone());

        write_frame(&mut client, &frame).await.unwrap();
        let decoded = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, RawFrame::binary(8, 0x21, Encoding::Json, payload));
    }

    #[tokio::test]
    async fn compressed_frame_decompressed_limit_is_enforced() {
        let (mut client, mut server) = duplex(1024);
        let compressed = compress_payload(&vec![b'a'; 2048]).unwrap();
        assert!(compressed.len() <= 512);
        let header = BinaryFrameHeader {
            version: PROTOCOL_VERSION,
            flags: FLAG_COMPRESSED,
            encoding: Encoding::Json,
            msg_type: 0x21,
            msg_id: 10,
            length: compressed.len() as u32,
        };

        client.write_all(&header.encode()).await.unwrap();
        client.write_all(&compressed).await.unwrap();

        match read_frame_with_limit(&mut server, 512).await {
            Err(WireError::BinaryMessageTooLarge {
                msg_id,
                msg_type,
                encoding,
                length,
                limit,
            }) => {
                assert_eq!(msg_id, 10);
                assert_eq!(msg_type, 0x21);
                assert_eq!(encoding, Encoding::Json as u8);
                assert!(length > limit);
                assert_eq!(limit, 512);
            }
            other => panic!("expected framed message-too-large error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn json_frame_limit_is_enforced() {
        let (mut client, mut server) = duplex(1024);
        client
            .write_all(b"{\"id\":1,\"type\":\"ping\"}\n")
            .await
            .unwrap();

        assert!(matches!(
            read_frame_with_limit(&mut server, 8).await,
            Err(WireError::MessageTooLarge { .. })
        ));
    }
}
