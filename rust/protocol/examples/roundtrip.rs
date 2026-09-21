use dispatch_protocol::v1::Assignment;
use prost::Message;
use std::io::{self, Read, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    io::stdin()
        .take(dispatch_protocol::MAX_MESSAGE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > dispatch_protocol::MAX_MESSAGE_BYTES {
        return Err("message too large".into());
    }
    let assignment = Assignment::decode(bytes.as_slice())?;
    let authority = assignment.authority.as_ref().ok_or("missing authority")?;
    if authority.generation != 9_007_199_254_740_993 {
        return Err("64-bit generation did not survive transport".into());
    }
    if assignment
        .resources
        .as_ref()
        .ok_or("missing resources")?
        .memory_bytes
        != 4_294_967_296
    {
        return Err("64-bit memory limit did not survive transport".into());
    }
    io::stdout().write_all(&assignment.encode_to_vec())?;
    Ok(())
}
