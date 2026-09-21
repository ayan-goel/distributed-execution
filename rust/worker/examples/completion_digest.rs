use dispatch_protocol::{v1::CompleteAttemptRequest, MAX_MESSAGE_BYTES};
use dispatch_worker::control::completion_digest;
use prost::Message;
use std::io::{self, Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = io::stdin().lock();
    loop {
        let mut size = [0_u8; 4];
        if input.read(&mut size[..1])? == 0 {
            return Ok(());
        }
        input.read_exact(&mut size[1..])?;
        let size = u32::from_le_bytes(size) as usize;
        if size > MAX_MESSAGE_BYTES {
            return Err("completion fixture too large".into());
        }
        let mut bytes = vec![0; size];
        input.read_exact(&mut bytes)?;
        let request = CompleteAttemptRequest::decode(bytes.as_slice())?;
        match completion_digest(&request) {
            Ok(digest) => println!("{digest}"),
            Err(_) => println!("INVALID"),
        }
    }
}
