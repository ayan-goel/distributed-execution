use dispatch_protocol::v1::{
    Assignment, CreateUploadRequest, CreateUploadResponse, FinalizeUploadRequest,
    GrantUploadPartRequest, ListAssignmentsResponse, UploadPart,
};
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
    match std::env::args().nth(1).as_deref() {
        Some("upload-request") => return roundtrip::<CreateUploadRequest>(&bytes),
        Some("upload-response") => return roundtrip::<CreateUploadResponse>(&bytes),
        Some("part-request") => return roundtrip::<GrantUploadPartRequest>(&bytes),
        Some("part-response") => return roundtrip::<UploadPart>(&bytes),
        Some("finalize") => return roundtrip::<FinalizeUploadRequest>(&bytes),
        _ => {}
    }
    let page = if std::env::args().nth(1).as_deref() == Some("page") {
        Some(ListAssignmentsResponse::decode(bytes.as_slice())?)
    } else {
        None
    };
    let assignment = if let Some(page) = &page {
        if page.next_after_job_id != "00000000-0000-0000-0000-000000000001" {
            return Err("page cursor changed".into());
        }
        page.assignments
            .first()
            .ok_or("missing page assignment")?
            .clone()
    } else {
        Assignment::decode(bytes.as_slice())?
    };
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
    let input = assignment.inputs.first().ok_or("missing input manifest")?;
    if input.dataset_id != "00000000-0000-0000-0000-000000000005"
        || input.dataset_name != "example"
        || input.mount_path != "/inputs/example"
        || input
            .archive
            .as_ref()
            .ok_or("missing archive version")?
            .size_bytes
            != 4096
        || !input
            .file_manifest_json
            .starts_with(b"{\"format\":\"tar.v1\"")
        || input.expires_unix_ms != 1_780_000_000_000
    {
        return Err("dataset input contract changed in transport".into());
    }
    io::stdout().write_all(&match page {
        Some(page) => page.encode_to_vec(),
        None => assignment.encode_to_vec(),
    })?;
    Ok(())
}

fn roundtrip<M: Message + Default>(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    io::stdout().write_all(&M::decode(bytes)?.encode_to_vec())?;
    Ok(())
}
