pub mod v1 {
    tonic::include_proto!("dispatch.worker.v1");
}

pub const VERSION: u32 = 1;
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
