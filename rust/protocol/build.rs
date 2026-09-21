fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto/dispatch/worker/v1/worker.proto");
    // Only successful acquisition carries a large assignment. Boxing it keeps
    // polling/error responses small at the cost of one grant allocation.
    tonic_prost_build::configure()
        .boxed(".dispatch.worker.v1.AcquireWorkResponse.outcome.assignment")
        .compile_protos(
            &["../../proto/dispatch/worker/v1/worker.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
