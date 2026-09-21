use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!("dispatch-worker {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    #[cfg(unix)]
    if args.len() == 4
        && args[0] == "run"
        && args[1] == "--config"
        && args[3] == "--dev-soft-scratch"
    {
        let result = match dispatch_worker::agent::AgentConfig::load(std::path::Path::new(&args[2]))
        {
            Ok(config) => dispatch_worker::agent::run(config).await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
        return ExitCode::SUCCESS;
    }
    eprintln!("usage: dispatch-worker --version | run --config FILE --dev-soft-scratch");
    ExitCode::from(2)
}
