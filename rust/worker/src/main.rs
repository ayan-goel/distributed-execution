use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!("dispatch-worker {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    #[cfg(unix)]
    if (args.len() == 3 || args.len() == 4)
        && matches!(args[0].as_str(), "run" | "init-scratch")
        && args[1] == "--config"
        && (args.len() == 3 || (args[0] == "run" && args[3] == "--dev-soft-scratch"))
    {
        let result = match dispatch_worker::agent::AgentConfig::load(std::path::Path::new(&args[2]))
        {
            Ok(config) if args[0] == "init-scratch" => {
                dispatch_worker::agent::initialize_scratch(&config)
            }
            Ok(config) if args.len() == 4 => dispatch_worker::agent::run_development(config).await,
            Ok(config) => dispatch_worker::agent::run(config).await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
        return ExitCode::SUCCESS;
    }
    eprintln!("usage: dispatch-worker --version | init-scratch --config FILE | run --config FILE [--dev-soft-scratch]");
    ExitCode::from(2)
}
