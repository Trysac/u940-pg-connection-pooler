//! Binary entrypoint for `trysac-pg-connection-pooler`.

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut stdout = std::io::stdout();
    let mut stderr = std::io::stderr();
    let code = pg_connection_pooler::cli::run(&args[1..], &mut stdout, &mut stderr);
    std::process::exit(code);
}
