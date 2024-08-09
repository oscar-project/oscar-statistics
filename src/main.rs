use clap::Parser;

mod cli;
mod errors;
mod oscar;
mod stats;

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();

    stats::compute_stats(&args.src, &args.dst, args.threads).await;
}
