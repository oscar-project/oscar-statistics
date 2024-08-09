use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "oscar-statistics")]
#[command(author = "Pedro Ortiz Suarez <pedro@commoncrawl.org>")]
#[command(version = "0.1.0")]
#[command(about = "Compute statistics of an OSCAR release", long_about = None)]
pub struct Args {
    /// Folder containing the indices
    #[arg(value_name = "INPUT FOLDER")]
    pub src: PathBuf,

    /// Parquet file to write
    #[arg(value_name = "DESTINATION FILE")]
    pub dst: PathBuf,

    /// Name of the snapshot
    #[arg(value_name = "SNAPSHOT")]
    pub snapshot: String,

    /// Number of threads to use
    #[arg(short, long, default_value = "10", value_name = "NUMBER OF THREADS")]
    pub threads: usize,
}
