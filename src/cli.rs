use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, value_name = "FOLDER")]
    pub folder: PathBuf,

    #[arg(short, long, value_name = "OUTPUT")]
    pub output: PathBuf,
}
