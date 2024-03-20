use clap::Parser;
use std::{
    env,
    fs::{self},
    io::{BufRead, BufReader},
    path::Path,
};
use walkdir::{DirEntry, WalkDir};

mod cli;

fn main() {
    let args = cli::Args::parse();

    let file_paths: Vec<DirEntry> = WalkDir::new(args.folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
        .collect();

    for file in file_paths {
        let path = file.path();
        let ancestors: Vec<&Path> = path.ancestors().collect();
        let lang = ancestors[1];
        let snapshot = ancestors[2];
        println!(
            "{:?}\t{:?}\t{:?}",
            lang,
            snapshot,
            path.file_name().unwrap()
        );
    }
}
