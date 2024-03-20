use clap::Parser;
use std::{
    io::{BufRead, BufReader},
    path::{Component, Path},
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
        let components: Vec<_> = path
            .components()
            .rev()
            .map(|comp| comp.as_os_str())
            .collect();
        let mut lang = components[1].to_str().unwrap();
        lang = lang.strip_suffix("_meta").unwrap();
        let snapshot = components[2].to_str().unwrap();
        println!("{}\t{}\t{:?}", lang, snapshot, path.file_name().unwrap());
    }
}
