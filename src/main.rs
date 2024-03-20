use clap::Parser;
use core::num;
use oscar_io::v3::Document;
use std::{
    fs::File,
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

        let decoder = {
            let file = File::open(file.path()).unwrap();
            zstd::Decoder::new(file).unwrap()
        };
        let reader = BufReader::new(decoder);
        let mut num_docs: u64 = 0;
        let mut num_toks: u64 = 0;
        for line in reader.lines() {
            let doc = serde_json::from_str::<Document>(&line.unwrap()).unwrap();
            let content = doc.content();
            for char in content.chars() {
                if char.is_whitespace() {
                    num_toks += 1;
                }
            }
            num_docs += 1;
        }
        println!(
            "{}\t{}\t{:?}\t{}\t{}",
            lang,
            snapshot,
            path.file_name().unwrap(),
            num_docs,
            num_toks
        );
    }
}
