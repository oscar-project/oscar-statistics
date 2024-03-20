use bytecount;
use clap::Parser;
use oscar_io::v3::Document;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};
use tokio::task::JoinSet;
use walkdir::{DirEntry, WalkDir};

mod cli;

fn counter(file: DirEntry, db: Arc<Mutex<HashMap<String, HashMap<String, (u64, u64)>>>>) {
    let path = file.path();
    let components: Vec<_> = path
        .components()
        .rev()
        .map(|comp| comp.as_os_str())
        .collect();
    let lang = components[1].to_str().unwrap();
    let lang = lang.strip_suffix("_meta").unwrap().to_string();
    let snapshot = components[2].to_str().unwrap().to_string();

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
        num_toks += u64::try_from(bytecount::count(content.as_bytes(), b' ')).unwrap();
        num_toks += u64::try_from(bytecount::count(content.as_bytes(), b'\n')).unwrap();
        num_docs += 1;
    }
    db.lock()
        .unwrap()
        .entry(lang.clone())
        .or_insert(HashMap::new())
        .entry(snapshot.clone())
        .and_modify(|e| {
            e.0 += num_docs;
            e.1 += num_toks;
        })
        .or_insert((num_docs, num_toks));
    println!(
        "{}\t{}\t{:?}\t{}\t{}",
        lang,
        snapshot,
        path.file_name().unwrap(),
        num_docs,
        num_toks
    );
}

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();

    let mut set = JoinSet::new();

    let db: Arc<Mutex<HashMap<String, HashMap<String, (u64, u64)>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let file_paths: Vec<DirEntry> = WalkDir::new(args.folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
        .collect();

    for file in file_paths {
        let db = db.clone();
        set.spawn(async move {
            counter(file, db);
        });
    }

    while let Some(res) = set.join_next().await {
        res.unwrap();
    }

    let mut dst = File::create(args.output).unwrap();

    let json = serde_json::to_string_pretty(&*db.lock().unwrap()).unwrap();

    dst.write_all(json.as_bytes()).unwrap();

    println!("{:?}", db.lock().unwrap());
}
