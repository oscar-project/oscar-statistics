use clap::Parser;
use oscar_io::v3::Document;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};
use walkdir::{DirEntry, WalkDir};

mod cli;

async fn counter(file: DirEntry, db: Arc<Mutex<HashMap<String, HashMap<String, (u64, u64)>>>>) {
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
        for char in content.chars() {
            if char.is_whitespace() {
                num_toks += 1;
            }
        }
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
        tokio::spawn(async move {
            counter(file, db).await;
        });
    }
    println!("{:?}", db.lock().unwrap());
}
