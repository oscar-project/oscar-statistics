use clap::Parser;
use oscar_io::v3::Document;
use std::collections::HashMap;
use std::collections::HashSet;
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

#[derive(Debug)]
pub enum Error {
    SerdeJson(serde_json::Error),
    IoError(std::io::Error),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::SerdeJson(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}

fn counter(
    file: DirEntry,
    db: Arc<Mutex<HashMap<String, HashMap<String, (u64, u64)>>>>,
) -> Result<(), Error> {
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
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                eprintln!("Error reading line in file {:?}: {}", path, e);
                return Err(Error::IoError(e));
            }
        };
        let doc = match serde_json::from_str::<Document>(&line) {
            Ok(doc) => doc,
            Err(e) => {
                eprintln!("Error parsing document in file {:?}: {}", path, e);
                return Err(Error::SerdeJson(e));
            }
        };
        let content = doc.content();
        let words = content.split_whitespace().count();
        num_toks += u64::try_from(words).unwrap();
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
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();

    let co1 = HashSet::from([
        "2015-14", "2016-40", "2017-43", "2018-47", "2019-22", "2020-24", "2020-45", "2021-49",
        "2022-27", "2022-49", "2023-14", "2023-23",
    ]);

    let mut set = JoinSet::new();

    let db: Arc<Mutex<HashMap<String, HashMap<String, (u64, u64)>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let file_paths: Vec<DirEntry> = WalkDir::new(args.folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
        .filter(|e| {
            let components: Vec<_> = e
                .path()
                .components()
                .rev()
                .map(|comp| comp.as_os_str())
                .collect();
            let snapshot = components[2].to_str().unwrap();
            co1.contains(snapshot)
        })
        .collect();

    for file in file_paths {
        let db = db.clone();
        set.spawn(async move {
            let res = counter(file, db);
            match res {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        match res {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
    }

    let mut dst = File::create(args.output).unwrap();

    let json = serde_json::to_string_pretty(&*db.lock().unwrap()).unwrap();

    dst.write_all(json.as_bytes()).unwrap();

    println!("{:?}", db.lock().unwrap());
}
