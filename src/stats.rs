use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    ops::AddAssign,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arrow::{
    array::{ArrayRef, RecordBatch, StringBuilder, StructArray, UInt64Builder},
    datatypes::{DataType, Field},
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio::{sync::Semaphore, task::JoinSet};
use walkdir::{DirEntry, WalkDir};

use crate::{errors::Error, oscar::Document};

// Converts OscarBuilder` into `StructArray`
#[derive(Debug, Default)]
struct StatsBuider {
    snapshot: StringBuilder,
    lang: StringBuilder,
    num_docs: UInt64Builder,
    num_toks: UInt64Builder,
    num_bytes: UInt64Builder,
    num_chars: UInt64Builder,
}

impl StatsBuider {
    fn append(&mut self, file_stats: &FileStats) {
        self.snapshot.append_value(file_stats.snapshot.as_str());
        self.lang.append_value(file_stats.lang.as_str());
        self.num_docs.append_value(file_stats.num_docs);
        self.num_toks.append_value(file_stats.num_toks);
        self.num_bytes.append_value(file_stats.num_bytes);
        self.num_chars.append_value(file_stats.num_chars);
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    fn finish(&mut self) -> StructArray {
        let snapshot = Arc::new(self.snapshot.finish()) as ArrayRef;
        let snapshot_field = Arc::new(Field::new("snapshot", DataType::Utf8, false));

        let lang = Arc::new(self.lang.finish()) as ArrayRef;
        let lang_field = Arc::new(Field::new("lang", DataType::Utf8, false));

        let num_docs = Arc::new(self.num_docs.finish()) as ArrayRef;
        let num_docs_field = Arc::new(Field::new("num_docs", DataType::Int64, false));

        let num_toks = Arc::new(self.num_toks.finish()) as ArrayRef;
        let num_toks_field = Arc::new(Field::new("num_toks", DataType::Int64, false));

        let num_bytes = Arc::new(self.num_bytes.finish()) as ArrayRef;
        let num_bytes_field = Arc::new(Field::new("num_bytes", DataType::Int64, false));

        let num_chars = Arc::new(self.num_chars.finish()) as ArrayRef;
        let num_chars_field = Arc::new(Field::new("num_chars", DataType::Int64, false));

        StructArray::from(vec![
            (snapshot_field, snapshot),
            (lang_field, lang),
            (num_docs_field, num_docs),
            (num_toks_field, num_toks),
            (num_bytes_field, num_bytes),
            (num_chars_field, num_chars),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct FileStats {
    snapshot: String,
    lang: String,
    num_docs: u64,
    num_toks: u64,
    num_bytes: u64,
    num_chars: u64,
}

impl AddAssign for FileStats {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            snapshot: self.snapshot.clone(),
            lang: self.lang.clone(),
            num_docs: self.num_docs + other.num_docs,
            num_toks: self.num_toks + other.num_toks,
            num_bytes: self.num_bytes + other.num_bytes,
            num_chars: self.num_chars + other.num_chars,
        }
    }
}

async fn counter(
    file: DirEntry,
    db: Arc<Mutex<HashMap<String, FileStats>>>,
    snapshot: String,
) -> Result<(), Error> {
    let path = file.path();

    println!("Processing: {}", path.to_str().unwrap());

    let reader = {
        let file = File::open(file.path()).unwrap();
        let decoder = zstd::Decoder::new(file).unwrap();
        BufReader::new(decoder)
    };

    let mut lang = String::new();
    let mut num_docs = 0;
    let mut num_toks = 0;
    let mut num_bytes = 0;

    for line in reader.lines() {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                eprintln!(
                    "Error reading line in file {:?} in line {}: {}",
                    path, num_docs, e
                );
                return Err(Error::IoError(e));
            }
        };
        let doc = match serde_json::from_str::<Document>(&line) {
            Ok(doc) => doc,
            Err(e) => {
                eprintln!(
                    "Error parsing document in file {:?} in line {}: {}",
                    path, num_docs, e
                );
                return Err(Error::SerdeJson(e));
            }
        };
        let content = doc.content;
        lang = doc.metadata.identification.label;
        num_bytes += u64::try_from(content.len()).unwrap();
        num_toks += u64::try_from(content.split_whitespace().count()).unwrap();
        num_docs += 1;
    }

    let file_stats = FileStats {
        snapshot: snapshot,
        lang: lang.clone(),
        num_docs: num_docs,
        num_toks: num_toks,
        num_bytes: num_bytes,
        num_chars: num_bytes,
    };

    db.lock()
        .unwrap()
        .entry(lang.clone())
        .and_modify(|e| *e += file_stats.clone())
        .or_insert(file_stats);
    println!("Finished processing: {}", path.to_str().unwrap());
    println!(
        "Stats: \n lang: {} \n num_docs: {} \n num_toks: {} \n num_bytes: {} \n num_chars: {}",
        lang, num_docs, num_toks, num_bytes, num_bytes
    );
    Ok(())
}

pub async fn compute_stats(src: &PathBuf, dst: &PathBuf, snapshot: String, threads: usize) {
    let mut set = JoinSet::new();

    let db: Arc<Mutex<HashMap<String, FileStats>>> = Arc::new(Mutex::new(HashMap::new()));

    let semaphore = Arc::new(Semaphore::new(threads));

    let file_paths: Vec<DirEntry> = WalkDir::new(src)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
        .collect();

    for file in file_paths {
        let db = db.clone();
        let snapshot = snapshot.clone();
        let semaphore = semaphore.clone();
        set.spawn(async move {
            let _permit = semaphore.acquire().await;
            counter(file, db, snapshot).await
        });
    }

    while let Some(res) = set.join_next().await {
        match res {
            Ok(e) => match e {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                }
            },
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
    }

    println!("{:?}", db.lock().unwrap());

    let parquet = File::create(dst).unwrap();

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut builder = StatsBuider::default();

    for (_, stats) in db.lock().unwrap().iter() {
        builder.append(stats);
    }

    let batch = RecordBatch::from(builder.finish());

    let mut writer = ArrowWriter::try_new(parquet, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
}
