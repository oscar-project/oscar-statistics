# OSCAR Statistics

This is an experimental package to compute statistics on the OSCAR corpus releases. For the moment it only computes the statistics for a single snapshot that you have to specify as an argument. Computes the following statistics per language:

- Number of documents
- Number of tokens
- Number of bytes
- Number of characters

The output is a parquet file.

## Usage

```text
➜ ./target/release/oscar-statistics -h
Compute statistics of an OSCAR release

Usage: oscar-statistics [OPTIONS] <INPUT FOLDER> <DESTINATION FILE> <SNAPSHOT>

Arguments:
  <INPUT FOLDER>      Folder containing the indices
  <DESTINATION FILE>  Parquet file to write
  <SNAPSHOT>          Name of the snapshot

Options:
  -t, --threads <NUMBER OF THREADS>  Number of threads to use [default: 10]
  -h, --help                         Print help
  -V, --version                      Print version
```
