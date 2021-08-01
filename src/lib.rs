use core::panic;
use std::{collections::HashMap, fs, io::{BufReader, BufWriter}, path::PathBuf};
use anyhow::Result;

const log_path: PathBuf = PathBuf::from("kvs_file");

pub struct Entry {
    // log file filed_id
    field_id: u64,
    // value size
    value_size: u64,
    // value position
    value_pos: u64,
}

pub struct KvStore {
    index: HashMap<String, Entry>,
    reader_map: HashMap<u64, BufReader<>>,
    writer: BufWriter<>,

}

impl KvStore {
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(&key);
        panic!();
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(path)?;

    }

}
