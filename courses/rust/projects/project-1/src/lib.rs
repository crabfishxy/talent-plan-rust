use core::panic;
use std::{collections::{BTreeMap, HashMap}, fmt::Result, fs::{self, File}, io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::PathBuf, string};
use anyhow::Result;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const log_path: PathBuf = PathBuf::from("kvs_file");

pub struct Entry {
    // log file file_id
    file_id: u64,
    // value size
    value_size: u64,
    // value position
    value_pos: u64,
}

pub enum Command {
    SET {key: String, value: String},
    REMOVE {key: String},
}

pub struct KvStore {
    index: BTreeMap<String, Entry>,
    reader_map: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,
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


fn load(file_id: u64, reader: &mut BufReaderWithPos<File>, index: &mut BTreeMap<String, Entry>) -> (Result<u64>) {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::SET {key, ..} => {

            }
            Command::REMOVE {key, ..} => {
                
            }
        }    
    }
    Ok(())
}

struct BufReaderWithPos<R: Read+Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read+Seek> BufReaderWithPos<R> {
    fn new(inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos{
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read+Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl <R: Read+Seek> Seek for BufReaderWithPos<R> {

    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write+Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl <W: Write+Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos = len as u64;
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
} 


impl <W: Write+Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64>{
        self.pos = self.writer.seek(pos)?;     
        Ok(self.pos)
    }
} 

