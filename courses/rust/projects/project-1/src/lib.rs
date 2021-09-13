use anyhow::{Error, Result};
use core::panic;
use std::{any, collections::{BTreeMap, HashMap}, ffi::OsStr, fs::{self, File, OpenOptions}, io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, ops::Range, path::{Path, PathBuf}, string};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

// const log_path: PathBuf = PathBuf::from("kvs_file");

#[derive(Debug)]
pub struct CommandEntry {
    // log file file_id
    file_id: u64,
    // value position
    cmd_pos: u64,
    // value size
    cmd_size: u64,
}

impl From<(u64, Range<u64>)> for CommandEntry {
    fn from((file_id, range): (u64, Range<u64>)) -> Self {
        CommandEntry {
            file_id,
            cmd_pos: range.start,
            cmd_size: range.end - range.start,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    SET { key: String, value: String },
    REMOVE { key: String },
}

pub struct KvStore {
    path: PathBuf,
    index: BTreeMap<String, CommandEntry>,
    reader_map: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    uncompacted: u64,
}

impl KvStore {
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_entry) = self.index.get(&key) {
            let reader = self
                .reader_map
                .get_mut(&cmd_entry.file_id)
                .expect("can't find reader");
            reader.seek(SeekFrom::Start(cmd_entry.cmd_pos))?;
            let cmd_reader = reader.take(cmd_entry.cmd_size);
            if let Command::SET { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(Error::msg("message"))
            }
        } else {
            Ok(None)
        }
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::SET { key, value: val };
        let pos = self.writer.pos;
        //println!{"old pos: {}", pos};
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        //println!{"new pos: {}", self.writer.pos};
        if let Command::SET { key, .. } = cmd {
            if let Some(old_cmd) = self.index.insert(
                key,
                CommandEntry::from((self.current_gen, pos..self.writer.pos)),
            ) {
                self.uncompacted += old_cmd.cmd_size;
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(cmd_entry) = self.index.get(&key) {
            let cmd = Command::REMOVE { key };
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &cmd);
            self.writer.flush()?;
            if let Command::REMOVE { key, .. } = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.cmd_size;
            }
            Ok(())
        } else {
            Err(Error::msg("key not found"))
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        //print!("path: {:?}", path);
        fs::create_dir_all(&path)?;

        let mut reader_map = HashMap::new();
        let mut index = BTreeMap::new();

        let mut uncompacted = 0;

        let log_list = sorted_gen_list(&path)?;
        for &log in &log_list {
            let mut reader = BufReaderWithPos::new(File::open(path.join(format!("{}.log", log)))?)?;
            uncompacted += load(log, &mut reader, &mut index)?;
            reader_map.insert(log, reader);
        }

        let current_gen =log_list.last().unwrap_or(&0) + 1;
        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path.join(format!("{}.log", current_gen)))?,
        )?;

        let mut reader = BufReaderWithPos::new(File::open(path.join(format!("{}.log",current_gen)))?)?;
        uncompacted += load(0, &mut reader, &mut index)?;
        reader_map.insert(current_gen, reader);
        Ok(KvStore {
            path,
            reader_map,
            writer,
            current_gen,
            uncompacted,
            index,
        })
    }

    pub fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;

        self.writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(self.path.join(format!("{}.log", self.current_gen)))?,
        )?;

        let mut compaction_writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(self.path.join(format!("{}.log", compaction_gen)))?,)?;

        let mut new_pos = 0; // pos in the new log file.
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .reader_map
                .get_mut(&cmd_pos.file_id)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.cmd_pos {
                reader.seek(SeekFrom::Start(cmd_pos.cmd_pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.cmd_size);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log files.
        let stale_gens: Vec<_> = self
            .reader_map
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.reader_map.remove(&stale_gen);
            fs::remove_file(&self.path.join(format!("{}.log",stale_gen)))?;
        }
        self.uncompacted = 0;

        Ok(())
    }
}

fn load(
    file_id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandEntry>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::SET { key, .. } => {
                if let Some(old_cmd) = index.insert(
                    key,
                    CommandEntry::from((
                        file_id,
                        Range {
                            start: pos,
                            end: new_pos,
                        },
                    )),
                ) {
                    uncompacted += old_cmd.cmd_size;
                }
            }
            Command::REMOVE { key, .. } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.cmd_size;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
