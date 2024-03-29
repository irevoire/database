#![feature(error_generic_member_access)]

mod error;

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
};

pub use error::Error;
use tempfile::NamedTempFile;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Database {
    /// When reached, rewrite the dirty segment as a clean segment
    dirty_thresholds: usize,

    // The path that holds all the segments
    path: PathBuf,

    // An in memory `BTreeMap` of all the keys + their index in the current dirty segment
    memtable: BTreeMap<Vec<u8>, u64>,
    dirty: File,
    segments: VecDeque<Segment>,
}

struct Segment {
    id: usize,
    file: File,
}

impl Segment {
    pub fn get(&mut self, key: &[u8], buf: &mut Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&mut self.file);

        loop {
            buf.clear();
            match read_entry(&mut reader, buf) {
                Ok(_) => (),
                // We went through the whole dirty entries, we can move to the next segment
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    println!("{e}");
                    return Err(e.into());
                }
            };
            if key == buf {
                // we found the entry
                read_entry(&mut reader, buf)?;
                return Ok(Some(buf.to_vec()));
            } else {
                skip_entry(&mut reader)?;
            }
        }

        Ok(None)
    }

    pub fn merge(writer: impl Write, new: &mut Self, old: &mut Self) -> io::Result<()> {
        let mut new_segment = BufWriter::new(writer);

        new.file.seek(SeekFrom::Start(0))?;
        old.file.seek(SeekFrom::Start(0))?;

        let mut new = BufReader::new(&mut new.file);
        let mut old = BufReader::new(&mut old.file);

        let mut new_key = read_entry_to_vec(&mut new)?;
        let mut old_key = read_entry_to_vec(&mut old)?;

        loop {
            if new_key <= old_key {
                new_segment.write_all(&(new_key.len() as u32).to_be_bytes())?;
                new_segment.write_all(&new_key)?;

                let value_size = read_u32(&mut new)?;
                new_segment.write_all(&value_size.to_be_bytes())?;
                io::copy(&mut new.by_ref().take(value_size as u64), &mut new_segment)?;

                if new_key == old_key {
                    // skip the value
                    skip_entry(&mut old)?;
                    // update the key
                    match read_entry(&mut old, &mut old_key) {
                        Ok(()) => (),
                        Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                            new_segment.write_all(&(new_key.len() as u32).to_be_bytes())?;
                            new_segment.write_all(&new_key)?;
                            io::copy(&mut new, &mut new_segment)?;
                            break;
                        }
                        Err(e) => return Err(e),
                    };
                }

                // read the next key in new_key
                match read_entry(&mut new, &mut new_key) {
                    Ok(()) => (),
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                        new_segment.write_all(&(old_key.len() as u32).to_be_bytes())?;
                        new_segment.write_all(&old_key)?;
                        io::copy(&mut old, &mut new_segment)?;
                        break;
                    }
                    Err(e) => return Err(e),
                };
            } else {
                new_segment.write_all(&(old_key.len() as u32).to_be_bytes())?;
                new_segment.write_all(&old_key)?;

                let value_size = read_u32(&mut old)?;
                new_segment.write_all(&value_size.to_be_bytes())?;
                io::copy(&mut old.by_ref().take(value_size as u64), &mut new_segment)?;

                // read the next key in old_key
                match read_entry(&mut old, &mut old_key) {
                    Ok(()) => (),
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                        new_segment.write_all(&(new_key.len() as u32).to_be_bytes())?;
                        new_segment.write_all(&new_key)?;
                        io::copy(&mut new, &mut new_segment)?;
                        break;
                    }
                    Err(e) => return Err(e),
                };
            }
        }
        Ok(())
    }

    pub fn dump(&mut self, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.clear();
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&mut self.file);
        reader.read_to_end(buf)?;
        Ok(())
    }
}

impl Database {
    pub fn new(dir: impl AsRef<Path>) -> Result<Database> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let mut dirty = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(dir.join("dirty"))?;

        Ok(Database {
            dirty_thresholds: 1024,
            path: dir.to_owned(),
            memtable: Self::init_memtable(&mut dirty)?,
            dirty,
            segments: VecDeque::new(),
        })
    }

    pub fn dirty_thresholds(&mut self, threshold: usize) {
        self.dirty_thresholds = threshold;
    }

    fn init_memtable(dirty: &mut File) -> Result<BTreeMap<Vec<u8>, u64>> {
        let mut memtable = BTreeMap::new();
        let mut reader = BufReader::new(dirty);

        let mut current_position = 0;
        let mut key_buf = Vec::new();

        loop {
            let key_size = match read_u32(&mut reader) {
                Ok(size) => size,
                // We went through the whole dirty entries, we can stop
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    println!("{e}");
                    return Err(e.into());
                }
            };

            read_bytes(&mut reader, key_size as usize, &mut key_buf)?;
            memtable.insert(key_buf.clone(), current_position);

            let value_size = read_u32(&mut reader)?;
            io::copy(
                &mut reader.by_ref().take(value_size as u64),
                &mut io::sink(),
            )?;

            // increase the current position by the size of the entry
            // aka: the size _of the size_ of the key and value + the size of the key + the size of the value
            current_position +=
                mem::size_of::<u32>() as u64 * 2 + key_size as u64 + value_size as u64;
        }

        Ok(memtable)
    }

    pub fn add(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let (key, value) = (key.as_ref(), value.as_ref());

        if key.len() > u32::MAX as usize {
            return Err(Error::KeyTooLarge(key.len()));
        }
        if value.len() > u32::MAX as usize {
            return Err(Error::KeyTooLarge(key.len()));
        }

        self.prepare_to_add()?;
        let pos = self.dirty.stream_position()?;

        // First we need to write everything on disk in case a crash happens
        write_entry(&mut self.dirty, key, value)?;
        // Then we can add it in the memtable
        self.memtable.insert(key.to_vec(), pos);

        if self.memtable.len() > self.dirty_thresholds {
            self.flush_dirty()?;
        }

        Ok(())
    }

    pub fn flush_dirty(&mut self) -> Result<()> {
        // We need to dump the dirty entries in a new segment

        // 1. Get a tempfile that'll be droped if something happens during the dumping operation
        let new_segment = NamedTempFile::new_in(&self.path)?;
        let mut writer = BufWriter::new(new_segment);

        // 1. Write all entries ordered by keys in a new file
        for (key, value) in self.memtable.iter() {
            self.dirty.seek(SeekFrom::Start(
                value + mem::size_of::<u32>() as u64 + key.len() as u64,
            ))?;
            let value = read_entry_to_vec(&mut self.dirty)?;

            write_entry(&mut writer, key, &value)?;
        }
        writer.flush()?;

        // 2. Clean the dirty segment
        self.memtable.clear();
        let next_id = self.segments.back().map_or(0, |segment| segment.id + 1);
        let new_segment = writer
            .into_inner()
            .unwrap()
            .persist(self.path.join(format!("segment-{next_id}")))?;
        self.dirty.set_len(0)?;

        // 3. Push the new file to the segment list
        self.segments.push_back(Segment {
            id: next_id,
            file: new_segment,
        });

        if self.segments.len() > 10 {
            self.merge_segment()?;
        }
        Ok(())
    }

    pub fn merge_segment(&mut self) -> Result<()> {
        // merge the first two segments
        let mut old = self.segments.pop_front().unwrap();
        let mut new = self.segments.pop_front().unwrap();
        let mut new_segment = NamedTempFile::new_in(&self.path)?;
        Segment::merge(&mut new_segment, &mut new, &mut old)?;
        let file = new_segment.persist(self.path.join(format!("segment-{}", old.id)))?;

        self.segments.push_front(Segment { id: old.id, file });

        Ok(())
    }

    pub fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let index = match self.memtable.get(key) {
            Some(index) => *index,
            None => return self.get_from_segments(key),
        };
        self.dirty.seek(SeekFrom::Start(
            // the index + skip the key
            index + mem::size_of::<u32>() as u64 + key.len() as u64,
        ))?;
        // and get the value
        let value = read_entry_to_vec(&mut self.dirty)?;

        Ok(Some(value))
    }

    fn get_from_segments(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut buf = Vec::new();
        // We want to go from the most recent segment to the most outdated one
        for segment in self.segments.iter_mut().rev() {
            if let Some(value) = segment.get(key, &mut buf)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn prepare_to_add(&mut self) -> io::Result<()> {
        self.dirty.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn prepare_to_read(&mut self) -> io::Result<()> {
        self.dirty.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    #[cfg(test)]
    fn dump(&mut self) -> io::Result<String> {
        let mut buf = String::new();
        buf.push_str(&format!("memtable:\n{:?}\n", self.memtable));

        let mut dirty_buf = Vec::new();
        self.prepare_to_read()?;
        self.dirty.read_to_end(&mut dirty_buf)?;

        buf.push_str(&format!("dirty segment:\n{dirty_buf:?}\n"));

        for (i, segment) in self.segments.iter_mut().enumerate() {
            segment.dump(&mut dirty_buf)?;
            buf.push_str(&format!("segment {i}:\n{dirty_buf:?}\n"));
        }

        Ok(buf)
    }
}

fn write_entry(mut writer: impl Write, key: &[u8], value: &[u8]) -> io::Result<()> {
    writer.write_all(&(key.len() as u32).to_be_bytes())?;
    writer.write_all(key)?;
    writer.write_all(&(value.len() as u32).to_be_bytes())?;
    writer.write_all(value)?;
    Ok(())
}

fn read_entry(reader: &mut impl Read, buf: &mut Vec<u8>) -> io::Result<()> {
    let size = read_u32(reader)?;
    read_bytes(reader, size as usize, buf)?;
    Ok(())
}

fn read_entry_to_vec(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    read_entry(reader, &mut buf)?;
    Ok(buf)
}

fn read_bytes(reader: &mut impl Read, size: usize, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.reserve(size);
    unsafe {
        // TODO: probably not safe since I didn't initialize the u8 in it
        buf.set_len(size);
    }
    reader.read_exact(buf)?;
    Ok(())
}

fn skip_entry(reader: &mut impl Read) -> io::Result<()> {
    let size = read_u32(reader)?;
    // we can't Seek thus we're throw away everything we've read
    io::copy(&mut reader.by_ref().take(size as u64), &mut io::sink())?;
    Ok(())
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut u32_buf = [0; 4];
    reader.read_exact(&mut u32_buf)?;
    let n = u32::from_be_bytes(u32_buf);
    Ok(n)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insert_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = Database::new(dir.path()).unwrap();

        database.add(b"hello", b"world").unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {[104, 101, 108, 108, 111]: 0}
        dirty segment:
        [0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100]
        "###);

        let v = database.get(b"hello").map_err(|e| println!("{e}")).unwrap();
        assert_eq!(v.as_deref(), Some(&b"world"[..]));
        let v = database.get(b"hemlo").map_err(|e| println!("{e}")).unwrap();
        assert_eq!(v.as_deref(), None);
        assert_eq!(v.as_deref(), None);
    }

    #[test]
    fn empty_key() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = Database::new(dir.path()).unwrap();

        database.add(b"", b"riengue").unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {[]: 0}
        dirty segment:
        [0, 0, 0, 0, 0, 0, 0, 7, 114, 105, 101, 110, 103, 117, 101]
        "###);

        let v = database.get(b"").map_err(|e| println!("{e}")).unwrap();
        assert_eq!(v.as_deref(), Some(&b"riengue"[..]));
    }

    #[test]
    fn empty_value() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = Database::new(dir.path()).unwrap();

        database.add(b"riengue", b"").unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {[114, 105, 101, 110, 103, 117, 101]: 0}
        dirty segment:
        [0, 0, 0, 7, 114, 105, 101, 110, 103, 117, 101, 0, 0, 0, 0]
        "###);

        let v = database
            .get(b"riengue")
            .map_err(|e| println!("{e}"))
            .unwrap();
        assert_eq!(v.as_deref(), Some(&b""[..]));
    }

    #[test]
    fn merge() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = Database::new(dir.path()).unwrap();

        // make a first segment
        database.add(b"hello", b"world").unwrap();
        database.add(b"tamo", b"kefir").unwrap();
        database.add(b"a", b"b").unwrap();
        database.flush_dirty().unwrap();

        // make a second clean segment out of the memtable
        database.add(b"hello", b"tamo").unwrap();
        database.add(b"b", b"c").unwrap();
        database.flush_dirty().unwrap();

        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {}
        dirty segment:
        []
        segment 0:
        [0, 0, 0, 1, 97, 0, 0, 0, 1, 98, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100, 0, 0, 0, 4, 116, 97, 109, 111, 0, 0, 0, 5, 107, 101, 102, 105, 114]
        segment 1:
        [0, 0, 0, 1, 98, 0, 0, 0, 1, 99, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4, 116, 97, 109, 111]
        "###);

        database.merge_segment().unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {}
        dirty segment:
        []
        segment 0:
        [0, 0, 0, 1, 97, 0, 0, 0, 1, 98, 0, 0, 0, 1, 98, 0, 0, 0, 1, 99, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4, 116, 97, 109, 111, 0, 0, 0, 4, 116, 97, 109, 111, 0, 0, 0, 5, 107, 101, 102, 105, 114]
        "###);
    }

    #[test]
    fn create_and_get_in_clean_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = Database::new(dir.path()).unwrap();
        database.dirty_thresholds(2);

        database.add(b"hello", b"world").unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {[104, 101, 108, 108, 111]: 0}
        dirty segment:
        [0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100]
        "###);
        database.add(b"tamo", b"world").unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {[104, 101, 108, 108, 111]: 0, [116, 97, 109, 111]: 18}
        dirty segment:
        [0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100, 0, 0, 0, 4, 116, 97, 109, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100]
        "###);
        database.add(b"patou", b"world").unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {}
        dirty segment:
        []
        segment 0:
        [0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100, 0, 0, 0, 5, 112, 97, 116, 111, 117, 0, 0, 0, 5, 119, 111, 114, 108, 100, 0, 0, 0, 4, 116, 97, 109, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100]
        "###);
        let v = database.get(b"hello").map_err(|e| println!("{e}")).unwrap();
        assert_eq!(v.as_deref(), Some(&b"world"[..]));
    }

    #[test]
    fn reload_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = Database::new(dir.path()).unwrap();

        database.add(b"hello", b"world").unwrap();
        database.add(b"tamo", b"world").unwrap();

        drop(database);
        // dropping the previous database and opening a new one in the same dir
        let mut database = Database::new(dir.path()).unwrap();
        insta::assert_display_snapshot!(database.dump().unwrap(), @r###"
        memtable:
        {[104, 101, 108, 108, 111]: 0, [116, 97, 109, 111]: 18}
        dirty segment:
        [0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100, 0, 0, 0, 4, 116, 97, 109, 111, 0, 0, 0, 5, 119, 111, 114, 108, 100]
        "###);
    }
}
