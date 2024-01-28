#![feature(error_generic_member_access)]

mod error;

use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
};

pub use error::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Database {
    // The path that holds all the segments
    path: PathBuf,

    // An in memory `BTreeMap` of all the keys + their index in the current dirty segment
    memtable: BTreeMap<Vec<u8>, usize>,
    dirty: File,
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
            path: dir.to_owned(),
            memtable: Self::init_memtable(&mut dirty)?,
            dirty,
        })
    }

    fn init_memtable(dirty: &mut File) -> Result<BTreeMap<Vec<u8>, usize>> {
        let mut memtable = BTreeMap::new();
        let mut reader = BufReader::new(dirty);

        let mut current_position = 0;
        let mut key_buf = Vec::new();

        loop {
            let key_size = match read_u32(&mut reader) {
                Ok(size) => size as usize,
                // We went through the whole dirty entries, we can stop
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    println!("{e}");
                    return Err(e.into());
                }
            };

            read_bytes(&mut reader, key_size, &mut key_buf)?;
            memtable.insert(key_buf.clone(), current_position);

            let value_size = read_u32(&mut reader)? as usize;
            io::copy(
                &mut reader.by_ref().take(value_size as u64),
                &mut io::sink(),
            )?;

            // increase the current position by the size of the entry
            // aka: the size _of the size_ of the key and value + the size of the key + the size of the value
            current_position += mem::size_of::<u32>() * 2 + key_size + value_size;
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

        self.dirty.write_all(&(key.len() as u32).to_be_bytes())?;
        self.dirty.write_all(key)?;
        self.dirty.write_all(&(value.len() as u32).to_be_bytes())?;
        self.dirty.write_all(value)?;
        Ok(())
    }

    pub fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        self.prepare_to_read()?;
        let mut key_buf = Vec::new();

        loop {
            let size = match read_u32(&mut self.dirty) {
                Ok(size) => size as usize,
                // we didn't find the element even thought we went through the whole segment
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => {
                    println!("{e}");
                    return Err(e.into());
                }
            };

            // If the size are the same we must check the if the values are the same as well
            if size == key.len() {
                read_bytes(&mut self.dirty, size, &mut key_buf)?;

                // we found the right value
                if key == key_buf {
                    // we'll re-use the key buffer to return the value
                    read_entry(&mut self.dirty, &mut key_buf)?;
                    return Ok(Some(key_buf));
                } else {
                    // We didn't find the value, we can move to the start of the next one
                    self.dirty.seek(SeekFrom::Current(size as i64))?;
                    skip_entry(&mut self.dirty)?;
                }
            } else {
                println!("at index {size}");
                // We the key doesn't match, we can move to the start of the next one
                self.dirty.seek(SeekFrom::Current(size as i64))?;
                // We must then skip the value
                skip_entry(&mut self.dirty)?;
            }
        }
    }

    fn prepare_to_add(&mut self) -> io::Result<()> {
        self.dirty.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn prepare_to_read(&mut self) -> io::Result<()> {
        self.dirty.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    fn dump(&mut self) -> io::Result<Vec<u8>> {
        self.prepare_to_read()?;
        let mut buf = Vec::new();
        self.dirty.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

/// Use the buffer provided to read your key and return the value in a
fn read_entry(reader: &mut impl Read, buf: &mut Vec<u8>) -> io::Result<()> {
    let size = read_u32(reader)?;
    read_bytes(reader, size as usize, buf)?;
    Ok(())
}

/// Use the buffer provided to read your key and return the value in a
fn read_bytes(reader: &mut impl Read, size: usize, buf: &mut Vec<u8>) -> io::Result<()> {
    buf.reserve(size);
    unsafe {
        // TODO: probably not safe since I didn't initialize the u8 in it
        buf.set_len(size);
    }
    reader.read_exact(buf)?;
    Ok(())
}

/// Use the buffer provided to read your key and return the value in a
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
        insta::assert_debug_snapshot!(database.dump().unwrap(), @r###"
        [
            0,
            0,
            0,
            5,
            104,
            101,
            108,
            108,
            111,
            0,
            0,
            0,
            5,
            119,
            111,
            114,
            108,
            100,
        ]
        "###);

        let v = database.get(b"hello").map_err(|e| println!("{e}")).unwrap();
        assert_eq!(v.as_deref(), Some(&b"world"[..]));
        let v = database.get(b"hemlo").map_err(|e| println!("{e}")).unwrap();
        assert_eq!(v.as_deref(), None);
    }
}
