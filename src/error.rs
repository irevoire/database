use std::{backtrace::Backtrace, io};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{source}: {backtrace}")]
    Io {
        #[from]
        source: io::Error,
        backtrace: Backtrace,
    },
    #[error("{source}: {backtrace}")]
    TempFile {
        #[from]
        source: tempfile::PersistError,
        backtrace: Backtrace,
    },
    #[error("Key too large {0}. Maximum size accepted is {}", u32::MAX)]
    KeyTooLarge(usize),

    #[error("Value too large {0}. Maximum size accepted is {}", u32::MAX)]
    ValueTooLarge(usize),
}
