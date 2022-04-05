

use std::io;
use std::fmt;


#[derive(Debug)]
pub enum IWError {
    DataTooShort(usize),
    DataLengthMismatch(usize),
    InvalidDataHeader,
    IO(io::Error),
}

impl std::error::Error for IWError {

}

impl fmt::Display for IWError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      match self {
            IWError::DataTooShort(s) => write!(f, "Data too short:  '{}'", s),
            IWError::DataLengthMismatch(s) => write!(f, "Data length does not match:  '{}'", s),
            IWError::InvalidDataHeader => write!(f, "Invalid data header"),
            IWError::IO(e) => write!(f, "IO error: '{}'", e),
        }
    }
}

impl From<io::Error> for IWError {
    fn from(e: io::Error) -> Self {
     IWError::IO(e)
    }
}
