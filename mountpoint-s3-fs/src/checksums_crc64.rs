use std::ops::{Bound, Range, RangeBounds};

use bytes::{Bytes, BytesMut};
use crc_fast::{CrcAlgorithm, checksum_combine};
use mountpoint_s3_client::checksums::crc64nvme::{self, Crc64nvme};

use thiserror::Error;

/// A `Crc64ChecksummedBytes` is a bytes buffer that carries its CRC64-NVME checksum.
/// The implementation guarantees that integrity will be validated before the data can be accessed.
/// Data transformations will either fail returning an [IntegrityError], or propagate the checksum
/// so that it can be validated on access.
#[derive(Clone, Debug)]
#[must_use]
pub struct Crc64ChecksummedBytes {
    /// Underlying buffer
    buffer: Bytes,
    /// Range over [Self::buffer]
    range: Range<usize>,
    /// Checksum for [Self::buffer]
    checksum: Crc64nvme,
}

impl Crc64ChecksummedBytes {
    /// Create a new [Crc64ChecksummedBytes] from the given [Bytes] and pre-calculated checksum.
    /// To be used for de-serialization.
    pub fn new_from_inner_data(bytes: Bytes, checksum: Crc64nvme) -> Self {
        let full_range = 0..bytes.len();
        Self {
            buffer: bytes,
            range: full_range,
            checksum,
        }
    }

    /// Create [Crc64ChecksummedBytes] from [Bytes], calculating its checksum.
    pub fn new(bytes: Bytes) -> Self {
        let checksum = crc64nvme::checksum(&bytes);
        Self::new_from_inner_data(bytes, checksum)
    }

    /// Convert the [Crc64ChecksummedBytes] into [Bytes], data integrity will be validated before converting.
    ///
    /// Return [IntegrityError] on data corruption.
    pub fn into_bytes(self) -> Result<Bytes, IntegrityError> {
        self.validate()?;
        Ok(self.buffer_slice())
    }

    /// Returns the number of bytes contained in this [Crc64ChecksummedBytes].
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns true if the [Crc64ChecksummedBytes] has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Split off the checksummed bytes at the given index.
    ///
    /// This operation just increases the reference count and sets a few indices,
    /// so there will be no validation and the checksum will not be recomputed.
    pub fn split_off(&mut self, at: usize) -> Crc64ChecksummedBytes {
        assert!(at < self.len());

        let new_start = self.range.start + at;
        let new_range = new_start..self.range.end;
        self.range = self.range.start..new_start;

        Crc64ChecksummedBytes {
            buffer: self.buffer.clone(),
            range: new_range,
            checksum: self.checksum,
        }
    }

    /// Create a new [Crc64ChecksummedBytes] for a slice of the original buffer.
    ///
    /// This operation just increases the reference count and sets a few indices,
    /// so there will be no validation and the checksum will not be recomputed.
    ///
    /// Will panic if the provided range is out of bounds.
    pub fn slice<R: RangeBounds<usize>>(&self, range: R) -> Crc64ChecksummedBytes {
        let len = self.len();

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        assert!(start <= len);

        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };
        assert!(end <= len);
        assert!(start <= end);

        let new_start = self.range.start + start;
        let new_end = self.range.start + end;

        Crc64ChecksummedBytes {
            buffer: self.buffer.clone(),
            range: new_start..new_end,
            checksum: self.checksum,
        }
    }

    /// "Shrink" a [Crc64ChecksummedBytes] to fit exactly the size of its range. If the range is not the
    /// full buffer range, this will extract a new buffer that contains only the bytes in the range.
    /// Will validate the buffer before shrinking it.
    ///
    /// Return [IntegrityError] on data corruption.
    pub fn shrink_to_fit(&mut self) -> Result<(), IntegrityError> {
        if self.len() == self.buffer.len() {
            return Ok(());
        }

        // Note that no data is copied: `bytes` still points to a subslice of `buffer`.
        let bytes = self.buffer_slice();
        let checksum = crc64nvme::checksum(&bytes);

        // Check the integrity of the whole buffer.
        self.validate()?;

        // Create a new buffer with just the subslice, and a new range to match.
        self.buffer = bytes;
        self.range = 0..self.buffer.len();
        self.checksum = checksum;

        Ok(())
    }

    /// Append the given checksummed bytes to current [Crc64ChecksummedBytes]. Will combine the
    /// existing checksums if possible, or compute a new one and validate data integrity.
    ///
    /// Return [IntegrityError] if data corruption is detected.
    pub fn extend(&mut self, mut extend: Crc64ChecksummedBytes) -> Result<(), IntegrityError> {
        if extend.is_empty() {
            // No op, but check that `extend` was not corrupted
            extend.validate()?;
            return Ok(());
        }

        if self.is_empty() {
            // Replace with `extend`, but check that `self` was not corrupted
            self.validate()?;
            *self = extend;
            return Ok(());
        }

        // When appending two slices, we can combine their checksums and obtain the new checksum
        // without having to recompute it from the data.
        // However, since a `Crc64ChecksummedBytes` potentially holds the checksum of some larger buffer,
        // rather than the exact one for the slice, we need to first invoke `shrink_to_fit` on each
        // slice and use the resulting exact checksums.
        self.shrink_to_fit()?;
        assert_eq!(self.buffer.len(), self.len());
        extend.shrink_to_fit()?;
        assert_eq!(extend.buffer.len(), extend.len());

        // Combine the checksums using crc-fast's checksum_combine function
        let new_checksum = combine_checksums(self.checksum, extend.checksum, extend.len());

        // Combine the slices.
        let new_bytes = {
            let mut bytes_mut = BytesMut::with_capacity(self.len() + extend.len());
            bytes_mut.extend_from_slice(&self.buffer);
            bytes_mut.extend_from_slice(&extend.buffer);
            bytes_mut.freeze()
        };

        let new_range = 0..(new_bytes.len());
        *self = Self {
            buffer: new_bytes,
            range: new_range,
            checksum: new_checksum,
        };
        Ok(())
    }

    /// Validate data integrity in this [Crc64ChecksummedBytes].
    ///
    /// Return [IntegrityError] on data corruption.
    pub fn validate(&self) -> Result<(), IntegrityError> {
        let checksum = crc64nvme::checksum(&self.buffer);
        if self.checksum != checksum {
            return Err(IntegrityError::ChecksumMismatch(self.checksum, checksum));
        }
        Ok(())
    }

    /// Extract the underlying byte buffer and checksum.
    /// You should only use this if you intend to serialize the buffer.
    /// Validation may or may not be triggered, and **bytes or checksum may be corrupt** even if result returns [Ok].
    ///
    /// If you are only interested in the underlying bytes, **you should use `into_bytes()`**.
    pub fn into_inner(mut self) -> Result<(Bytes, Crc64nvme), IntegrityError> {
        self.shrink_to_fit()?;
        Ok((self.buffer, self.checksum))
    }

    /// Return the slice of `buffer` corresponding to `range`.
    ///
    /// Note that no data is copied: the returned `Bytes` still points to a subslice of `buffer`.
    fn buffer_slice(&self) -> Bytes {
        self.buffer.slice(self.range.clone())
    }
}

impl Default for Crc64ChecksummedBytes {
    fn default() -> Self {
        Self {
            buffer: Default::default(),
            range: Default::default(),
            checksum: Crc64nvme::new(0),
        }
    }
}

impl From<Bytes> for Crc64ChecksummedBytes {
    fn from(value: Bytes) -> Self {
        Self::new(value)
    }
}

impl TryFrom<Crc64ChecksummedBytes> for Bytes {
    type Error = IntegrityError;

    fn try_from(value: Crc64ChecksummedBytes) -> Result<Self, Self::Error> {
        value.into_bytes()
    }
}

/// Calculates the combined checksum for `AB` where `prefix_crc` is the checksum for `A`,
/// `suffix_crc` is the checksum for `B`, and `suffix_len` is the length of `B`.
///
/// This function efficiently combines CRC64 checksums using the crc-fast crate,
/// which provides hardware-accelerated implementations where available.
pub fn combine_checksums(prefix_crc: Crc64nvme, suffix_crc: Crc64nvme, suffix_len: usize) -> Crc64nvme {
    // CRC64-NVME uses the Rocksoft model and is equivalent to CrcAlgorithm::Crc64Nvme in the crc-fast crate
    // Use the checksum_combine function which efficiently combines checksums
    let combined = checksum_combine(
        CrcAlgorithm::Crc64Nvme,
        prefix_crc.value(),
        suffix_crc.value(),
        suffix_len as u64,
    );

    Crc64nvme::new(combined)
}

#[derive(Debug, Error)]
pub enum IntegrityError {
    #[error("Checksum mismatch. expected: {0:?}, actual: {1:?}")]
    ChecksumMismatch(Crc64nvme, Crc64nvme),
}

// Implement equality for tests only. We implement equality, and will panic if the data is corrupted.
#[cfg(test)]
impl PartialEq for Crc64ChecksummedBytes {
    fn eq(&self, other: &Self) -> bool {
        let result = self.buffer_slice() == other.buffer_slice();
        self.validate().expect("should be valid");
        other.validate().expect("should be valid");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_bytes() {
        let bytes = Bytes::from_static(b"some bytes");
        let expected = bytes.clone();
        let checksummed_bytes = Crc64ChecksummedBytes::new(bytes);

        let actual = checksummed_bytes.into_bytes().unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_into_bytes_integrity_error() {
        let bytes = Bytes::from_static(b"some bytes");
        let mut checksummed_bytes = Crc64ChecksummedBytes::new(bytes);

        // alter the content
        checksummed_bytes.buffer = Bytes::from_static(b"otherbytes");

        let actual = checksummed_bytes.into_bytes();
        assert!(matches!(actual, Err(IntegrityError::ChecksumMismatch(_, _))));
    }

    #[test]
    fn test_split_off() {
        let split_off_at = 4;
        let bytes = Bytes::from_static(b"some bytes");
        let expected = bytes.clone();
        let expected_checksum = crc64nvme::checksum(&expected);
        let mut checksummed_bytes = Crc64ChecksummedBytes::new(bytes);

        let mut expected_part1 = expected.clone();
        let expected_part2 = expected_part1.split_off(split_off_at);
        let new_checksummed_bytes = checksummed_bytes.split_off(split_off_at);

        assert_eq!(expected, checksummed_bytes.buffer);
        assert_eq!(expected, new_checksummed_bytes.buffer);
        assert_eq!(expected_part1, checksummed_bytes.buffer_slice());
        assert_eq!(expected_part2, new_checksummed_bytes.buffer_slice());
        assert_eq!(expected_checksum, checksummed_bytes.checksum);
        assert_eq!(expected_checksum, new_checksummed_bytes.checksum);
    }

    #[test]
    fn test_combine_checksums() {
        let buf: &[u8] = b"123456789";
        let (buf1, buf2) = buf.split_at(4);
        let crc = crc64nvme::checksum(buf);
        let crc1 = crc64nvme::checksum(buf1);
        let crc2 = crc64nvme::checksum(buf2);
        let combined = combine_checksums(crc1, crc2, buf2.len());
        assert_eq!(crc, combined);
    }
}
