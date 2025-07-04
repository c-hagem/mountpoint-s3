use std::ptr::NonNull;

use mountpoint_s3_crt_sys::{
    AWS_SHA1_LEN, aws_hash, aws_hash_destroy, aws_hash_finalize, aws_hash_update, aws_sha1_new,
};

use crate::common::allocator::Allocator;
use crate::common::byte_buf::ByteBuf;
use crate::common::error::Error;
use crate::{CrtError as _, ToAwsByteCursor};

/// SHA1 checksum
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sha1([u8; Self::LENGTH]);

impl Sha1 {
    /// Length of a SHA1 checksum.
    pub const LENGTH: usize = AWS_SHA1_LEN as usize;

    /// Create a SHA1 checksum.
    pub fn new(value: [u8; Self::LENGTH]) -> Self {
        Self(value)
    }

    /// The binary value
    pub fn value(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }
}

/// Computes the SHA1 checksum of a byte slice.
///
/// Use [Sha1Hasher] for more advanced use-cases.
pub fn checksum(buf: &[u8]) -> Result<Sha1, Error> {
    let allocator = Allocator::default();
    let mut hasher = Sha1Hasher::new(&allocator)?;
    hasher.update(buf)?;
    hasher.finalize(&allocator)
}

/// SHA1 Hasher
#[derive(Debug)]
pub struct Sha1Hasher {
    inner: NonNull<aws_hash>,
}

/// Safety: [Sha1Hasher] is not [Clone] and owns the inner [aws_hash].
unsafe impl Send for Sha1Hasher {}

impl Sha1Hasher {
    /// Create a new [Sha1Hasher].
    pub fn new(allocator: &Allocator) -> Result<Self, Error> {
        // SAFETY: allocator is a valid aws_allocator, and we check the return is non-null.
        let inner = unsafe { aws_sha1_new(allocator.inner.as_ptr()).ok_or_last_error()? };
        Ok(Self { inner })
    }

    /// Update the hash state with the given bytes slice.
    pub fn update(&mut self, buf: &[u8]) -> Result<(), Error> {
        // SAFETY: `self.inner` is a valid `aws_hash` and `buf` will outlive the call to `aws_hash_update`.
        unsafe { aws_hash_update(self.inner.as_ptr(), &buf.as_aws_byte_cursor()).ok_or_last_error() }
    }

    /// Finalize the hash state and return the computed SHA1 checksum value.
    pub fn finalize(self, allocator: &Allocator) -> Result<Sha1, Error> {
        let mut buffer = ByteBuf::new(allocator, Sha1::LENGTH)?;

        // SAFETY: `self.inner` is a valid `aws_hash` and `buffer` was initialized above.
        unsafe { aws_hash_finalize(self.inner.as_ptr(), buffer.as_mut_ptr(), 0).ok_or_last_error()? };

        // Slice will be copied into the struct.
        Ok(Sha1(buffer.as_slice().try_into().unwrap()))
    }
}

impl Drop for Sha1Hasher {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is a valid `aws_hash` and safe to destroy since it's the only reference.
        unsafe {
            aws_hash_destroy(self.inner.as_ptr());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_simple() {
        let buf: &[u8] = b"123456789";
        let sha1 = checksum(buf).expect("checksum failed");
        assert_eq!(
            sha1,
            Sha1([
                247, 195, 188, 29, 128, 142, 4, 115, 42, 223, 103, 153, 101, 204, 195, 76, 167, 174, 52, 65
            ])
        );
    }

    #[test]
    fn sha1_append() {
        let allocator = Allocator::default();
        let mut hasher = Sha1Hasher::new(&allocator).expect("hasher creation failed");
        hasher.update(b"1234").expect("hasher updated failed");
        hasher.update(b"56789").expect("hasher updated failed");
        let sha1 = hasher.finalize(&allocator).expect("hasher finalization failed");
        assert_eq!(
            sha1,
            Sha1([
                247, 195, 188, 29, 128, 142, 4, 115, 42, 223, 103, 153, 101, 204, 195, 76, 167, 174, 52, 65
            ])
        );
    }
}
