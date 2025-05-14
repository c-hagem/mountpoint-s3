use crate::superblock::path::ValidKey;
/// A MountSpace is a generalisation of a SuperBlock.
///
use crate::superblock::InodeNo;

use crate::superblock::inode::Expiry;
use crate::superblock::inode::InodeErrorInfo;
use crate::superblock::InodeError;
use crate::superblock::InodeKind;
use crate::sync::atomic::AtomicBool;
use mountpoint_s3_client::types::RestoreStatus;
use mountpoint_s3_client::ObjectClient;
use std::ffi::OsStr;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct InodeStat {
    /// Time this stat becomes invalid and needs to be refreshed
    pub expiry: Expiry,

    /// Size in bytes
    pub size: usize,

    /// Time of last file content modification
    pub mtime: OffsetDateTime,
    /// Time of last file metadata (or content) change
    pub ctime: OffsetDateTime,
    /// Time of last access
    pub atime: OffsetDateTime,
    /// Etag for the file (object)
    pub etag: Option<Box<str>>,
    /// Inodes corresponding to S3 objects with GLACIER or DEEP_ARCHIVE storage classes
    /// are only readable after restoration. For objects with other storage classes
    /// this field should be always `true`.
    pub is_readable: bool,
}

impl InodeStat {
    pub fn is_valid(&self) -> bool {
        !self.expiry.is_expired()
    }

    /// Objects in flexible retrieval storage classes can't be accessed via GetObject unless they are
    /// restored, and so we override their permissions to 000 and reject reads to them. We also warn
    /// the first time we see an object like this, because FUSE enforces the 000 permissions on our
    /// behalf so we might not see an attempted `open` call.
    fn is_readable(storage_class: Option<&str>, restore_status: Option<RestoreStatus>) -> bool {
        static HAS_SENT_WARNING: AtomicBool = AtomicBool::new(false);
        match storage_class {
            Some("GLACIER") | Some("DEEP_ARCHIVE") => {
                let restored =
                    matches!(restore_status, Some(RestoreStatus::Restored { expiry }) if expiry > SystemTime::now());
                if !restored && !HAS_SENT_WARNING.swap(true, Ordering::SeqCst) {
                    tracing::warn!(
                        "objects in the GLACIER and DEEP_ARCHIVE storage classes are only accessible if restored"
                    );
                }
                restored
            }
            _ => true,
        }
    }

    /// Initialize an [InodeStat] for a file, given some metadata.
    pub fn for_file(
        size: usize,
        datetime: OffsetDateTime,
        etag: Option<Box<str>>,
        storage_class: Option<&str>,
        restore_status: Option<RestoreStatus>,
        validity: Duration,
    ) -> InodeStat {
        let is_readable = Self::is_readable(storage_class, restore_status);
        InodeStat {
            expiry: Expiry::from_now(validity),
            size,
            atime: datetime,
            ctime: datetime,
            mtime: datetime,
            etag,
            is_readable,
        }
    }

    /// Initialize an [InodeStat] for a directory, given some metadata.
    pub fn for_directory(datetime: OffsetDateTime, validity: Duration) -> InodeStat {
        InodeStat {
            expiry: Expiry::from_now(validity),
            size: 0,
            atime: datetime,
            ctime: datetime,
            mtime: datetime,
            etag: None,
            is_readable: true,
        }
    }

    pub fn update_validity(&mut self, validity: Duration) {
        self.expiry = Expiry::from_now(validity);
    }
}

/// Result of a call to [Superblock::lookup] or [Superblock::getattr]. `stat` is a copy of the
/// inode's `stat` field that has already had its expiry checked and so is guaranteed to be valid
/// until `stat.expiry`.
#[derive(Debug, Clone)]
pub struct LookedUp {
    pub ino: InodeNo,
    pub full_key: ValidKey,
    pub stat: InodeStat,
    pub is_remote: bool,
}

impl LookedUp {
    pub fn kind(&self) -> InodeKind {
        self.full_key.kind()
    }

    pub fn name(&self) -> &str {
        self.full_key.name()
    }

    /// How much longer this lookup will be valid for
    pub fn validity(&self) -> Duration {
        self.stat.expiry.remaining_ttl()
    }

    pub fn err(&self) -> InodeErrorInfo {
        InodeErrorInfo::new(self.ino, self.full_key.clone())
    }
}

pub trait MountSpace<OC: ObjectClient> {
    // Lookup
    async fn lookup(&self, client: &OC, parent_ino: InodeNo, name: &OsStr) -> Result<LookedUp, InodeError>;

    // Functions for writing

    //
    fn forget(&self, ino: InodeNo, n: u64);
    fn remember(&self, ino: InodeNo);
}
