//! Keep virtio-fs directory identities stable across guest mount-device renumbering.
use sha2::{Digest, Sha256};
use std::{fs::File, io, os::unix::fs::MetadataExt};

pub fn device(directory: &File) -> io::Result<String> {
    let number = directory.metadata()?.dev();
    let filesystem = rustix::fs::fstatfs(directory)?;
    if filesystem.f_type as u64 != 0x65735546 {return Ok(number.to_string());}
    let device = format!("{}:{}",libc::major(number),libc::minor(number));
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(virtiofs_device(&mounts,&device).unwrap_or_else(||number.to_string()))
}

fn virtiofs_device(mounts: &str, device: &str) -> Option<String> {
    mounts.lines().find_map(|line| {
        let (mount, filesystem) = line.split_once(" - ")?;
        let fields:Vec<_> = mount.split_whitespace().collect();
        let filesystem:Vec<_> = filesystem.split_whitespace().collect();
        if fields.get(2).copied()!=Some(device) || filesystem.first().copied()!=Some("virtiofs") {return None;}
        let source = filesystem.get(1)?;
        let digest = Sha256::digest(source.as_bytes()).iter().map(|byte|format!("{byte:02x}")).collect::<String>();
        Some(format!("virtiofs@{digest}"))
    })
}

/// Old records lack a mount source. Only virtio-fs can accept device-only drift;
/// the configured export, inode and birth time must still identify the directory.
pub fn matches(expected: &str, current: &str) -> bool {
    if expected == current {return true;}
    let (Some((old_device,old_directory)),Some((new_device,new_directory))) =
        (expected.split_once(':'),current.split_once(':')) else {return false;};
    new_device.starts_with("virtiofs@") && !old_device.is_empty()
        && old_device.bytes().all(|byte|byte.is_ascii_digit()) && old_directory==new_directory
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn virtiofs_source_survives_mount_device_changes_but_other_filesystems_stay_strict() {
        let first=virtiofs_device("10 1 0:47 / /prj rw - virtiofs prj rw","0:47").unwrap();
        let next=virtiofs_device("20 1 0:48 / /different rw - virtiofs prj rw","0:48").unwrap();
        assert_eq!(first,next);
        assert_ne!(first,virtiofs_device("20 1 0:48 / /prj rw - virtiofs another rw","0:48").unwrap());
        assert!(virtiofs_device("20 1 0:48 / /prj rw - fuse.sshfs prj rw","0:48").is_none());
        assert!(matches("47:123:456",&format!("{next}:123:456")));
        assert!(!matches("47:123:456",&format!("{next}:124:456")));
        assert!(!matches("47:123:456",&format!("{next}:123:457")));
        assert!(!matches("47:123:456","48:123:456"));
        assert!(!matches("virtiofs@old:123:456",&format!("{next}:123:456")));
    }
}
