//! Durable native-context namespace, independent of process and event epochs.
use super::config::ProfileConfig;
use sha2::{Digest, Sha256};
use std::{fs::{File, OpenOptions}, io::{Read, Write}, os::unix::fs::OpenOptionsExt};

pub(super) fn load(config: &ProfileConfig) -> Result<String, String> {
    let directory = config.home.join(".cells/context-identities");
    std::fs::create_dir_all(&directory).map_err(|_| "Context identity directory unavailable")?;
    let key = format!("{:x}", Sha256::digest(serde_json::json!([config.id, config.kind]).to_string().as_bytes()));
    let path = directory.join(format!("{key}.id"));
    match OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path) {
        Ok(mut file) => {
            let id = crate::app::random_epoch()?;
            file.write_all(id.as_bytes()).and_then(|_| file.sync_all()).map_err(|_| "Context identity publication failed")?;
            File::open(&directory).and_then(|file| file.sync_all()).map_err(|_| "Context identity directory sync failed")?;
            Ok(id)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut id = String::new();
            File::open(path).and_then(|file| file.take(65).read_to_string(&mut id)).map_err(|_| "Context identity read failed")?;
            if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {return Err("Context identity invalid".into());}
            Ok(id)
        }
        Err(_) => Err("Context identity publication failed".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_the_profile_mapping_across_service_restarts() {
        let home=tempfile::tempdir().unwrap();
        let config:ProfileConfig=serde_json::from_value(serde_json::json!({"id":"test","kind":"codex","home":home.path()})).unwrap();
        let first=load(&config).unwrap();
        assert_eq!(load(&config).unwrap(),first);
        let a=crate::harness::Harnesses::new(vec![config.clone()],"process-one".into()).unwrap();
        let b=crate::harness::Harnesses::new(vec![config],"process-two".into()).unwrap();
        assert_eq!(a.profiles()["profiles"][0]["contextId"],b.profiles()["profiles"][0]["contextId"]);
        assert_ne!(a.profiles()["epoch"],b.profiles()["epoch"]);
    }
}
