//! Readable IDs are unique across the node project catalog.
use crate::core::error::Error;
pub fn allocate<'a>(name: &str, ids: impl Iterator<Item = &'a str>, requested: Option<&str>) -> Result<String, Error> {
    let used: std::collections::HashSet<_> = ids.map(str::to_ascii_lowercase).collect();
    if let Some(id) = requested {
        if id.is_empty() || id.len() > 64 || !id.as_bytes()[0].is_ascii_alphanumeric()
            || !id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_') {
            return Err(Error::new(axum::http::StatusCode::BAD_REQUEST, "PROJECT_ID_INVALID"));
        }
        if uuid_shape(id) { return Err(Error::new(axum::http::StatusCode::BAD_REQUEST, "PROJECT_ID_UUID")); }
        if used.contains(&id.to_ascii_lowercase()) { return Err(Error::conflict("PROJECT_ID_EXISTS")); }
        return Ok(id.into());
    }
    let mut base = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() { base.push(c.to_ascii_lowercase()); }
        else if !base.is_empty() && !base.ends_with('-') { base.push('-'); }
        if base.len() >= 48 { break; }
    }
    let base = base.trim_end_matches('-');
    let base = if base.is_empty() || uuid_shape(base) { "project" } else { base };
    let mut id = base.to_owned();
    let mut suffix = 2;
    while used.contains(&id) { id = format!("{base}-{suffix}"); suffix += 1; }
    Ok(id)
}

fn uuid_shape(id: &str) -> bool {
    id.len() == 36 && [8,13,18,23].iter().all(|i| id.as_bytes()[*i] == b'-')
        && id.bytes().enumerate().all(|(i,c)| [8,13,18,23].contains(&i) || c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readable_ids_are_unique_case_insensitively_and_never_accept_uuid_ids() {
        assert_eq!(allocate("Notes", ["NOTES","notes-2"].into_iter(), None).unwrap(), "notes-3");
        assert_eq!(allocate("中文", [].into_iter(), None).unwrap(), "project");
        assert_eq!(allocate("Name", [].into_iter(), Some("my_notes")).unwrap(), "my_notes");
        for id in ["../notes", "-notes", "a2fdd384-d811-4be5-9922-30b9176c2798"] {
            assert!(allocate("Name", [].into_iter(), Some(id)).is_err());
        }
        assert_eq!(allocate("Name", ["NOTES"].into_iter(), Some("notes")).unwrap_err().code,"PROJECT_ID_EXISTS");
    }
}
