//! Reject unsupported If grammar rather than treating it as an unconditional write.
use super::header;
use crate::{core::error::Error, fs::Export};
use axum::http::HeaderMap;
#[derive(Clone)]
pub(super) struct Conditions {
    pub tokens: Vec<String>,
    pub revision: Option<String>,
    pub create: bool,
    tags: Vec<(String, String)>,
}
impl Conditions {
    pub fn parse(headers: &HeaderMap) -> Result<Self, Error> {
        let mut tokens = Vec::new();
        let mut tags = Vec::new();
        if let Some(mut value) = header(headers, "if")? {
            while !value.trim().is_empty() {
                value = value.trim();
                if value.starts_with('<') {
                    if !tokens.is_empty() && tags.is_empty() {
                        return Err(unsupported());
                    }
                    let end = value.find('>').ok_or_else(unsupported)?;
                    let uri: axum::http::Uri = value[1..end].parse().map_err(|_| unsupported())?;
                    if uri.authority().is_some_and(|authority| {
                        Some(authority.as_str()) != header(headers, "host").ok().flatten()
                    }) {
                        return Err(unsupported());
                    }
                    tags.push(super::location(
                        uri.path_and_query().ok_or_else(unsupported)?.as_str(),
                    )?);
                    value = value[end + 1..].trim();
                } else if !tokens.is_empty() {
                    return Err(unsupported());
                }
                let end = value
                    .strip_prefix('(')
                    .and_then(|value| value.find(')').map(|end| end + 1))
                    .ok_or_else(unsupported)?;
                let token = value[1..end]
                    .trim()
                    .strip_prefix('<')
                    .and_then(|value| value.strip_suffix('>'))
                    .filter(|value| {
                        value.starts_with("opaquelocktoken:")
                            && !value.contains(['<', '>', ' ', '\t', '\n', '\r'])
                    })
                    .ok_or_else(unsupported)?;
                tokens.push(token.to_owned());
                if tokens.len() > 16 {
                    return Err(unsupported());
                }
                value = &value[end + 1..];
            }
            if tokens.is_empty() {
                return Err(unsupported());
            }
        }
        let revision = header(headers, "if-match")?.map(str::to_owned);
        if revision.as_ref().is_some_and(|value| {
            !value.starts_with('"') || !value.ends_with('"') || value.contains(',')
        }) {
            return Err(Error::invalid());
        }
        let create = match header(headers, "if-none-match")? {
            None => false,
            Some("*") => true,
            _ => return Err(Error::invalid()),
        };
        if create && revision.is_some() {
            return Err(Error::invalid());
        }
        Ok(Self {
            tokens,
            revision,
            create,
            tags,
        })
    }
    pub fn scopes(&self, paths: &[&str]) -> Vec<usize> {
        if self.tags.is_empty() {
            return vec![0; self.tokens.len()];
        }
        self.tags
            .iter()
            .map(|(_, path)| {
                paths
                    .iter()
                    .position(|candidate| *candidate == path)
                    .unwrap_or(usize::MAX)
            })
            .collect()
    }
    pub fn validate_tags(&self, alias: &str, paths: &[&str]) -> Result<(), Error> {
        if self
            .tags
            .iter()
            .any(|(tag_alias, path)| tag_alias != alias || !paths.contains(&path.as_str()))
        {
            return Err(unsupported());
        }
        Ok(())
    }
    pub fn check(&self, export: &Export, path: &str) -> Result<(), Error> {
        let stat = export.stat_inner(path)?;
        if self.create && stat.is_some() {
            return Err(Error::precondition_failed());
        }
        if let Some(expected) = &self.revision {
            if stat.as_ref().and_then(|stat| stat.revision.as_ref()) != Some(expected) {
                return Err(Error::precondition_failed());
            }
        }
        Ok(())
    }
}

fn unsupported() -> Error {
    Error::new(axum::http::StatusCode::BAD_REQUEST, "DAV_IF_UNSUPPORTED")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_scoped_positive_tokens_and_rejects_ambiguous_conditions() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "cells.test".parse().unwrap());
        headers.insert("if","<http://cells.test/dav/docs/a> (<opaquelocktoken:a>) <http://cells.test/dav/docs/b> (<opaquelocktoken:b>)".parse().unwrap());
        let conditions = Conditions::parse(&headers).unwrap();
        conditions.validate_tags("docs", &["a", "b"]).unwrap();
        assert_eq!(conditions.scopes(&["a", "b"]), vec![0, 1]);
        assert!(conditions.validate_tags("other", &["a", "b"]).is_err());
        for value in [
            "(Not <opaquelocktoken:a>)",
            "(<opaquelocktoken:a>) (<opaquelocktoken:b>)",
            "([\"version\"])",
            "<http://other.test/dav/docs/a> (<opaquelocktoken:a>)",
        ] {
            headers.insert("if", value.parse().unwrap());
            assert!(Conditions::parse(&headers).is_err());
        }
    }
}
