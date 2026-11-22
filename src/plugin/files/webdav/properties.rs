use super::{header, locking};
use crate::{
    app::State,
    core::error::Error,
    fs::{Export, Stat},
    http::access,
};
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;
const PROPERTIES: &[&str] = &[
    "displayname",
    "resourcetype",
    "getcontentlength",
    "getcontenttype",
    "getlastmodified",
    "creationdate",
    "getetag",
    "supportedlock",
    "lockdiscovery",
];
pub(super) fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
pub(super) fn href(alias: &str, path: &str, directory: bool) -> String {
    let path = path
        .split('/')
        .map(|part| {
            percent_encoding::utf8_percent_encode(part, percent_encoding::NON_ALPHANUMERIC)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("/");
    format!(
        "/dav/{alias}/{path}{}",
        if directory && !path.is_empty() {
            "/"
        } else {
            ""
        }
    )
}
struct Selection {
    names: Vec<(String, String)>,
    names_only: bool,
}
fn selection(body: &str) -> Result<Selection, Error> {
    if body.is_empty() {
        return Ok(Selection {
            names: PROPERTIES
                .iter()
                .map(|name| ("DAV:".into(), (*name).into()))
                .collect(),
            names_only: false,
        });
    }
    let document = roxmltree::Document::parse(body).map_err(|_| Error::invalid())?;
    let root = document.root_element();
    if !root.has_tag_name(("DAV:", "propfind")) {
        return Err(Error::invalid());
    }
    let children: Vec<_> = root.children().filter(|node| node.is_element()).collect();
    if children.len() != 1 {
        return Err(Error::invalid());
    }
    let node = children[0];
    if node.has_tag_name(("DAV:", "allprop")) || node.has_tag_name(("DAV:", "propname")) {
        return Ok(Selection {
            names: PROPERTIES
                .iter()
                .map(|name| ("DAV:".into(), (*name).into()))
                .collect(),
            names_only: node.tag_name().name() == "propname",
        });
    }
    if !node.has_tag_name(("DAV:", "prop")) {
        return Err(Error::invalid());
    }
    let names: Vec<_> = node
        .children()
        .filter(|node| node.is_element())
        .map(|node| {
            (
                node.tag_name().namespace().unwrap_or("").to_owned(),
                node.tag_name().name().to_owned(),
            )
        })
        .collect();
    if names.len() > 128 {
        return Err(Error::too_large("DAV_PROPERTY_LIMIT"));
    }
    Ok(Selection {
        names,
        names_only: false,
    })
}
pub(super) async fn find(
    state: Arc<State>,
    export: Arc<Export>,
    alias: String,
    path: String,
    headers: HeaderMap,
    body: Body,
    guards: Vec<OwnedRwLockReadGuard<()>>,
) -> Result<Response, Error> {
    let depth = match header(&headers, "depth")? {
        Some("0") => false,
        Some("1") => true,
        None | Some("infinity") => {
            return Err(Error::new(
                StatusCode::FORBIDDEN,
                "DAV_FINITE_DEPTH_REQUIRED",
            ))
        }
        _ => return Err(Error::invalid()),
    };
    let deadline = access::deadline(&headers)?;
    let selection = selection(&super::small_body(body, deadline).await?)?;
    let owner = access::identity(&state, &headers)?;
    let authorization = state.clone();
    let result = state
        .workers
        .read(&state.files, deadline, move |cancel| {
            let _guards = guards;
            let stat = export
                .stat(&path)?
                .ok_or_else(|| Error::not_found("ENOENT"))?;
            let directory = stat.kind == "directory";
            let mut result = String::from(
                "<?xml version=\"1.0\" encoding=\"utf-8\"?><D:multistatus xmlns:D=\"DAV:\">",
            );
            result.push_str(&entry(&export, &alias, &path, &stat, &selection, owner)?);
            if directory && depth {
                let listing = export.list(&path, &cancel)?;
                if listing.entries.len() > 10000 || !listing.warnings.is_empty() {
                    return Err(Error::too_large("DAV_LISTING_INCOMPLETE"));
                }
                for child in listing.entries {
                    if cancel.is_cancelled() {
                        return Err(Error::cancelled());
                    }
                    let child_path = if path.is_empty() {
                        child.name
                    } else {
                        format!("{path}/{}", child.name)
                    };
                    let _child_guard = match access::authorize_path(
                        &authorization,
                        &headers,
                        &alias,
                        &child_path,
                        false,
                        false,
                    ) {
                        Ok(guard) => guard,
                        Err(error) if error.status == StatusCode::FORBIDDEN => continue,
                        Err(error) => return Err(error),
                    };
                    result.push_str(&entry(
                        &export,
                        &alias,
                        &child_path,
                        &child.stat,
                        &selection,
                        owner,
                    )?);
                }
            }
            result.push_str("</D:multistatus>");
            Ok(result)
        })
        .await?;
    Ok(super::xml(StatusCode::MULTI_STATUS, result))
}
fn entry(
    export: &Export,
    alias: &str,
    path: &str,
    stat: &Stat,
    selection: &Selection,
    owner: usize,
) -> Result<String, Error> {
    if path.chars().any(|character| {
        (character < ' ' && !matches!(character, '\t' | '\n' | '\r'))
            || matches!(character, '\u{fffe}' | '\u{ffff}')
    }) {
        return Err(Error::conflict("DAV_NAME_UNREPRESENTABLE"));
    }
    let url = href(alias, path, stat.kind == "directory");
    let mut found = String::new();
    let mut missing = String::new();
    for (namespace, name) in &selection.names {
        let tag = format!("<P:{name} xmlns:P=\"{}\"", escape(namespace));
        let value = if namespace == "DAV:" {
            value(export, path, stat, name, owner, &url)?
        } else {
            None
        };
        match value {
            Some(value) => found.push_str(&if selection.names_only {
                format!("{tag}/>")
            } else {
                format!("{tag}>{value}</P:{name}>")
            }),
            None => missing.push_str(&format!("{tag}/>")),
        }
    }
    let mut result = format!("<D:response><D:href>{}</D:href>", escape(&url));
    for (properties, status) in [(found, "200 OK"), (missing, "404 Not Found")] {
        if !properties.is_empty() {
            result.push_str(&format!("<D:propstat><D:prop>{properties}</D:prop><D:status>HTTP/1.1 {status}</D:status></D:propstat>"));
        }
    }
    result.push_str("</D:response>");
    Ok(result)
}
fn value(
    export: &Export,
    path: &str,
    stat: &Stat,
    name: &str,
    owner: usize,
    _url: &str,
) -> Result<Option<String>, Error> {
    let directory = stat.kind == "directory";
    Ok(Some(match name {
        "displayname" => escape(path.rsplit('/').next().unwrap_or("")),
        "resourcetype" => {
            if directory {
                "<D:collection/>".into()
            } else {
                String::new()
            }
        }
        "getcontentlength" => stat.size.to_string(),
        "getcontenttype" => {
            if directory {
                "httpd/unix-directory".into()
            } else {
                "application/octet-stream".into()
            }
        }
        "getetag" => match &stat.revision {
            Some(value) => escape(value),
            None => return Ok(None),
        },
        "getlastmodified" => chrono::DateTime::from_timestamp_millis(stat.modified_at as i64)
            .ok_or_else(Error::internal)?
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string(),
        "creationdate" => chrono::DateTime::from_timestamp_millis(stat.created_at as i64)
            .ok_or_else(Error::internal)?
            .to_rfc3339(),
        "supportedlock" => {
            if export.webdav_locks {
                "<D:lockentry><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry>".into()
            } else {
                String::new()
            }
        }
        "lockdiscovery" if export.webdav_locks => export.lease_registry()?.read()?
            .discovery(&export.resource(path)?, owner)
            .iter()
            .map(|(token, seconds, url)| locking::active(token, *seconds, url))
            .collect(),
        "lockdiscovery" => String::new(),
        _ => return Ok(None),
    }))
}
