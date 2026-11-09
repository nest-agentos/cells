//! Preserve explicit proxy and certificate settings without inheriting arbitrary host variables.
const NETWORK: &[&str] = &[
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "https_proxy",
    "http_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];
pub(super) fn inherit(command: &mut tokio::process::Command) {
    for (key, value) in selected(|key| std::env::var_os(key)) {
        command.env(key, value);
    }
}
pub(super) fn inherit_prefixed(command:&mut tokio::process::Command,prefixes:&[&str]) {
    copy_prefixed(command,prefixes,std::env::vars_os());
}
fn copy_prefixed(command:&mut tokio::process::Command,prefixes:&[&str],values:impl IntoIterator<Item=(std::ffi::OsString,std::ffi::OsString)>) {
    for (key,value) in values {
        if key.to_str().is_some_and(|name|prefixes.iter().any(|prefix|name.starts_with(prefix))) {command.env(key,value);}
    }
}

fn selected(
    mut lookup: impl FnMut(&str) -> Option<std::ffi::OsString>,
) -> Vec<(&'static str, std::ffi::OsString)> {
    NETWORK
        .iter()
        .filter_map(|key| lookup(key).map(|value| (*key, value)))
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_case_specific_proxy_settings_and_excludes_unrelated_secrets() {
        let values = selected(|key| match key {
            "http_proxy" => Some("http://lower:7890".into()),
            "HTTP_PROXY" => Some("http://upper:7891".into()),
            "no_proxy" => Some("localhost".into()),
            "SSL_CERT_FILE" => Some("/certs/custom.pem".into()),
            "UNRELATED_SECRET" => panic!("must not inspect unrelated variables"),
            _ => None,
        });
        assert_eq!(values.len(), 4);
        assert!(values.contains(&("http_proxy", "http://lower:7890".into())));
        assert!(values.contains(&("HTTP_PROXY", "http://upper:7891".into())));
        assert!(values.contains(&("no_proxy", "localhost".into())));
    }
}

#[cfg(test)]
mod namespace_tests {
    use super::*;
    #[test]
    fn forwards_vendor_options_without_copying_unrelated_secrets() {
        let mut command=tokio::process::Command::new("/bin/true");command.env_clear();
        let values=[("ANTHROPIC_MODEL","fixture"),("CLAUDE_CODE_EFFORT_LEVEL","max"),("UNRELATED_SECRET","private"),("DEEPSEEK_API_KEY","fixture-key")].map(|(key,value)|(key.into(),value.into()));
        copy_prefixed(&mut command,&["ANTHROPIC_","CLAUDE_CODE_"],values);
        let names=command.as_std().get_envs().map(|(name,_)|name.to_string_lossy().into_owned()).collect::<Vec<_>>();
        assert_eq!(names,vec!["ANTHROPIC_MODEL","CLAUDE_CODE_EFFORT_LEVEL"]);
    }
}
