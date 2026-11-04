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
