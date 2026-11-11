//! Request correlation only; identifiers never grant authority.
use serde_json::{Map, Value};
use std::future::Future;

tokio::task_local! {static CONTEXT: Map<String, Value>;}
pub fn current() -> Map<String, Value> {
    CONTEXT.try_with(Clone::clone).unwrap_or_default()
}
pub async fn scope<T>(context: Map<String, Value>, work: impl Future<Output = T>) -> T {
    CONTEXT.scope(context, work).await
}
/// Spawn a causal child instead of losing task-local correlation at an async boundary.
pub fn spawn<F>(work:F)->tokio::task::JoinHandle<F::Output> where F:Future+Send+'static,F::Output:Send+'static {
    tokio::spawn(scope(resume(&Value::Object(current())),work))
}
/// Persist only validated correlation identifiers, never arbitrary request metadata.
pub fn capture()->Value {Value::Object(validated(&Value::Object(current())))}
pub fn resume(value:&Value)->Map<String,Value> {
    let mut context=validated(value);
    if !context.contains_key("traceId"){return fields(None,None,None);}
    if let Some(parent)=context.get("spanId").cloned(){context.insert("parentSpanId".into(),parent);}
    if let Some(span)=random_hex(8){context.insert("spanId".into(),span.into());}
    context
}
fn validated(value:&Value)->Map<String,Value> {
    let mut result=Map::new();
    for key in ["traceId","spanId","parentSpanId","requestId","parentRequestId","browserSessionId"] {
        let Some(text)=value[key].as_str() else{continue};
        let valid=match key {"traceId"=>hex(text,32),"spanId"|"parentSpanId"=>hex(text,16),
            _=>crate::core::ids::is_identifier_within(text,128)};
        if valid{result.insert(key.into(),text.into());}
    }
    result
}
pub fn sync_scope<T>(context: Map<String, Value>, work: impl FnOnce() -> T) -> T {
    CONTEXT.sync_scope(context, work)
}
pub fn fields(
    parent: Option<&str>,
    browser: Option<&str>,
    request: Option<&str>,
) -> Map<String, Value> {
    let parts: Vec<_> = parent.unwrap_or_default().split('-').collect();
    let valid = parts.len() == 4
        && parts[0] == "00"
        && hex(parts[1], 32)
        && hex(parts[2], 16)
        && matches!(parts[3], "00" | "01");
    let mut result = Map::new();
    let trace = if valid {
        Some(parts[1].to_owned())
    } else {
        random_hex(16)
    };
    if let Some(trace) = trace {
        result.insert("traceId".into(), trace.into());
    }
    if let Some(span) = random_hex(8) {
        result.insert("spanId".into(), span.into());
    }
    if let Some(id) = random_hex(16) {
        result.insert("requestId".into(), id.into());
    }
    if valid {
        result.insert("parentSpanId".into(), parts[2].into());
    }
    if let Some(value) =
        browser.filter(|v| v.len() == 36 && v.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'))
    {
        result.insert("browserSessionId".into(), value.into());
    }
    if let Some(value) = request.filter(|v| crate::core::ids::is_identifier_within(v, 128)) {
        result.insert("parentRequestId".into(), value.into());
    }
    result
}
fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value.bytes().any(|b| b != b'0')
}
fn random_hex(bytes: usize) -> Option<String> {
    let mut value = vec![0; bytes];
    getrandom::getrandom(&mut value).ok()?;
    Some(value.iter().map(|byte| format!("{byte:02x}")).collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn scopes_are_isolated_and_cross_blocking_workers() {
        let a = fields(
            Some("00-11111111111111111111111111111111-2222222222222222-01"),
            None,
            None,
        );
        let b = fields(
            Some("00-33333333333333333333333333333333-4444444444444444-01"),
            None,
            None,
        );
        let check = |context| {
            scope(context, async {
                tokio::task::yield_now().await;
                let captured = current();
                tokio::task::spawn_blocking(move || sync_scope(captured, current))
                    .await
                    .unwrap()
            })
        };
        let (a, b) = tokio::join!(check(a), check(b));
        assert_eq!(a["traceId"], "11111111111111111111111111111111");
        assert_eq!(b["traceId"], "33333333333333333333333333333333");
        assert!(current().is_empty());
    }
    #[test]
    fn invalid_metadata_is_not_reflected() {
        for parent in [
            "bad",
            "00-00000000000000000000000000000000-2222222222222222-01",
            "00-11111111111111111111111111111111-0000000000000000-01",
        ] {
            let value = fields(Some(parent), Some("secret\nvalue"), Some("secret\nvalue"));
            assert!(!value.contains_key("parentSpanId"));
            assert!(!value.contains_key("browserSessionId"));
            assert!(!value.contains_key("parentRequestId"));
            assert!(value["traceId"]
                .as_str()
                .map(|v| hex(v, 32))
                .unwrap_or(false));
        }
    }
}

#[cfg(test)]
mod causal_tests {
    use super::*;
    #[tokio::test]
    async fn spawned_and_recovered_work_retain_the_origin_trace_without_copying_payloads() {
        let mut origin=fields(None,None,Some("request-1"));origin.insert("secret".into(),"PRIVATE".into());
        let expected=origin["traceId"].clone();let parent=origin["spanId"].clone();
        let (child,persisted)=scope(origin,async{let persisted=capture();(spawn(async{current()}).await.unwrap(),persisted)}).await;
        assert_eq!(child["traceId"],expected);assert_eq!(child["parentSpanId"],parent);
        assert_ne!(child["spanId"],parent);assert!(!persisted.to_string().contains("PRIVATE"));
        assert_eq!(resume(&persisted)["traceId"],expected);assert!(current().is_empty());
        assert!(!resume(&serde_json::json!({"traceId":"invalid","secret":"PRIVATE"})).contains_key("secret"));
    }
}
