use super::*;
impl Cursor {
    pub(super) fn claude(&mut self, record: &Value) -> Result<(), Error> {
        if record["type"] == "user"
            && record["isMeta"] != true
            && record["isCompactSummary"] != true
        {
            self.turn = text(&record["uuid"]);
        }
        if record["type"] != "assistant" {
            return Ok(());
        }
        let message = &record["message"];
        let usage = &message["usage"];
        if !usage.is_object() {
            return Ok(());
        }
        if !valid_counts(
            usage,
            &["cache_read_input_tokens", "cache_creation_input_tokens"],
        ) {
            self.incomplete = true;
            return Ok(());
        }
        let Some(id) = text(&message["id"]) else {
            self.incomplete = true;
            return Ok(());
        };
        self.provider = Some("anthropic".into());
        self.model = text(&message["model"]);
        self.put(id, tokens(usage), true)
    }
}
fn tokens(usage: &Value) -> Tokens {
    Tokens {
        input_tokens: count(usage, "input_tokens"),
        output_tokens: count(usage, "output_tokens"),
        cache_read_tokens: count(usage, "cache_read_input_tokens"),
        cache_write_tokens: count(usage, "cache_creation_input_tokens"),
        reasoning_tokens: 0,
    }
}
