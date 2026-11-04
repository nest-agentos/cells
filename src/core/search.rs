//! Literal keyword matching shared by project files and parsed native history.
pub struct KeywordQuery {
    terms: Vec<(String, String)>,
}
impl KeywordQuery {
    pub fn new(query: &str) -> Self {
        let mut terms: Vec<(String, String)> = Vec::new();
        for term in query
            .split(|c: char| (c.is_whitespace() || c == '\u{feff}') && c != '\n' && c != '\r')
            .filter(|term| !term.is_empty())
        {
            let folded = term.to_lowercase();
            if !terms.iter().any(|(_, lower)| lower == &folded) {
                terms.push((term.into(), folded));
            }
        }
        Self { terms }
    }
    pub fn matches(&self, text: &str) -> bool {
        let text = text.to_lowercase();
        self.terms.iter().all(|(_, lower)| text.contains(lower))
    }
    pub fn anchor(&self) -> &str {
        self.terms
            .iter()
            .max_by_key(|(raw, _)| raw.len())
            .map_or("", |(raw, _)| raw.as_str())
    }
    pub fn multiple(&self) -> bool {
        self.terms.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keywords_are_literal_unordered_case_insensitive_and_required_together() {
        let query = KeywordQuery::new("  Beta\t中文　a.* BETA ");
        assert!(query.matches("中文 A.* other bEtA"));
        assert!(!query.matches("中文 beta axxx"));
        assert!(KeywordQuery::new("before\nneedle").matches("before\nneedle"));
        assert!(!KeywordQuery::new("before\nneedle").matches("before needle"));
        assert!(KeywordQuery::new(" ").matches("any"));
    }
}
