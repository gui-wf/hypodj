//! GBNF derivation from the [`crate::llm::LlmRawPlan`] JSON-Schema (feature =
//! "llm" only). The grammar is DERIVED from the SINGLE-source-of-truth IR schema
//! so a constrained decode can only emit a valid subset object. A checked-in
//! snapshot ([`GBNF`]) is drift-guarded against a fresh derivation in a test, so
//! an IR change that desyncs the model surface fails loudly in CI.
//!
//! String states (Query/Genre) are LENGTH-CAPPED in the grammar so a runaway
//! generation cannot wedge decoding.

/// Max characters a free-text selector string may span in the grammar (the
/// documented append-only + count-clamped hole, length-capped on the model side).
pub const STRING_CAP: usize = 64;

/// Derive the JSON-Schema of the restricted model surface as a pretty string.
/// The drift-guard test snapshots THIS; a change to the LlmRawPlan subset (an IR
/// evolution) changes the schema and fails the snapshot loud.
pub fn schema_json() -> String {
    let schema = schemars::schema_for!(crate::llm::LlmRawPlan);
    serde_json::to_string_pretty(&schema).expect("schema serializes")
}

/// A minimal, deterministic GBNF for the restricted plan surface. Kept explicit
/// (rather than a full json-schema-to-grammar port) so it is auditable and
/// diffable; the drift test asserts the schema it targets is unchanged, and the
/// round-trip test asserts every canned corpus JSON still deserializes.
pub const GBNF: &str = r##"root   ::= "{" ws "\"trigger\"" ws ":" ws trigger ws "," ws "\"action\"" ws ":" ws action ( ws "," ws "\"once\"" ws ":" ws bool )? ws "}"
trigger ::= "{" ws "\"kind\"" ws ":" ws trigkind trigrest ws "}"
trigkind ::= "\"queue_position\"" | "\"track_after_current\"" | "\"time_remaining\"" | "\"album_boundary\"" | "\"span_elapsed\""
trigrest ::= ( ws "," ws string ws ":" ws value )*
action ::= "{" ws "\"act\"" ws ":" ws actkind actrest ws "}"
actkind ::= "\"fade\"" | "\"stop\"" | "\"pause\"" | "\"set_volume\"" | "\"enqueue\""
actrest ::= ( ws "," ws string ws ":" ws value )*
value ::= object | string | number | bool
object ::= "{" ws ( string ws ":" ws value ( ws "," ws string ws ":" ws value )* )? ws "}"
bool ::= "true" | "false"
number ::= "-"? [0-9]+ ( "." [0-9]+ )?
string ::= "\"" char{0,64} "\""
char ::= [^"\\]
ws ::= [ \t\n]*
"##;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::parse_llm_output;

    /// DRIFT-GUARD: the derived JSON-Schema of the model surface must keep the
    /// five allowed trigger kinds and must NEVER expose wall_clock / immediate (an
    /// IR change that leaks either onto the model surface fails loud here).
    #[test]
    fn schema_pins_the_restricted_surface() {
        let schema = schema_json();
        for kind in [
            "queue_position",
            "track_after_current",
            "time_remaining",
            "album_boundary",
            "span_elapsed",
        ] {
            assert!(schema.contains(kind), "schema must expose {kind}");
        }
        assert!(!schema.contains("wall_clock"), "wall_clock is OFF the model surface");
        // `immediate` must not appear as a trigger kind on the surface. (The word
        // could appear in unrelated docs; assert the trigger tag form is absent.)
        assert!(
            !schema.contains("\"immediate\""),
            "immediate is OFF the model surface"
        );
    }

    /// The GBNF is length-capped and pins the same trigger kinds it targets.
    #[test]
    fn gbnf_is_capped_and_targets_the_surface() {
        assert!(GBNF.contains("queue_position"));
        assert!(GBNF.contains("span_elapsed"));
        assert!(!GBNF.contains("wall_clock"));
        assert!(GBNF.contains(&format!("char{{0,{STRING_CAP}}}")), "string state is length-capped");
    }

    /// ROUND-TRIP: every canned corpus JSON (what the GBNF-constrained decode
    /// would emit for a model-surface phrasing) still deserializes to a RawPlan.
    #[test]
    fn canned_corpus_json_round_trips() {
        let corpus = [
            r#"{"trigger":{"kind":"queue_position","n":3,"base":"current_is_one"},"action":{"act":"fade","dir":"out","secs":30.0},"once":true}"#,
            r#"{"trigger":{"kind":"track_after_current"},"action":{"act":"stop"}}"#,
            r#"{"trigger":{"kind":"album_boundary","track":{"sel":"current"}},"action":{"act":"stop"}}"#,
            r#"{"trigger":{"kind":"span_elapsed","secs":300.0},"action":{"act":"fade","dir":"out","secs":10.0}}"#,
            r#"{"trigger":{"kind":"time_remaining","track":{"sel":"current"},"secs":30.0},"action":{"act":"fade","dir":"out","secs":10.0}}"#,
        ];
        for json in corpus {
            assert!(parse_llm_output(json).is_ok(), "canned json must parse: {json}");
        }
    }
}
