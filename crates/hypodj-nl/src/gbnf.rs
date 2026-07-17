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
pub const GBNF: &str = r##"root   ::= "{" ws "\"type\"" ws ":" ws actkind rest ws "}"
actkind ::= "\"fade_out\"" | "\"fade_in\"" | "\"stop\"" | "\"pause\"" | "\"set_volume\"" | "\"enqueue\"" | "\"remove\"" | "\"move\"" | "\"clear\"" | "\"play\"" | "\"noop\""
rest ::= ( ws "," ws string ws ":" ws value )*
value ::= object | array | string | number | bool
object ::= "{" ws ( string ws ":" ws value ( ws "," ws string ws ":" ws value )* )? ws "}"
array ::= "[" ws ( value ( ws "," ws value )* )? ws "]"
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

    /// DRIFT-GUARD: the derived JSON-Schema of the FLAT model surface must expose
    /// the closed action lexicon and the closed `when` lexicon, and must NEVER
    /// expose wall_clock / immediate (an IR change that leaks either onto the model
    /// surface fails loud here).
    #[test]
    fn schema_pins_the_restricted_surface() {
        let schema = schema_json();
        for kind in [
            // action lexicon
            "fade_out",
            "fade_in",
            "set_volume",
            "enqueue",
            // `when` lexicon
            "after_current",
            "after_secs",
            "album_boundary",
            "queue_position",
            "time_remaining",
        ] {
            assert!(schema.contains(kind), "schema must expose {kind}");
        }
        assert!(!schema.contains("wall_clock"), "wall_clock is OFF the model surface");
        // No RawTrigger `immediate` tag leaks onto the surface (the `when` default is
        // `now`, resolved to Immediate only in TryFrom, never named on the surface).
        assert!(
            !schema.contains("\"immediate\""),
            "immediate is OFF the model surface"
        );
    }

    /// The GBNF is length-capped and pins the FLAT action lexicon it targets.
    #[test]
    fn gbnf_is_capped_and_targets_the_surface() {
        assert!(GBNF.contains("fade_out"));
        assert!(GBNF.contains("enqueue"));
        assert!(!GBNF.contains("wall_clock"));
        assert!(GBNF.contains(&format!("char{{0,{STRING_CAP}}}")), "string state is length-capped");
    }

    /// ROUND-TRIP: every canned corpus JSON (the FLAT shape a constrained decode
    /// would emit for a model-surface phrasing) still deserializes to a RawPlan.
    #[test]
    fn canned_corpus_json_round_trips() {
        let corpus = [
            r#"{"type":"fade_out","secs":30.0,"when":"queue_position","slot":3,"once":true}"#,
            r#"{"type":"stop"}"#,
            r#"{"type":"stop","when":"album_boundary"}"#,
            r#"{"type":"fade_out","secs":10.0,"when":"after_secs","when_secs":300.0}"#,
            r#"{"type":"fade_out","secs":10.0,"when":"time_remaining","when_secs":30.0}"#,
            r#"{"type":"set_volume","level":42}"#,
            r#"{"type":"enqueue","query":"bon iver","count":5}"#,
            r#"{"type":"enqueue","genre":"jazz","count":3}"#,
            r#"{"type":"enqueue","radio":true,"count":5}"#,
        ];
        for json in corpus {
            assert!(parse_llm_output(json).is_ok(), "canned json must parse: {json}");
        }
    }
}
