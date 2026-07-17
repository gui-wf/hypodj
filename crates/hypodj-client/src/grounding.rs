//! Client-side library-context gathering for the Claude Code DJ translate path.
//!
//! The `dj` CLI / `dj-gui` run `claude` LOCALLY, so the model must be told what
//! ACTUALLY exists in the library BEFORE it plans - otherwise it guesses a blind
//! query string ("nice calm music") from the words alone. These helpers read a
//! COMPACT real-candidate context off an already-open [`MpdConn`] (the daemon then
//! resolves the chosen label to real ids at execute time, exactly as today):
//!
//! - [`list_genres`] - the library's real genre names (`list genre`), process-cached.
//! - [`search_labels`] - top-N real artist/track LABELS matching the utterance
//!   keywords (`search any <kw>`).
//!
//! Both DEGRADE CLEANLY: any MPD error / empty result yields an empty Vec, so the
//! caller falls back to today's un-grounded prompt (never a hard failure). Model-free
//! by construction (no hypodj-nl dep): they return plain `Vec<String>` label lists;
//! the caller (which has the `cc` feature) folds them into a `LibraryContext`.

use std::sync::OnceLock;

use crate::mpd::MpdConn;

/// Process-lifetime cache of the genre list (genres rarely change; the long-lived
/// TUI re-asks per DJ query, so caching keeps the grounding cheap). Only a non-empty
/// successful fetch is cached, so a transient failure never poisons it.
static GENRE_CACHE: OnceLock<Vec<String>> = OnceLock::new();

/// Cap on the number of genre names folded into the prompt. An eclectic library
/// (Discogs-style multi-genre tagging) can expose hundreds of distinct genres via
/// `list genre`; without a bound the genre line grows with the library and is
/// re-sent on every DJ query, so prompt cost is not flat. The cap keeps the block
/// bounded (mirrors the candidate cap the caller passes to `search_labels`).
const GENRE_LIMIT: usize = 40;

/// Real genre names via `list genre`, capped at [`GENRE_LIMIT`] and cached for the
/// process. Returns an empty Vec on any MPD error (clean degrade - the caller then
/// omits the genre block).
pub fn list_genres(conn: &mut MpdConn) -> Vec<String> {
    if let Some(cached) = GENRE_CACHE.get() {
        return cached.clone();
    }
    let mut genres: Vec<String> = match conn.command("list genre") {
        Ok(pairs) => pairs
            .into_iter()
            .filter(|(k, _)| k == "Genre")
            .map(|(_, v)| v)
            .filter(|v| !v.trim().is_empty())
            .collect(),
        Err(_) => return Vec::new(),
    };
    // Bound the genre block so its size does not track library richness.
    genres.truncate(GENRE_LIMIT);
    if !genres.is_empty() {
        // A racing thread may win the set; either cached value is equally valid.
        let _ = GENRE_CACHE.set(genres.clone());
    }
    genres
}

/// Up to `limit` distinct real artist/track LABELS ("Artist - Title") matching the
/// utterance keywords, via `search any <kw>`. Command/filler words are stripped from
/// the utterance first so the full-text search keys on content, not verbs. Returns an
/// empty Vec when there are no content keywords or on any MPD error (clean degrade).
pub fn search_labels(conn: &mut MpdConn, utterance: &str, limit: usize) -> Vec<String> {
    let keywords = content_keywords(utterance);
    let words: Vec<&str> = keywords.split_whitespace().collect();
    if words.is_empty() {
        return Vec::new();
    }
    // Search PER keyword and MERGE the resulting labels (OR across keywords), rather
    // than gluing them into one contiguous `search any "kw1 kw2"` substring. A single
    // substring requires the literal phrase "kw1 kw2" inside one tag, which almost
    // never exists, so a 2+ word ask ("upbeat funk") would silently return nothing.
    // Per-keyword search recovers real candidates (funk tracks match "funk" even when
    // no tag holds the phrase); dedup + the shared cap keep the block bounded.
    let mut labels: Vec<String> = Vec::new();
    for kw in words {
        if labels.len() >= limit {
            break;
        }
        let line = format!("search any \"{}\"", mpd_escape(kw));
        // A per-keyword MPD error degrades cleanly to skipping that keyword, not
        // aborting the whole seed search - other keywords can still contribute.
        let pairs = match conn.command(&line) {
            Ok(p) => p,
            Err(_) => continue,
        };
        for label in labels_from_song_pairs(&pairs, limit) {
            if labels.len() >= limit {
                break;
            }
            if !labels.iter().any(|l| l == &label) {
                labels.push(label);
            }
        }
    }
    labels
}

/// Fold MPD song-row pairs into up to `limit` distinct "Artist - Title" labels. A new
/// song starts at each `file` key (the song-row lead key). Pure + unit-tested.
pub fn labels_from_song_pairs(pairs: &[(String, String)], limit: usize) -> Vec<String> {
    let mut labels: Vec<String> = Vec::new();
    let mut artist: Option<String> = None;
    let mut title: Option<String> = None;
    // Push the accumulated song (if any) as a deduped label.
    let flush = |labels: &mut Vec<String>, artist: &Option<String>, title: &Option<String>| {
        let label = match (artist, title) {
            (Some(a), Some(t)) => format!("{a} - {t}"),
            (None, Some(t)) => t.clone(),
            (Some(a), None) => a.clone(),
            (None, None) => return,
        };
        let label = label.trim().to_string();
        if !label.is_empty() && !labels.iter().any(|l| l == &label) {
            labels.push(label);
        }
    };
    for (k, v) in pairs {
        match k.as_str() {
            "file" => {
                flush(&mut labels, &artist, &title);
                artist = None;
                title = None;
            }
            "Title" => title = Some(v.clone()),
            "Artist" if artist.is_none() => artist = Some(v.clone()),
            _ => {}
        }
        if labels.len() >= limit {
            return labels;
        }
    }
    flush(&mut labels, &artist, &title);
    labels.truncate(limit);
    labels
}

/// Common command/filler words that carry no library-search signal. Stripping them
/// keeps the full-text `search any` keyed on content (genre/mood/artist words).
const STOPWORDS: &[&str] = &[
    "play", "queue", "add", "put", "on", "some", "a", "an", "the", "few", "couple",
    "bunch", "of", "track", "tracks", "song", "songs", "music", "please", "at", "end",
    "next", "now", "after", "current", "me", "something", "stuff", "and", "to", "for",
    "up", "more", "bit", "little", "playing", "start", "with",
];

/// Lowercased content keywords from the utterance: split on non-alphanumeric, drop
/// stopwords and 1-char tokens. Pure + unit-tested.
pub fn content_keywords(utterance: &str) -> String {
    let words: Vec<String> = utterance
        .split(|c: char| !c.is_alphanumeric())
        .map(|w| w.to_lowercase())
        .filter(|w| w.len() > 1 && !STOPWORDS.contains(&w.as_str()))
        .collect();
    words.join(" ")
}

/// Escape a value for an MPD quoted argument: backslash-escape `"` and `\` (the
/// daemon tokenizer unescapes `\<c>` inside quotes).
fn mpd_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn keywords_strip_command_and_filler_words() {
        assert_eq!(content_keywords("play some calm music"), "calm");
        assert_eq!(content_keywords("queue a few bossa nova tracks"), "bossa nova");
        assert_eq!(content_keywords("add three upbeat songs after the current track"), "three upbeat");
        // An all-stopword ask yields nothing -> the caller skips the seed search.
        assert_eq!(content_keywords("play some music"), "");
    }

    #[test]
    fn labels_group_per_song_dedup_and_cap() {
        let p = pairs(&[
            ("file", "song/1"),
            ("Title", "Blue in Green"),
            ("Artist", "Bill Evans"),
            ("Album", "Kind of Blue"),
            ("file", "song/2"),
            ("Title", "So What"),
            ("Artist", "Miles Davis"),
            // A duplicate label must not repeat.
            ("file", "song/3"),
            ("Title", "Blue in Green"),
            ("Artist", "Bill Evans"),
        ]);
        let got = labels_from_song_pairs(&p, 10);
        assert_eq!(got, vec!["Bill Evans - Blue in Green", "Miles Davis - So What"]);
        // The cap bounds the label count (prompt cost stays flat).
        assert_eq!(labels_from_song_pairs(&p, 1).len(), 1);
    }

    #[test]
    fn labels_tolerate_title_only_rows() {
        let p = pairs(&[("file", "song/9"), ("Title", "Untitled")]);
        assert_eq!(labels_from_song_pairs(&p, 5), vec!["Untitled"]);
        // No song rows -> no labels.
        assert!(labels_from_song_pairs(&[], 5).is_empty());
    }

    #[test]
    fn multi_keyword_utterance_yields_separate_search_words() {
        // The seed search keys on EACH content word, not one glued phrase, so a
        // 2+ word ask can match tracks tagged with either word.
        let keywords = content_keywords("queue some upbeat funk tracks");
        let words: Vec<&str> = keywords.split_whitespace().collect();
        assert_eq!(words, vec!["upbeat", "funk"]);
    }

    #[test]
    fn genre_cap_is_bounded() {
        // The genre block must not grow with library richness.
        assert!(GENRE_LIMIT >= 1);
        assert!(GENRE_LIMIT <= 100);
    }

    #[test]
    fn escape_quotes_and_backslashes() {
        assert_eq!(mpd_escape(r#"a"b\c"#), r#"a\"b\\c"#);
    }
}
