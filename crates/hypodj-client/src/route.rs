//! Pure verb-vs-NL routing. A control-verb shortcut fires ONLY when the input is
//! EXACTLY the bare verb (a single token) or the verb plus its one expected
//! scalar. Everything else - a multi-word phrase, an unknown first word, a verb
//! with trailing words ("play something calmer", "next 3 songs") - goes to NL.
//! This is the silent-wrong-action trap: bare `play <n>` would start the queue
//! and drop intent, so "play something calmer" MUST route to NL.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Render the now-playing card (status + currentsong). Also the no-args case.
    NowPlaying,
    /// Render the queue list (playlistinfo).
    Queue,
    /// A single MPD command line to run, then auto-print the card.
    Command(String),
    /// `clear`, but gated behind a default-No y/N confirm (destructive).
    ClearConfirm,
    /// Print help (reuses the NotUnderstood supported-shapes hint).
    Help,
    /// Favorite (star) the CURRENTLY playing track. Not a static command line -
    /// the CLI must resolve the current song's uri at runtime and issue
    /// `playlistadd Starred <uri>`, so it carries no argument here.
    FavoriteCurrent,
    /// Send the whole phrase to the NL handshake.
    Nl(String),
}

/// Route the argument vector (already split into tokens) to an Action.
pub fn route(args: &[String]) -> Action {
    match args.len() {
        0 => Action::NowPlaying,
        1 => route_one(&args[0], args),
        2 => route_two(&args[0], &args[1], args),
        _ => Action::Nl(args.join(" ")),
    }
}

fn route_one(verb: &str, args: &[String]) -> Action {
    match verb {
        "now" | "status" => Action::NowPlaying,
        "queue" => Action::Queue,
        "play" => Action::Command("play".into()),
        "pause" => Action::Command("pause".into()),
        "stop" => Action::Command("stop".into()),
        "next" | "skip" => Action::Command("next".into()),
        "prev" | "previous" | "back" => Action::Command("previous".into()),
        // Favoriting is a first-class server feature (Subsonic Starred); expose the
        // natural bare verbs. Resolved against the current track by the CLI.
        "fav" | "favorite" | "favourite" | "star" => Action::FavoriteCurrent,
        "clear" => Action::ClearConfirm,
        "help" => Action::Help,
        // vol/volume alone (no scalar) is under-specified -> NL.
        _ => Action::Nl(args.join(" ")),
    }
}

/// The trailing noun in a natural transport/favorite phrase ("next SONG",
/// "favorite THIS") - a filler word that does not change the meaning. A DIFFERENT
/// trailing word (e.g. "next 3", "favorite jazz") is a real argument and belongs
/// in NL, so only these exact fillers collapse to the bare gesture.
fn is_filler_noun(w: &str) -> bool {
    matches!(w, "song" | "track" | "this" | "current" | "one" | "it")
}

fn route_two(verb: &str, arg: &str, args: &[String]) -> Action {
    match verb {
        "vol" | "volume" => match arg.parse::<u32>() {
            Ok(n) if n <= 100 => Action::Command(format!("setvol {n}")),
            // Out-of-range / non-numeric -> the whole phrase is natural language.
            _ => Action::Nl(args.join(" ")),
        },
        // Natural transport phrasing: "next song", "skip this", "previous track".
        // The tool bills itself NL-first, so the phrasing a human actually uses must
        // work - the bare token `next` is not what anyone says. A non-filler second
        // word ("next 3 songs") still falls through to NL.
        "next" | "skip" if is_filler_noun(arg) => Action::Command("next".into()),
        "prev" | "previous" | "back" if is_filler_noun(arg) => {
            Action::Command("previous".into())
        }
        // "favorite current", "fav this", "star song".
        "fav" | "favorite" | "favourite" | "star" if is_filler_noun(arg) => {
            Action::FavoriteCurrent
        }
        // Any other two-token phrase (including "play something") is NL.
        _ => Action::Nl(args.join(" ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(s: &str) -> Action {
        let args: Vec<String> = s.split_whitespace().map(str::to_string).collect();
        route(&args)
    }

    #[test]
    fn route_bare_verb_alone() {
        assert_eq!(r(""), Action::NowPlaying);
        assert_eq!(r("now"), Action::NowPlaying);
        assert_eq!(r("status"), Action::NowPlaying);
        assert_eq!(r("queue"), Action::Queue);
        assert_eq!(r("play"), Action::Command("play".into()));
        assert_eq!(r("pause"), Action::Command("pause".into()));
        assert_eq!(r("stop"), Action::Command("stop".into()));
        assert_eq!(r("next"), Action::Command("next".into()));
        assert_eq!(r("prev"), Action::Command("previous".into()));
        assert_eq!(r("previous"), Action::Command("previous".into()));
        assert_eq!(r("clear"), Action::ClearConfirm);
        assert_eq!(r("help"), Action::Help);
    }

    #[test]
    fn route_vol() {
        assert_eq!(r("vol 40"), Action::Command("setvol 40".into()));
        assert_eq!(r("volume 40"), Action::Command("setvol 40".into()));
        assert_eq!(r("vol 0"), Action::Command("setvol 0".into()));
        assert_eq!(r("vol 100"), Action::Command("setvol 100".into()));
        // Under-specified / out-of-range / non-numeric -> NL.
        assert_eq!(r("vol"), Action::Nl("vol".into()));
        assert_eq!(r("vol loud"), Action::Nl("vol loud".into()));
        assert_eq!(r("vol 200"), Action::Nl("vol 200".into()));
    }

    #[test]
    fn route_play_something_calmer_goes_to_nl() {
        // THE trap: must NOT become bare `play`.
        assert_eq!(r("play something calmer"), Action::Nl("play something calmer".into()));
        assert_eq!(r("play jazz"), Action::Nl("play jazz".into()));
    }

    #[test]
    fn route_natural_transport_phrasing() {
        // The human-native phrasings must resolve to the bare gesture, not NL.
        assert_eq!(r("skip"), Action::Command("next".into()));
        assert_eq!(r("back"), Action::Command("previous".into()));
        assert_eq!(r("next song"), Action::Command("next".into()));
        assert_eq!(r("next track"), Action::Command("next".into()));
        assert_eq!(r("skip this"), Action::Command("next".into()));
        assert_eq!(r("previous song"), Action::Command("previous".into()));
        assert_eq!(r("prev track"), Action::Command("previous".into()));
        // A real argument (a count) is NOT a filler noun -> stays NL.
        assert_eq!(r("next 3 songs"), Action::Nl("next 3 songs".into()));
        assert_eq!(r("skip 2"), Action::Nl("skip 2".into()));
    }

    #[test]
    fn route_favorite() {
        assert_eq!(r("fav"), Action::FavoriteCurrent);
        assert_eq!(r("favorite"), Action::FavoriteCurrent);
        assert_eq!(r("favourite"), Action::FavoriteCurrent);
        assert_eq!(r("star"), Action::FavoriteCurrent);
        assert_eq!(r("fav current"), Action::FavoriteCurrent);
        assert_eq!(r("favorite current"), Action::FavoriteCurrent);
        assert_eq!(r("favorite this"), Action::FavoriteCurrent);
        assert_eq!(r("star song"), Action::FavoriteCurrent);
        // A named target is not "the current track" -> NL (may resolve later).
        assert_eq!(r("favorite miles davis"), Action::Nl("favorite miles davis".into()));
    }

    #[test]
    fn route_multiword_and_unknown_first_word() {
        assert_eq!(r("stop after this album"), Action::Nl("stop after this album".into()));
        assert_eq!(r("wake me at 7 with jazz"), Action::Nl("wake me at 7 with jazz".into()));
        assert_eq!(r("fade the 3rd track"), Action::Nl("fade the 3rd track".into()));
        assert_eq!(r("next 3 songs"), Action::Nl("next 3 songs".into()));
        assert_eq!(r("queue jazz"), Action::Nl("queue jazz".into()));
        // Unknown lone first word.
        assert_eq!(r("shuffle"), Action::Nl("shuffle".into()));
    }
}
