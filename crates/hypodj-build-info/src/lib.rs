//! Version-display enrichment: a DISPLAY-ONLY channel that augments the plain
//! semver (`CARGO_PKG_VERSION` / the git tag) with commits-since-tag and the
//! git short hash for source builds.
//!
//! Two halves live here:
//!
//! - [`emit`] is a build-script helper. A binary crate's own `build.rs` calls
//!   it; it runs `git describe`, parses the result, and emits a
//!   `cargo:rustc-env=HYPODJ_BUILD_INFO=<grammar>` for THAT crate (a
//!   `cargo:rustc-env` from one build.rs only reaches the crate whose build.rs
//!   ran it). It NEVER fails the build - any git problem yields an empty value.
//! - [`compose`] is a normal (non-build) function the binaries call at print
//!   time to render the enriched string from the base semver plus the raw
//!   build-info grammar.
//!
//! Grammar of the `HYPODJ_BUILD_INFO` string (fixed, space-separated fields):
//!   `count=<N> hash=<short> dirty=<0|1>`
//! Any field may be absent. An empty string means "no build info" (git
//! unavailable), which composes to the bare base semver.

/// Build-script entry point. Call from a binary crate's `build.rs`:
///
/// ```ignore
/// fn main() {
///     hypodj_build_info::emit();
/// }
/// ```
///
/// Emits `cargo:rustc-env=HYPODJ_BUILD_INFO=<grammar>` for the calling crate,
/// plus the `rerun-if-changed` lines that make the value track HEAD, the
/// resolved branch ref, and packed-refs. On any git failure it emits an EMPTY
/// value and returns normally - it never panics and never aborts the build.
pub fn emit() {
    // Rerun triggers. HEAD moves on commit/checkout; the resolved branch ref
    // file moves when the branch advances; packed-refs changes when tags or
    // refs get packed (which does NOT touch .git/HEAD). Missing files are fine
    // - cargo ignores rerun-if-changed paths that do not exist.
    emit_rerun_triggers();

    let value = describe_build_info().unwrap_or_default();
    println!("cargo:rustc-env=HYPODJ_BUILD_INFO={value}");
}

/// Emit the `cargo:rerun-if-changed` lines. Resolves `.git/HEAD`'s
/// `ref: refs/heads/xxx` to the concrete ref file so a plain commit re-triggers
/// the build script; always includes packed-refs for the packed-tag case.
fn emit_rerun_triggers() {
    let git_dir = std::path::Path::new(".git");
    let head = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head.display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display()
    );

    // Resolve the symbolic ref so branch advances re-trigger.
    if let Ok(contents) = std::fs::read_to_string(&head) {
        let line = contents.trim();
        if let Some(rest) = line.strip_prefix("ref:") {
            let ref_path = rest.trim();
            println!(
                "cargo:rerun-if-changed={}",
                git_dir.join(ref_path).display()
            );
        }
    }
}

/// Run `git describe --tags --long --dirty --match 'v[0-9]*'` and re-render its
/// output into the fixed grammar. Returns `None` on any failure (no git, no
/// .git, shallow clone with no tags, non-zero exit), which the caller maps to an
/// empty emitted value.
fn describe_build_info() -> Option<String> {
    let output = std::process::Command::new("git")
        .args([
            "describe",
            "--tags",
            "--long",
            "--dirty",
            "--match",
            "v[0-9]*",
        ])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let described = String::from_utf8(output.stdout).ok()?;
    parse_describe(described.trim())
}

/// Parse `git describe --long --dirty` output into the emit grammar.
///
/// Example inputs:
///   `v0.1.0-7-ga86b53b`         -> `count=7 hash=a86b53b dirty=0`
///   `v0.1.0-7-ga86b53b-dirty`   -> `count=7 hash=a86b53b dirty=1`
///   `v0.1.0-0-ga86b53b`         -> `count=0 hash=a86b53b dirty=0`
///
/// The `--long` form always ends with `-<count>-g<hash>` (optionally
/// `-dirty`). We parse from the RIGHT so a tag containing hyphens (e.g.
/// `v1.0.0-alpha.5`) does not confuse the split. `g` is git's object-name
/// prefix; the emitted hash keeps it OFF (compose re-adds the display `g`).
fn parse_describe(described: &str) -> Option<String> {
    let mut rest = described;
    let dirty = if let Some(stripped) = rest.strip_suffix("-dirty") {
        rest = stripped;
        true
    } else {
        false
    };

    // Trailing `-g<hash>`.
    let g_pos = rest.rfind("-g")?;
    let hash = &rest[g_pos + 2..];
    if hash.is_empty() || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    rest = &rest[..g_pos];

    // Now trailing `-<count>`.
    let count_pos = rest.rfind('-')?;
    let count = &rest[count_pos + 1..];
    if count.is_empty() || !count.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    Some(format!(
        "count={count} hash={hash} dirty={}",
        if dirty { 1 } else { 0 }
    ))
}

/// Parsed fields of the raw build-info grammar.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    pub count: Option<u64>,
    pub hash: Option<String>,
    pub dirty: bool,
}

impl BuildInfo {
    /// Parse the `count=.. hash=.. dirty=..` grammar. An empty/whitespace-only
    /// input yields an all-absent `BuildInfo`.
    pub fn parse(raw: &str) -> BuildInfo {
        let mut info = BuildInfo::default();
        for field in raw.split_whitespace() {
            let Some((key, val)) = field.split_once('=') else {
                continue;
            };
            match key {
                "count" => info.count = val.parse().ok(),
                "hash" => {
                    if !val.is_empty() {
                        info.hash = Some(val.to_string());
                    }
                }
                "dirty" => info.dirty = val == "1",
                _ => {}
            }
        }
        info
    }
}

/// Compose the DISPLAY version string from a base semver plus the optional raw
/// build-info grammar. This is the single source of the format rules:
///
/// - `count>0`                        -> `<base> +N (g<hash>[-dirty])`
/// - `count==0 && !dirty` (on the tag) -> `<base>` (bare - no `+0`, no hash)
/// - count absent but hash present    -> `<base> (g<hash>[-dirty])`
/// - nothing usable                   -> `<base>` (bare base)
///
/// A `base` that is not real semver (e.g. `dev`, empty) always renders bare -
/// enrichment only makes sense on a released version line.
pub fn compose(base: &str, raw_build_info: Option<&str>) -> String {
    let base = base.trim();
    let raw = raw_build_info.unwrap_or("").trim();

    // Guard: a placeholder / non-semver base never gets decorated.
    if base.is_empty() || base == "dev" || !looks_like_semver(base) {
        return base.to_string();
    }
    if raw.is_empty() {
        return base.to_string();
    }

    let info = BuildInfo::parse(raw);
    let dirty_suffix = if info.dirty { "-dirty" } else { "" };

    match (info.count, info.hash.as_deref()) {
        // On the tag exactly, clean: bare base (no `+0`, no hash).
        (Some(0), _) if !info.dirty => base.to_string(),
        // On the tag but dirty: still worth flagging the uncommitted state.
        (Some(0), Some(hash)) => format!("{base} (g{hash}{dirty_suffix})"),
        (Some(0), None) => format!("{base} +0{dirty_suffix}"),
        // N commits ahead with a hash.
        (Some(n), Some(hash)) => format!("{base} +{n} (g{hash}{dirty_suffix})"),
        // Count known but hash somehow absent - show the count.
        (Some(n), None) => format!("{base} +{n}{dirty_suffix}"),
        // Count unknown but hash present - hash only, no invented count.
        (None, Some(hash)) => format!("{base} (g{hash}{dirty_suffix})"),
        // Nothing usable.
        (None, None) => base.to_string(),
    }
}

/// Resolve the raw build-info grammar with the right precedence, then render.
/// `compile_time` is the value baked by THIS binary's build.rs, passed in via
/// `option_env!("HYPODJ_BUILD_INFO")` at the call site (a `cargo:rustc-env` only
/// reaches the crate whose build.rs emitted it, so it cannot be read from here).
///
/// Precedence:
///  1. compile-time value (source builds, when non-empty);
///  2. a runtime `HYPODJ_BUILD_INFO` env var - what a nix-built binary sees, its
///     wrapper injecting the value since the sandbox has no .git;
///  3. None (bare base semver).
pub fn resolve(compile_time: Option<&str>) -> Option<String> {
    if let Some(ct) = compile_time {
        if !ct.trim().is_empty() {
            return Some(ct.to_string());
        }
    }
    std::env::var("HYPODJ_BUILD_INFO")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

/// The enriched DISPLAY version for a binary: [`compose`] over the resolved raw
/// build info. Call as
/// `version(env!("CARGO_PKG_VERSION"), option_env!("HYPODJ_BUILD_INFO"))`.
pub fn version(base: &str, compile_time: Option<&str>) -> String {
    compose(base, resolve(compile_time).as_deref())
}

/// Cheap semver-ish sniff: `major.minor.patch` leading numeric triple. Avoids a
/// semver dependency in this leaf crate.
fn looks_like_semver(s: &str) -> bool {
    let core = s.split(['-', '+']).next().unwrap_or(s);
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_describe_clean_ahead() {
        assert_eq!(
            parse_describe("v0.1.0-7-ga86b53b").as_deref(),
            Some("count=7 hash=a86b53b dirty=0")
        );
    }

    #[test]
    fn parse_describe_dirty() {
        assert_eq!(
            parse_describe("v0.1.0-7-ga86b53b-dirty").as_deref(),
            Some("count=7 hash=a86b53b dirty=1")
        );
    }

    #[test]
    fn parse_describe_on_tag() {
        assert_eq!(
            parse_describe("v0.1.0-0-ga86b53b").as_deref(),
            Some("count=0 hash=a86b53b dirty=0")
        );
    }

    #[test]
    fn parse_describe_prerelease_tag_with_hyphens() {
        assert_eq!(
            parse_describe("v1.0.0-alpha.5-4-gdeadbee").as_deref(),
            Some("count=4 hash=deadbee dirty=0")
        );
    }

    #[test]
    fn parse_describe_garbage() {
        assert_eq!(parse_describe("not-a-describe"), None);
        assert_eq!(parse_describe(""), None);
    }

    #[test]
    fn compose_ahead() {
        assert_eq!(
            compose("0.1.0", Some("count=5 hash=1bd6f7a dirty=0")),
            "0.1.0 +5 (g1bd6f7a)"
        );
    }

    #[test]
    fn compose_on_tag_bare() {
        assert_eq!(
            compose("0.1.0", Some("count=0 hash=1bd6f7a dirty=0")),
            "0.1.0"
        );
    }

    #[test]
    fn compose_dirty() {
        assert_eq!(
            compose("0.1.0", Some("count=5 hash=1bd6f7a dirty=1")),
            "0.1.0 +5 (g1bd6f7a-dirty)"
        );
    }

    #[test]
    fn compose_on_tag_dirty_flags_uncommitted() {
        assert_eq!(
            compose("0.1.0", Some("count=0 hash=1bd6f7a dirty=1")),
            "0.1.0 (g1bd6f7a-dirty)"
        );
    }

    #[test]
    fn compose_hash_only_unknown_count() {
        assert_eq!(
            compose("0.1.0", Some("hash=1bd6f7a dirty=0")),
            "0.1.0 (g1bd6f7a)"
        );
    }

    #[test]
    fn compose_no_build_info_bare() {
        assert_eq!(compose("0.1.0", None), "0.1.0");
        assert_eq!(compose("0.1.0", Some("")), "0.1.0");
    }

    #[test]
    fn compose_non_semver_base_bare() {
        assert_eq!(compose("dev", Some("count=5 hash=1bd6f7a dirty=0")), "dev");
        assert_eq!(compose("", Some("count=5 hash=1bd6f7a dirty=0")), "");
    }

    #[test]
    fn compose_count_only_no_hash() {
        assert_eq!(compose("0.1.0", Some("count=3 dirty=0")), "0.1.0 +3");
    }

    #[test]
    fn buildinfo_parse_empty() {
        assert_eq!(BuildInfo::parse("  "), BuildInfo::default());
    }
}
