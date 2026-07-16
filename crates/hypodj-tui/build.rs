// Emits HYPODJ_BUILD_INFO for this binary so `--version` can render the enriched
// display version (semver + commits-since-tag + git short hash) on source builds.
// Never fails the build - git problems yield an empty value.
fn main() {
    hypodj_build_info::emit();
}
