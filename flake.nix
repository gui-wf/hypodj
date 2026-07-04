{
  description = "subsonity - MPD-speaking OpenSubsonic client daemon";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        devShells.default = pkgs.mkShell {
          # Rust toolchain + pkg-config + the audio system lib (libmpv).
          # Reproducible: the build finds mpv via pkg-config here.
          nativeBuildInputs = [
            pkgs.rustc
            pkgs.cargo
            pkgs.pkg-config
            pkgs.rust-analyzer
            pkgs.clippy
          ];
          buildInputs = [
            # provides libmpv + mpv.pc for libmpv2 (Phase 1); the foundation is
            # link-light and does not yet depend on it, but the devshell ships it
            # so adding the dep in Phase 1 is a one-line change that will link.
            pkgs.mpv-unwrapped
            # NOTE: no openssl. opensubsonic 0.3 pulls reqwest 0.13 with
            # default-features=false + "rustls" (verified in its Cargo.toml), so
            # TLS is aws-lc-rs/rustls, not OpenSSL. Adding openssl here would be a
            # devshell input that nothing actually links against.
          ];
          # Honor the hard build cap: never saturate the machine.
          CARGO_BUILD_JOBS = "2";
          PKG_CONFIG_PATH = "${pkgs.mpv-unwrapped.dev}/lib/pkgconfig";
        };
      });
}
