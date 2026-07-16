{
  description = "hypodj - MPD-speaking OpenSubsonic client daemon";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      # ONE shared module definition, parameterized by flavor. Lives outside the
      # per-system block so the modules evaluate system-independently.
      mkHypodjModule = import ./nix/hypodj-module.nix;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # The released semver, read from the workspace manifest so the derivation
        # version tracks the tag and is bumped in exactly one place at release.
        cargoVersion = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;

        # DISPLAY-only build info baked into the runtime env of the wrapped bins,
        # so a nix-built binary (no .git in the sandbox) still shows its short
        # hash via `--version`. Mirrors the HYPODJ_BUILD_INFO grammar emitted by
        # the build.rs on source builds: `count=<N> hash=<short> dirty=<0|1>`.
        #
        # - biHash: bare short commit hash. self.dirtyShortRev carries a trailing
        #   "-dirty" suffix, so strip it - the dirty state is carried by biDirty.
        #   Parens are load-bearing: `or` binds looser than function application.
        # - biDirty: a clean tree exposes self.rev; a dirty one does not.
        # - anchor: optional released-commit revCount (nix/release-anchor.nix); it
        #   lets count= be commits-since-release. Omitted (no count=) when absent.
        anchor = if builtins.pathExists ./nix/release-anchor.nix then import ./nix/release-anchor.nix else null;
        biHash = self.shortRev or (nixpkgs.lib.removeSuffix "-dirty" (self.dirtyShortRev or ""));
        biDirty = !(self ? rev);
        biCount = if (self ? revCount) && anchor != null then self.revCount - anchor.revCount else null;
        buildInfo =
          if biHash == "" then
            ""
          else
            nixpkgs.lib.concatStringsSep " " (
              (nixpkgs.lib.optional (biCount != null && biCount >= 0) "count=${toString biCount}")
              ++ [ "hash=${biHash}" "dirty=${if biDirty then "1" else "0"}" ]
            );
      in
      {
        packages.hypodj = pkgs.callPackage ./nix/package.nix { inherit buildInfo cargoVersion; };
        # The client bins (dj + dj-gui) - libmpv-free, so a separate, lighter
        # derivation that a workstation can install without pulling mpv.
        packages.hypodj-clients = pkgs.callPackage ./nix/clients.nix { inherit buildInfo cargoVersion; };
        packages.default = self.packages.${system}.hypodj;

        apps.hypodj = {
          type = "app";
          program = "${self.packages.${system}.hypodj}/bin/hypodj";
        };
        apps.dj = {
          type = "app";
          program = "${self.packages.${system}.hypodj-clients}/bin/dj";
        };
        apps.dj-gui = {
          type = "app";
          program = "${self.packages.${system}.hypodj-clients}/bin/dj-gui";
        };
        apps.default = self.apps.${system}.dj;

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
      })
    // {
      # System-independent outputs: the two modules + the overlay.
      overlays.default = import ./nix/overlay.nix;

      nixosModules.default = mkHypodjModule { flavor = "nixos"; };
      homeManagerModules.default = mkHypodjModule { flavor = "home-manager"; };
    };
}
