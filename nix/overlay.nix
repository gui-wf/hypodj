# The hypodj overlay: adds `pkgs.hypodj`. Shared by flake.overlays.default and
# imported automatically by the NixOS module so `services.hypodj.package`'s
# default (`pkgs.hypodj`) resolves without the user wiring the overlay by hand.
final: prev: {
  # cargoVersion is read from the workspace Cargo.toml (the single semver SSOT)
  # so pkgs.hypodj - the NixOS module's default services.hypodj.package - carries
  # the same derivation version as the flake package and `--version`, instead of
  # silently falling back to package.nix's hardcoded default.
  hypodj = final.callPackage ./package.nix {
    cargoVersion = (builtins.fromTOML (builtins.readFile ../Cargo.toml)).workspace.package.version;
  };
}
