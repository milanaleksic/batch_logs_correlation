let
  pkgs = import <nixpkgs> {};
in

with pkgs;
mkShell {
  buildInputs = [
    go_1_17
  ];
  shellHook = ''
    # avoid broken github.com/mattn/go-sqlite3
    export CC=gcc
    export LANG=en_US.UTF-8
  '';
}
