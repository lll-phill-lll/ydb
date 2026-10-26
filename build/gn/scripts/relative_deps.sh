#!/bin/sh
# Compiler launcher: runs the compiler, then makes the paths of its -MF file
# relative to the build directory, the way build.ninja spells them.
#
# usage: relative_deps.sh <abs build dir>/ <abs source root>/ <source root from the build dir>/ <command...>
#
# clang writes the headers that come from a PCH by absolute paths. ninja
# takes /abs/out/gen/x.h and gen/x.h for different files, so a header
# regenerated through a PCH would rebuild its users only in the next run.
build=$1
root=$2
rel=$3
shift 3

dep=
prev=
for a in "$@"; do
  [ "$prev" = -MF ] && dep=$a
  prev=$a
done

"$@" || exit

[ -n "$dep" ] || exit 0
quote() { printf '%s' "$1" | sed 's/[][\.*^$|]/\\&/g'; }
exec sed -i -e "s|$(quote "$build")||g" -e "s|$(quote "$root")|$rel|g" "$dep"
