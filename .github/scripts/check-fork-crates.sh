#!/usr/bin/env bash
# Fails unless each vendored fork crate is on crates.io, byte-for-byte, at the
# version atmoq pins; otherwise release-plz can't publish atmoq (or ships stale forks).
set -euo pipefail
cd "$(dirname "$0")/../../rust"

pins=$(cargo metadata --format-version 1 --no-deps |
	jq -r '.packages[] | select(.name == "atmoq") | .dependencies[] | "\(.name) \(.req)"')
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
status=0

for dir in vendor/moq-net vendor/moq-native; do
	read -r name version < <(cargo metadata --format-version 1 --no-deps --manifest-path "$dir/Cargo.toml" |
		jq -r '.packages[0] | "\(.name) \(.version)"')
	pin=$(awk -v n="$name" '$1 == n { print $2 }' <<<"$pins")
	if [ "$pin" != "^$version" ]; then
		echo "::error::crates/atmoq pins $name $pin, but rust/$dir is $version; pin the vendored version"
		status=1
		continue
	fi

	crate="$tmp/$name-$version.crate"
	code=$(curl -sSL -o "$crate" -w '%{http_code}' "https://static.crates.io/crates/$name/$name-$version.crate")
	case "$code" in
	200) ;;
	403 | 404)
		echo "::error::$name $version is not on crates.io; run \`cargo publish\` in rust/$dir before merging"
		status=1
		continue
		;;
	*)
		echo "::error::fetching $name $version from crates.io returned HTTP $code"
		status=1
		continue
		;;
	esac

	mkdir -p "$tmp/published" "$tmp/local/$name"
	tar -xzf "$crate" -C "$tmp/published"
	# Rebuild the package's file set from the working tree; Cargo.toml.orig is the source manifest.
	(cd "$dir" && cargo package --list --allow-dirty 2>/dev/null) | while read -r f; do
		case "$f" in
		.cargo_vcs_info.json | Cargo.toml | Cargo.lock) ;; # generated at package time
		Cargo.toml.orig) cp "$dir/Cargo.toml" "$tmp/local/$name/Cargo.toml.orig" ;;
		*) mkdir -p "$tmp/local/$name/$(dirname "$f")" && cp "$dir/$f" "$tmp/local/$name/$f" ;;
		esac
	done
	if ! diff -r -x .cargo_vcs_info.json -x Cargo.toml -x Cargo.lock \
		"$tmp/published/$name-$version" "$tmp/local/$name" >"$tmp/$name.diff"; then
		echo "::error::rust/$dir differs from published $name $version; bump its version, publish it, and update the pin in crates/atmoq/Cargo.toml"
		head -40 "$tmp/$name.diff"
		status=1
	else
		echo "$name $version matches crates.io"
	fi
done
exit $status
