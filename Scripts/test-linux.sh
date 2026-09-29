#!/bin/bash
# Runs the test suite on Linux locally with Apple's `container` tool, matching CI:
# Swift 6.4 on Ubuntu 24.04, libmongoc 2.5.5 built from source, MongoDB 8.
#
#   Scripts/test-linux.sh                    # full suite
#   Scripts/test-linux.sh --filter Phase4    # extra arguments go to `swift test`
#
# Needs `container system start` to have been run once. The first run compiles
# libmongoc into the `perfect-mongodb-mongoc` volume (a few minutes); later runs reuse
# it. Linux build products live in the `perfect-mongodb-build` volume, separate from
# the macOS .build directory. No image build (`container build`) is involved.

set -euo pipefail

SWIFT_IMAGE="docker.io/library/swift:6.4-noble"
MONGO_IMAGE="docker.io/library/mongo:8"
MONGOC_VERSION="2.5.5"
MONGOC_VOLUME="perfect-mongodb-mongoc"
BUILD_VOLUME="perfect-mongodb-build"
MONGO="perfect-mongodb-test-mongo"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

for volume in "$MONGOC_VOLUME" "$BUILD_VOLUME"; do
	container volume inspect "$volume" >/dev/null 2>&1 || container volume create "$volume" >/dev/null
done

# One-time: build libmongoc into the volume. The marker file records the version.
if ! container run --rm --volume "$MONGOC_VOLUME:/opt/mongoc" "$SWIFT_IMAGE" \
	test -f "/opt/mongoc/.version-$MONGOC_VERSION" >/dev/null 2>&1; then
	echo "Building libmongoc $MONGOC_VERSION into the $MONGOC_VOLUME volume (first run only)..."
	container run --rm --cpus 4 --memory 4G --volume "$MONGOC_VOLUME:/opt/mongoc" "$SWIFT_IMAGE" bash -euc "
		apt-get update -qq
		apt-get install -y -qq --no-install-recommends build-essential cmake curl libssl-dev libsasl2-dev libzstd-dev >/dev/null
		rm -rf /opt/mongoc/*
		curl -fsSL https://github.com/mongodb/mongo-c-driver/releases/download/$MONGOC_VERSION/mongo-c-driver-$MONGOC_VERSION.tar.gz | tar xz -C /tmp
		cmake -S /tmp/mongo-c-driver-$MONGOC_VERSION -B /tmp/mongoc-build -DCMAKE_BUILD_TYPE=Release \
			-DCMAKE_INSTALL_PREFIX=/opt/mongoc -DENABLE_TESTS=OFF -DENABLE_EXAMPLES=OFF >/dev/null
		cmake --build /tmp/mongoc-build --parallel 4 >/dev/null
		cmake --install /tmp/mongoc-build >/dev/null
		touch /opt/mongoc/.version-$MONGOC_VERSION
	"
fi

cleanup() {
	container stop "$MONGO" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
container run --detach --rm --name "$MONGO" "$MONGO_IMAGE" >/dev/null
MONGO_IP="$(container inspect "$MONGO" | python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["status"]["networks"][0]["ipv4Address"].split("/")[0])')"
echo "MongoDB at $MONGO_IP"

container run --rm \
	--cpus 4 --memory 6G \
	--volume "$ROOT:/work" \
	--volume "$BUILD_VOLUME:/build" \
	--volume "$MONGOC_VOLUME:/opt/mongoc" \
	--workdir /work \
	--env "MONGODB_URI=mongodb://$MONGO_IP:27017" \
	"$SWIFT_IMAGE" \
	bash -c '
		export PKG_CONFIG_PATH=$(dirname $(find /opt/mongoc -name mongoc2.pc))
		export LD_LIBRARY_PATH=$(dirname $(find /opt/mongoc -name "libmongoc2.so" | head -1))
		for i in $(seq 1 30); do (echo > /dev/tcp/'"$MONGO_IP"'/27017) 2>/dev/null && break; sleep 1; done
		swift test --scratch-path /build "$@"
	' bash "$@"
