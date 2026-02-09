#!/bin/sh
# Verify CI workflow files reference the correct binary name from Cargo.toml
# This prevents regressions where the binary name changes but CI isn't updated

set -e

BINARY_NAME=$(grep -A1 '^\[\[bin\]\]' crates/foiacquire-cli/Cargo.toml | grep 'name' | cut -d'"' -f2)

if [ -z "$BINARY_NAME" ]; then
    echo "ERROR: Could not find [[bin]] name in Cargo.toml"
    exit 1
fi

echo "Binary name from Cargo.toml: $BINARY_NAME"

ERRORS=0

# Check CI workflow
if grep -q "target/release/foiacquire" .github/workflows/ci.yml 2>/dev/null; then
    echo "ERROR: .github/workflows/ci.yml references 'foiacquire' but binary is '$BINARY_NAME'"
    ERRORS=$((ERRORS + 1))
fi

# Check release workflow
if grep -q "release/foiacquire" .github/workflows/release.yml 2>/dev/null; then
    echo "ERROR: .github/workflows/release.yml references 'foiacquire' but binary is '$BINARY_NAME'"
    ERRORS=$((ERRORS + 1))
fi

# Verify correct name IS present
if ! grep -q "target/release/$BINARY_NAME" .github/workflows/ci.yml 2>/dev/null; then
    echo "ERROR: .github/workflows/ci.yml does not reference 'target/release/$BINARY_NAME'"
    ERRORS=$((ERRORS + 1))
fi

if [ $ERRORS -gt 0 ]; then
    echo ""
    echo "Found $ERRORS error(s). Update CI workflows to use binary name: $BINARY_NAME"
    exit 1
fi

echo "✓ All CI workflows reference correct binary name: $BINARY_NAME"
