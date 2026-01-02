#!/bin/bash
# Check that:
# 1. Existing migration files haven't been modified (immutability)
# 2. All postgres migrations are registered in migrations.rs

set -e

MIGRATIONS_DIR="migrations"
MIGRATIONS_RS="src/repository/migrations.rs"

# Get the base commit to compare against
if [ -n "$GITHUB_BASE_REF" ]; then
    # In a PR, compare against the base branch
    BASE="origin/$GITHUB_BASE_REF"
elif [ -n "$CI" ]; then
    # In CI on main branch, compare against previous commit
    BASE="HEAD~1"
else
    # Local development - compare against HEAD (staged changes)
    BASE="HEAD"
fi

# Get list of migration FILES (not directories) that existed in the base commit
EXISTING_MIGRATIONS=$(git ls-tree -r --name-only "$BASE" -- "$MIGRATIONS_DIR" 2>/dev/null || echo "")

if [ -z "$EXISTING_MIGRATIONS" ]; then
    echo "No existing migrations to check."
    exit 0
fi

# Check if any existing migration files were modified (not just if new files were added)
MODIFIED=0
for migration in $EXISTING_MIGRATIONS; do
    # Check if this specific file has content changes (not just that new files exist nearby)
    if git diff --name-only "$BASE" HEAD -- "$migration" | grep -qx "$migration"; then
        echo "ERROR: Migration file was modified: $migration"
        echo "       Migrations are immutable. Create a new migration instead."
        MODIFIED=1
    fi
done

if [ $MODIFIED -eq 1 ]; then
    echo ""
    echo "To add a new migration, create a file like:"
    echo "  migrations/$(date +%Y%m%d%H%M%S)_description.sql"
    exit 1
fi

echo "Migration immutability check passed."

# Check that all postgres migrations are registered in POSTGRES_MIGRATION_FILES
echo ""
echo "Checking postgres migration registration..."

UNREGISTERED=0
for migration_dir in migrations/postgres/*/; do
    # Extract version from directory name (e.g., "2025-01-01-200000" from "2025-01-01-200000_archive_history")
    dir_name=$(basename "$migration_dir")
    version=$(echo "$dir_name" | sed 's/_.*$//')

    # Check if this version is in migrations.rs
    if ! grep -q "\"$version\"" "$MIGRATIONS_RS" 2>/dev/null; then
        echo "ERROR: Postgres migration not registered: $dir_name"
        echo "       Add it to POSTGRES_MIGRATION_FILES in $MIGRATIONS_RS"
        UNREGISTERED=1
    fi
done

if [ $UNREGISTERED -eq 1 ]; then
    echo ""
    echo "All postgres migrations must be registered in POSTGRES_MIGRATION_FILES."
    echo "SQLite migrations are auto-discovered, but postgres requires manual registration."
    exit 1
fi

echo "Postgres migration registration check passed."
