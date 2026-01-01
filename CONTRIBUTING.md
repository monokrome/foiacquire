# Contributing to FOIAcquire

## Development Setup

```bash
# Clone the repository
git clone https://github.com/monokrome/foiacquire
cd foiacquire

# Build
cargo build

# Run tests
cargo test

# Run with verbose logging
RUST_LOG=debug cargo run -- <command>
```

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` and address warnings
- Follow existing patterns in the codebase
- Keep functions focused and under 50 lines when possible

## Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `cargo test`
5. Run formatting: `cargo fmt`
6. Submit a PR against `main`

## Database Migrations

**Migrations are immutable.** Once a migration has been released (included in a tagged version), it cannot be modified. This is because migrations may have already been applied to production databases.

If you need to fix a released migration:
1. Create a new migration that applies the fix
2. Never edit existing migration files in `migrations/`

The CI pipeline enforces this - commits that modify existing migrations will be rejected.

## Releases

### Version Numbering

FOIAcquire follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (x.0.0): Breaking changes to core functionality, database schema migrations required
- **MINOR** (0.x.0): New features, CLI changes, backwards-incompatible config changes
- **PATCH** (0.0.x): Bug fixes, documentation, minor improvements

While in 0.x.y development, minor versions may contain breaking changes.

### Release Process

**You must run all checks locally before tagging.** CI will reject releases that fail these checks.

```bash
# 1. Run all checks (required before tagging)
cargo fmt --check
cargo clippy
cargo test
cargo build --release

# 2. Update version and commit
# Edit Cargo.toml: version = "0.x.y"
git add Cargo.toml Cargo.lock
git commit -m "chore: bump version to 0.x.y"

# 3. Tag and push (only after checks pass)
git tag v0.x.y
git push origin main
git push origin v0.x.y
```

**Post-release:**
- [ ] Verify CI/CD pipeline succeeds
- [ ] Verify Docker images published to docker.io/monokrome

### Breaking Changes

Requires **minor** version bump:
- CLI argument/subcommand changes
- Config file format changes
- Changing default behavior

Requires **patch** version bump:
- Bug fixes
- New optional features
- Documentation updates
