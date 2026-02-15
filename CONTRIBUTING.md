# Contributing to foia

## Development Setup

```bash
# Clone the repository
git clone https://github.com/foiacquire/foia
cd foia

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

### HTTP Client Usage

**CRITICAL: Never use `reqwest::Client` directly**

All HTTP operations must go through `crate::scrapers::HttpClient` to ensure:
- Privacy/Tor support is properly handled
- SOCKS proxy configuration is respected
- Rate limiting is applied consistently
- Request logging works correctly

```rust
// ❌ WRONG - Direct reqwest usage
let client = reqwest::Client::new();
let response = client.get(url).send().await?;

// ✅ CORRECT - Use HttpClient with privacy config
use crate::scrapers::HttpClient;
use crate::privacy::PrivacyConfig;
use std::time::Duration;

let client = HttpClient::with_privacy(
    "my_source",
    Duration::from_secs(30),      // timeout
    Duration::from_millis(1000),  // rate limit delay
    None,                         // user agent (None = default)
    &privacy_config,              // privacy configuration
)?;

let response = client.get_text(url).await?;
```

**Available HttpClient methods:**
- `get(url, etag, last_modified)` - GET request with conditional headers
- `get_text(url)` - GET request returning text
- `post(url, form_data)` - POST with form-encoded data
- `post_json(url, json_data)` - POST with JSON body
- `head(url, etag, last_modified)` - HEAD request

**Clippy enforcement:**
A clippy lint (`disallowed-methods`) will flag any direct `reqwest::Client` usage.
Only `src/scrapers/http_client/mod.rs` is allowed to use reqwest directly.

## Privacy Enforcement & Automation

foia has multiple layers of automated privacy enforcement to prevent accidental leaks:

### 1. Clippy Disallowed Methods

The `clippy.toml` configuration bans direct `reqwest::Client` usage:

```bash
# This will fail if you use reqwest directly
cargo clippy --all-features
```

### 2. Privacy Check Script

Run comprehensive privacy checks:

```bash
./scripts/check-privacy.sh
```

This script checks for:
- Direct reqwest usage (bypassing HttpClient wrapper)
- Network commands (curl, wget, yt-dlp) without proxy handling
- Direct TCP/UDP sockets that might bypass proxy
- DNS resolution that bypasses SOCKS proxy
- Missing clippy.toml configuration
- HttpClient constructors not respecting environment

### 3. Pre-commit Hook (Optional but Recommended)

Install the pre-commit hook to catch issues before committing:

```bash
# Option 1: Symlink (recommended - stays up to date)
ln -sf ../../scripts/pre-commit-hook .git/hooks/pre-commit

# Option 2: Copy
cp scripts/pre-commit-hook .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

The hook runs automatically on `git commit` and blocks commits with privacy violations.

### 4. CI/CD Privacy Audit

Every pull request automatically runs privacy checks in GitHub Actions:
- **Privacy Audit** job runs `check-privacy.sh`
- **Clippy** job runs with `-D warnings` to fail on disallowed methods

See `.github/workflows/ci.yml` for details.

### External Network Commands

When using external commands like `curl`, `wget`, or `yt-dlp`:

```rust
// ✅ CORRECT - Respect SOCKS_PROXY environment variable
let socks_proxy = std::env::var("SOCKS_PROXY").ok();
let mut cmd = Command::new("curl");

if let Some(ref proxy) = socks_proxy {
    cmd.args(["--proxy", proxy]);
}
```

### Documenting Exceptions

If you have a legitimate reason to bypass privacy (e.g., localhost connections):

```rust
// ALLOWED: Chrome DevTools Protocol connects to localhost/Docker
// Privacy/Tor routing is not applicable for browser control protocol
#[allow(clippy::disallowed_methods)]
let client = reqwest::Client::new();
```

**Requirements for exceptions:**
1. Add `#[allow(clippy::disallowed_methods)]` annotation
2. Include a clear comment explaining WHY it's safe
3. Document what the connection is for (e.g., "localhost", "internal Docker network")

### Privacy Testing

Before submitting a PR, test with SOCKS_PROXY:

```bash
# Start Tor
sudo systemctl start tor

# Set SOCKS proxy (Tor default)
export SOCKS_PROXY="socks5://127.0.0.1:9050"

# Run your code and verify it uses the proxy
cargo run -- <your-command>

# Verify with tcpdump (requires root)
sudo tcpdump -i any -n "not host 127.0.0.1 and not port 9050"
# Should see NO external connections
```

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

foia follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (x.0.0): Breaking changes to core functionality, database schema migrations required
- **MINOR** (0.x.0): New features, CLI changes, backwards-incompatible config changes
- **PATCH** (0.0.x): Bug fixes, documentation, minor improvements

While in 0.x.y development, minor versions may contain breaking changes.

### Release Process

**You must run all checks locally before tagging.** CI will reject releases that fail these checks.

```bash
# 1. Run all checks (required before tagging)
cargo fmt --check
cargo check --all-features
cargo clippy --all-features -- -D warnings
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
