# foia Scripts

This directory contains utility scripts for development, CI/CD, and automation.

## Privacy & Security

### `check-privacy.sh`

Comprehensive privacy violation detection.

**Purpose:** Automatically detect code that might bypass Tor/SOCKS proxy configuration.

**Usage:**
```bash
./scripts/check-privacy.sh
```

**Checks:**
- Direct `reqwest::Client` usage (should use HttpClient wrapper)
- Network commands (curl, wget, yt-dlp) without proxy handling
- Direct TCP/UDP sockets that bypass proxy
- DNS resolution that bypasses SOCKS proxy
- Clippy configuration integrity
- HttpClient constructor environment handling

**Exit codes:**
- `0`: All checks passed
- `1`: Violations found

**CI Integration:** Runs on every PR via `.github/workflows/ci.yml`

---

### `pre-commit-hook`

Git pre-commit hook for fast privacy checks on staged files.

**Purpose:** Catch privacy issues before they're committed.

**Installation:**
```bash
# Option 1: Symlink (recommended - stays up to date)
ln -sf ../../scripts/pre-commit-hook .git/hooks/pre-commit

# Option 2: Copy
cp scripts/pre-commit-hook .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

**What it checks:**
- Direct reqwest usage in staged files
- Network commands without proxy in staged files
- Runs clippy on staged files

**Bypass:**
```bash
git commit --no-verify  # Not recommended
```

---

## Database

### `check-migrations.sh`

Ensures database migrations remain immutable after release.

**Purpose:** Prevent modifications to migrations that may have already been applied in production.

**Usage:**
```bash
./scripts/check-migrations.sh
```

**CI Integration:** Runs on every push/PR via `migrations-check` job.

**Details:** Once a migration is included in a tagged release, it cannot be modified. Create a new migration instead.

---

---

## Contributing

When adding new scripts:

1. **Make executable:**
   ```bash
   chmod +x scripts/your-script.sh
   ```

2. **Add shebang:**
   ```bash
   #!/bin/bash
   set -e  # Exit on error
   ```

3. **Document here:** Add entry to this README

4. **Add usage in script:**
   ```bash
   # Show usage if no args
   if [ $# -eq 0 ]; then
       echo "Usage: $0 <args>"
       exit 1
   fi
   ```

5. **Test locally:** Run script multiple times to ensure idempotency

6. **Add to CI if needed:** Update `.github/workflows/ci.yml`

---

## Script Conventions

- **Exit codes:**
  - `0`: Success
  - `1`: General error
  - `2`: Invalid usage/args

- **Error handling:**
  - Use `set -e` to fail on errors
  - Provide clear error messages
  - Clean up temp files on exit

- **Output:**
  - Use color codes for clarity (red=error, yellow=warning, green=success)
  - Provide progress indicators for long operations
  - Log to stderr for errors, stdout for output

- **Variables:**
  - Use `UPPERCASE` for environment variables
  - Use `lowercase` for local variables
  - Quote variables: `"$VAR"` not `$VAR`

---

## See Also

- CI configuration: `../.github/workflows/docker.yml`
