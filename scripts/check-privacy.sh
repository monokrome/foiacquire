#!/bin/bash
# Privacy violation detection script for FOIAcquire
# Run this in CI or as a pre-commit hook to catch privacy issues early

set -e

echo "🔒 Checking for privacy violations..."
echo ""

VIOLATIONS=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Check for direct reqwest usage (should be caught by clippy, but double-check)
echo "1. Checking for direct reqwest::Client usage..."
if git grep -n "reqwest::Client::" -- '*.rs' \
    | grep -v "src/scrapers/http_client/mod.rs" \
    | grep -v "^Binary file" \
    | grep -v "\.git/" \
    | grep -v "target/" > /tmp/reqwest_all.txt 2>/dev/null; then
    # Filter out files that have allow annotations within 5 lines before the usage
    while IFS=: read -r file line content; do
        # Check if there's an allow annotation within 5 lines before
        if ! sed -n "$((line-5)),$((line-1))p" "$file" 2>/dev/null | grep -q "allow.*clippy::disallowed_methods"; then
            echo "$file:$line:$content"
        fi
    done < /tmp/reqwest_all.txt > /tmp/reqwest_violations.txt

    if [ -s /tmp/reqwest_violations.txt ]; then
        echo -e "${RED}❌ Found direct reqwest::Client usage without allow annotation:${NC}"
        cat /tmp/reqwest_violations.txt
        VIOLATIONS=$((VIOLATIONS + 1))
    else
        echo -e "${GREEN}✓ No unauthorized reqwest usage${NC}"
    fi
else
    echo -e "${GREEN}✓ No direct reqwest usage${NC}"
fi
echo ""

# 2. Check for Command::new with network tools without proxy handling
echo "2. Checking for network commands without proxy support..."
NETWORK_COMMANDS="curl|wget|yt-dlp|aria2c|axel"
if git grep -n "Command::new.*\($NETWORK_COMMANDS\)" -- '*.rs' \
    | while IFS=: read -r file line content; do
        # Check if SOCKS_PROXY or proxy is mentioned within 20 lines
        if ! sed -n "$((line-20)),$((line+20))p" "$file" 2>/dev/null | grep -q "SOCKS_PROXY\|proxy_url\|--proxy"; then
            echo "$file:$line:$content"
        fi
    done > /tmp/command_violations.txt 2>/dev/null && [ -s /tmp/command_violations.txt ]; then
    echo -e "${RED}❌ Found network commands without proxy handling:${NC}"
    cat /tmp/command_violations.txt
    echo -e "${YELLOW}⚠️  Ensure these commands respect SOCKS_PROXY environment variable${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
else
    echo -e "${GREEN}✓ All network commands appear to handle proxies${NC}"
fi
echo ""

# 3. Check for TcpStream::connect (direct TCP, bypasses proxy)
echo "3. Checking for direct TCP connections..."
if git grep -n "TcpStream::connect\|UdpSocket::bind" -- '*.rs' \
    | grep -v "allow.*privacy" \
    | grep -v "^Binary file" \
    | grep -v "\.git/" \
    | grep -v "target/" > /tmp/tcp_violations.txt 2>/dev/null && [ -s /tmp/tcp_violations.txt ]; then
    echo -e "${YELLOW}⚠️  Found direct TCP/UDP usage (may bypass proxy):${NC}"
    cat /tmp/tcp_violations.txt
    echo -e "${YELLOW}⚠️  Review these to ensure they're not for external connections${NC}"
    # Don't increment violations - this is just a warning, as localhost TCP is often legitimate
else
    echo -e "${GREEN}✓ No direct TCP/UDP sockets found${NC}"
fi
echo ""

# 4. Check for DNS resolution that bypasses SOCKS
echo "4. Checking for DNS resolution..."
if git grep -n "to_socket_addrs\|lookup_host\|dns::lookup" -- '*.rs' \
    | grep -v "allow.*privacy\|PRIVACY NOTE\|internal infrastructure" \
    | grep -v "^Binary file" \
    | grep -v "\.git/" \
    | grep -v "target/" > /tmp/dns_violations.txt 2>/dev/null && [ -s /tmp/dns_violations.txt ]; then
    echo -e "${YELLOW}⚠️  Found DNS resolution (may leak if not through SOCKS5h):${NC}"
    cat /tmp/dns_violations.txt
    echo -e "${YELLOW}⚠️  Ensure these are for localhost/internal infrastructure only${NC}"
    # Warning only - legitimate for localhost
else
    echo -e "${GREEN}✓ No problematic DNS resolution found${NC}"
fi
echo ""

# 5. Check that clippy disallowed-methods is still in place
echo "5. Checking clippy configuration..."
if [ ! -f "clippy.toml" ]; then
    echo -e "${RED}❌ clippy.toml missing!${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
elif ! grep -q "disallowed-methods" clippy.toml; then
    echo -e "${RED}❌ clippy.toml missing disallowed-methods configuration!${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
elif ! grep -q "reqwest::Client" clippy.toml; then
    echo -e "${RED}❌ clippy.toml missing reqwest::Client in disallowed-methods!${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
else
    echo -e "${GREEN}✓ clippy.toml configured correctly${NC}"
fi
echo ""

# 6. Check for HttpClient constructors to ensure they read env
echo "6. Checking HttpClient constructors respect environment..."
if ! grep -q "with_env_overrides" src/http_client/mod.rs; then
    echo -e "${RED}❌ HttpClient doesn't appear to call with_env_overrides${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
else
    echo -e "${GREEN}✓ HttpClient constructors respect environment${NC}"
fi
echo ""

# 7. Run clippy to catch disallowed methods
echo "7. Running clippy for disallowed methods..."
if cargo clippy --all-features -- -D clippy::disallowed-methods 2>&1 | grep "error.*disallowed" > /tmp/clippy_violations.txt; then
    echo -e "${RED}❌ Clippy found disallowed method usage:${NC}"
    cat /tmp/clippy_violations.txt
    VIOLATIONS=$((VIOLATIONS + 1))
else
    echo -e "${GREEN}✓ No disallowed methods detected by clippy${NC}"
fi
echo ""

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ $VIOLATIONS -eq 0 ]; then
    echo -e "${GREEN}✅ All privacy checks passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Found $VIOLATIONS privacy violation(s)${NC}"
    echo ""
    echo "Please fix these issues or add appropriate documentation/allow annotations"
    echo "See CONTRIBUTING.md for privacy guidelines"
    exit 1
fi
