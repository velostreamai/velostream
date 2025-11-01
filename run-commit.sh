#!/bin/bash

# Velostream Pre-Commit Runner and Auto-Commit Script
# Runs comprehensive pre-commit checks and commits if all pass

set -e  # Exit on first error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧹 Running Velostream pre-commit checks...${NC}"
echo ""

# Stage 1: Fast Feedback Checks
echo -e "${YELLOW}⚡ Stage 1: Fast Feedback Checks${NC}"
echo ""

echo -e "${BLUE}1️⃣ Checking code formatting...${NC}"
if cargo fmt --all -- --check; then
    echo -e "${GREEN}✅ Code formatting passed${NC}"
else
    echo -e "${RED}❌ Formatting check failed. Running cargo fmt to fix...${NC}"
    cargo fmt --all
    echo -e "${GREEN}✅ Code formatted. Please review the changes.${NC}"
fi
echo ""

echo -e "${BLUE}2️⃣ Checking compilation...${NC}"
if cargo check --all-targets --no-default-features; then
    echo -e "${GREEN}✅ Compilation passed${NC}"
else
    echo -e "${RED}❌ Compilation failed. Please fix errors before committing.${NC}"
    exit 1
fi
echo ""

echo -e "${BLUE}3️⃣ Running clippy linting...${NC}"
if cargo clippy --all-targets --no-default-features; then
    echo -e "${GREEN}✅ Clippy linting passed${NC}"
else
    echo -e "${RED}❌ Clippy linting failed. Please fix warnings.${NC}"
    exit 1
fi
echo ""

echo -e "${BLUE}4️⃣ Verifying test registration...${NC}"
UNREGISTERED=$(find tests/unit -name "*_test.rs" -type f 2>/dev/null | while read test_file; do
    test_name=$(basename "$test_file" .rs)
    mod_file=$(dirname "$test_file")/mod.rs
    if [ -f "$mod_file" ] && ! grep -q "pub mod $test_name" "$mod_file"; then
        echo "⚠️  $test_file not registered in $mod_file"
    fi
done)
if [ -n "$UNREGISTERED" ]; then
    echo -e "${RED}❌ Found unregistered test files:${NC}"
    echo "$UNREGISTERED"
    echo -e "${YELLOW}Run: echo 'pub mod <test_name>;' >> <mod_file>${NC}"
    exit 1
fi
echo -e "${GREEN}✅ All test files registered${NC}"
echo ""

echo -e "${BLUE}5️⃣ Running unit tests...${NC}"
if cargo test --lib --no-default-features --quiet; then
    echo -e "${GREEN}✅ Unit tests passed${NC}"
else
    echo -e "${RED}❌ Unit tests failed.${NC}"
    exit 1
fi
echo ""

# Stage 2: Comprehensive Validation
echo -e "${YELLOW}🔄 Stage 2: Comprehensive Validation${NC}"
echo ""

echo -e "${BLUE}6️⃣ Testing example compilation...${NC}"
if cargo build --examples --no-default-features --quiet; then
    echo -e "${GREEN}✅ Examples compiled successfully${NC}"
else
    echo -e "${RED}❌ Example compilation failed.${NC}"
    exit 1
fi
echo ""

echo -e "${BLUE}7️⃣ Testing binary compilation...${NC}"
if cargo build --bins --no-default-features --quiet; then
    echo -e "${GREEN}✅ Binaries compiled successfully${NC}"
else
    echo -e "${RED}❌ Binary compilation failed.${NC}"
    exit 1
fi
echo ""

echo -e "${BLUE}8️⃣ Running comprehensive test suite...${NC}"
if cargo test --tests --no-default-features --quiet -- --skip integration:: --skip performance::; then
    echo -e "${GREEN}✅ Comprehensive tests passed${NC}"
else
    echo -e "${RED}❌ Comprehensive tests failed.${NC}"
    exit 1
fi
echo ""

echo -e "${BLUE}9️⃣ Running documentation tests...${NC}"
if cargo test --doc --no-default-features --quiet; then
    echo -e "${GREEN}✅ Documentation tests passed${NC}"
else
    echo -e "${RED}❌ Documentation tests failed.${NC}"
    exit 1
fi
echo ""

# All checks passed
echo ""
echo -e "${GREEN}🎉 ALL PRE-COMMIT CHECKS PASSED!${NC}"
echo -e "${GREEN}✅ Code is ready for commit${NC}"
echo ""
echo -e "${BLUE}📊 Summary:${NC}"
echo "   • Code formatting: ✅"
echo "   • Compilation: ✅"
echo "   • Clippy linting: ✅"
echo "   • Test registration: ✅"
echo "   • Unit tests: ✅"
echo "   • Examples: ✅"
echo "   • Binaries: ✅"
echo "   • Comprehensive tests: ✅"
echo "   • Documentation tests: ✅"
echo ""

# Git commit section
echo -e "${YELLOW}📝 Preparing to commit...${NC}"
echo ""

# Check if there are changes to commit
if git diff --quiet && git diff --cached --quiet; then
    echo -e "${YELLOW}⚠️  No changes to commit${NC}"
    exit 0
fi

# Show status
echo -e "${BLUE}Current git status:${NC}"
git status --short
echo ""

# Prompt for commit message
echo -e "${YELLOW}Enter commit message (or press Ctrl+C to cancel):${NC}"
read -p "Message: " commit_message

if [ -z "$commit_message" ]; then
    echo -e "${RED}❌ Commit message cannot be empty${NC}"
    exit 1
fi

# Add all changes
echo ""
echo -e "${BLUE}Adding all changes...${NC}"
git add .

# Create commit with Claude Code signature
echo -e "${BLUE}Creating commit...${NC}"
git commit -m "$commit_message

echo ""
echo -e "${GREEN}✅ Successfully committed!${NC}"
echo ""
echo -e "${BLUE}Recent commits:${NC}"
git log --oneline -3
echo ""
echo -e "${YELLOW}💡 To push to remote, run: git push${NC}"
