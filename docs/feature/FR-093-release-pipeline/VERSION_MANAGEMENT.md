# Version Management Guide

## 🔒 Version Validation Enforced

**CRITICAL**: The release workflow **validates and enforces** version matching. If Cargo.toml version doesn't match the git tag, the build will **FAIL** with a clear error message.

This prevents user confusion where someone downloads `v0.1.0-beta.2` but `--version` shows something different.

---

## Two Version Systems Explained

Velostream has **two different version numbers** that **MUST be kept in sync**:

### 1. Git Tag Version (Release Workflow)

**Location**: Git tags (not in a file!)
**Purpose**: Triggers release workflow, determines release version
**How to set**: `git tag` command

```bash
# The tag name IS the version
git tag v0.1.0-beta.1  # ← This is the version number!
git push origin v0.1.0-beta.1
```

**This is what matters for releases!**

### 2. Cargo.toml Version (Rust Package)

**Location**: `Cargo.toml` file, line 3
**Purpose**: Rust package version, embedded in binaries
**How to set**: Edit the file

```toml
[package]
name = "velostream"
version = "0.1.0"  # ← Edit this line
edition = "2024"
```

**This is embedded in `--version` output**

---

## Quick Answer

### For Release Workflow Testing

**You DON'T edit a file!** Just create a new tag:

```bash
# First test
git tag v0.1.0-beta.1 -m "Test release"
git push origin v0.1.0-beta.1

# Need to iterate? Just increment the tag number
git tag v0.1.0-beta.2 -m "Test release: Fixed issue"
git push origin v0.1.0-beta.2

# Another iteration
git tag v0.1.0-beta.3 -m "Test release: Another fix"
git push origin v0.1.0-beta.3
```

**The tag name becomes the release version automatically!**

### For Production Releases

**Recommended**: Keep Cargo.toml in sync with git tag

```bash
# 1. Update Cargo.toml
vim Cargo.toml
# Change: version = "0.1.0" → version = "1.0.0"

# 2. Commit the version bump
git add Cargo.toml
git commit -m "chore: bump version to v1.0.0"
git push

# 3. Create matching git tag
git tag v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

But for **testing**, you can skip step 1-2 and just create tags!

---

## Detailed Explanation

### How Git Tags Work as Versions

When you push a tag matching `v*.*.*`, the workflow uses the tag name as the version:

**Example**:
```bash
git tag v0.1.0-beta.1
git push origin v0.1.0-beta.1
```

**Result**:
- Release title: `v0.1.0-beta.1`
- Archive names: `velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz`
- Download URL: `.../releases/download/v0.1.0-beta.1/...`

**The tag name IS the version!** No file editing needed.

### How Cargo.toml Version Works

**Current state** (Cargo.toml line 3):
```toml
version = "0.1.0"
```

**This version is embedded in binaries**:
```bash
./velo-sql --version
# Output: Velostream 0.1.0
```

**How it's embedded** (automatically by Cargo):
```rust
// In your code (already exists somewhere)
const VERSION: &str = env!("CARGO_PKG_VERSION");  // ← Reads from Cargo.toml
```

---

## Version Synchronization

### Should They Match?

**For ALL releases (including beta testing)**: YES - Always keep them synchronized

| Scenario | Cargo.toml | Git Tag | Sync? |
|----------|-----------|---------|-------|
| Beta release 1 | `0.1.0-beta.1` | `v0.1.0-beta.1` | ✅ Must match |
| Beta release 2 | `0.1.0-beta.2` | `v0.1.0-beta.2` | ✅ Must match |
| Production release | `1.0.0` | `v1.0.0` | ✅ Must match |

**Why they should ALWAYS match**:
- ✅ Avoids user confusion when running `--version`
- ✅ Professional appearance (versions consistent everywhere)
- ✅ Clear traceability between binary and release
- ✅ No surprises: what users download matches what they run
- ⚠️ Mismatch creates confusion: "I downloaded beta.2 but binary says beta.1?"

**User Feedback**: Even for testing, mismatched versions cause confusion. Always update both.

---

## Workflow Examples

### Example 1: Beta Testing (CORRECT - Update Both)

**Goal**: Release v0.1.0-beta.1 with matching versions everywhere

```bash
# Step 1: Update Cargo.toml
vim Cargo.toml
# Change line 3: version = "0.1.0" → version = "0.1.0-beta.1"

# Step 2: Commit version change
git add Cargo.toml
git commit -m "chore: bump version to v0.1.0-beta.1"
git push

# Step 3: Create matching tag
git tag v0.1.0-beta.1 -m "Beta release 1: Initial testing"
git push origin v0.1.0-beta.1

# Result:
# - Release: v0.1.0-beta.1
# - Binary --version: Velostream 0.1.0-beta.1  ✅ Matches!
# - No user confusion ✅
```

**For next beta**: Repeat the process with beta.2

```bash
# Update Cargo.toml to "0.1.0-beta.2"
# Commit the change
# Tag as v0.1.0-beta.2
```

**Result**: Clear, consistent versioning throughout the release cycle.

---

### Example 2: Production Release (Same Process)

**Goal**: Release v1.0.0 with everything in sync (same as beta!)

```bash
# Step 1: Update Cargo.toml
vim Cargo.toml
# Change line 3: version = "0.1.0" → version = "1.0.0"

# Step 2: Commit version bump
git add Cargo.toml
git commit -m "chore: bump version to v1.0.0

Prepare for production release"
git push origin feat/release-pipeline

# Step 3: Create git tag
git tag v1.0.0 -m "Release v1.0.0: Multi-platform binary builds

Features:
- 5-platform builds (Linux x86/ARM, macOS Intel/ARM, Windows)
- Demo packaging with out-of-box functionality
- SHA256 checksums for verification
- Complete documentation"

# Step 4: Push tag (triggers workflow)
git push origin v1.0.0

# Result:
# - Release: v1.0.0
# - Binary --version: Velostream 1.0.0  ✅ Matches!
```

---

## Where Files Are Located

### Cargo.toml (Rust Package Version)

**Path**: `/Users/navery/RustroverProjects/velostream-release/Cargo.toml`

**Current content** (lines 1-4):
```toml
[package]
name = "velostream"
version = "0.1.0"  # ← Edit this line for version
edition = "2024"
```

**How to update**:
```bash
# Option 1: Edit manually
vim Cargo.toml
# Change version = "0.1.0" to desired version

# Option 2: Use sed
sed -i '' 's/version = "0.1.0"/version = "1.0.0"/' Cargo.toml

# Option 3: Use script (future enhancement)
./scripts/bump-version.sh 1.0.0
```

### Git Tags (Release Version)

**Location**: Git metadata (not a file!)

**View existing tags**:
```bash
git tag --list
# Output:
# v0.1.0-beta.1
# v0.1.0-beta.2
# v1.0.0
```

**View remote tags**:
```bash
git ls-remote --tags origin
```

**View tag details**:
```bash
git show v0.1.0-beta.1
# Shows: tag message, commit, diff
```

---

## Common Workflows

### The ONLY Recommended Workflow: Update Both (Beta AND Production)

**Use case**: ALL releases - beta testing and production

```bash
# 1. Update Cargo.toml version
echo 'version = "1.0.0"' # Edit line 3 of Cargo.toml

# 2. Commit version bump
git add Cargo.toml
git commit -m "chore: bump version to v1.0.0"
git push

# 3. Create matching tag
git tag v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

**Benefits**:
- ✅ Professional appearance (versions match everywhere)
- ✅ Binary `--version` shows correct version
- ✅ Clear version history with audit trail
- ✅ No user confusion
- ✅ Traceability between downloads and binaries

**Note**: The extra commit per version is a FEATURE, not a bug - it provides clear version history and is standard practice for professional releases.

---

## Recommendation for Velostream

### For ALL Releases (Beta AND Production)

**ALWAYS update Cargo.toml to match git tag** - even for beta testing:

```bash
# For EVERY release (beta or production):

# 1. Edit Cargo.toml to match desired version
vim Cargo.toml
# Change: version = "X.Y.Z" to match tag

# 2. Commit the version change
git add Cargo.toml
git commit -m "chore: bump version to vX.Y.Z"
git push

# 3. Create matching git tag
git tag vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

**Examples**:
- Beta 1: Cargo.toml = "0.1.0-beta.1", Tag = v0.1.0-beta.1
- Beta 2: Cargo.toml = "0.1.0-beta.2", Tag = v0.1.0-beta.2
- Release: Cargo.toml = "1.0.0", Tag = v1.0.0

**Why**: Prevents user confusion. When someone downloads v0.1.0-beta.2, they expect `--version` to show beta.2, not beta.1 or some other version.

---

## How Release Workflow Uses Version

### From release.yml

**Tag name is used everywhere**:

```yaml
# Archive names
velostream-${{ github.ref_name }}-x86_64-unknown-linux-musl.tar.gz
# If tag is v0.1.0-beta.1, archive is:
# velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz

# Release title
Release ${{ github.ref_name }}
# If tag is v1.0.0, release title is:
# Release v1.0.0

# Pre-release detection
prerelease: ${{ contains(github.ref_name, 'beta') }}
# If tag contains 'beta', marked as pre-release
```

**Everything uses the tag name!**

---

## FAQ

### Q: Do I need to edit any file to change the release version?

**A**: NO! Just create a new git tag with the version you want:
```bash
git tag v0.1.0-beta.2  # ← This IS the version
git push origin v0.1.0-beta.2
```

### Q: What happens if Cargo.toml version doesn't match git tag?

**A**: The release workflow will **FAIL** with a clear error message.

**Workflow validation** (added to release.yml):
- ✅ Extracts version from Cargo.toml (line 3)
- ✅ Extracts version from git tag (removes 'v' prefix)
- ✅ Compares them
- ❌ **FAILS BUILD** if they don't match

**Error message shown**:
```
❌ ERROR: Version mismatch detected!

  Cargo.toml version: 0.1.0
  Git tag version:    0.1.0-beta.2

These MUST match to avoid user confusion.

Fix this by updating Cargo.toml:
  1. Edit Cargo.toml line 3: version = "0.1.0-beta.2"
  2. git add Cargo.toml
  3. git commit -m 'chore: bump version to 0.1.0-beta.2'
  ...
```

**Result**: Versions are **locked** - workflow enforces matching to prevent confusion.

### Q: How do I see what version will be released?

**A**: Look at the tag name you're about to push:
```bash
git tag v0.1.0-beta.1  # ← This will be the release version
git push origin v0.1.0-beta.1
```

### Q: Can I change the version after pushing the tag?

**A**: Not easily. Better to:
1. Delete tag and release (messy)
2. Or just push a new tag with different version (clean)

**Recommended**: Just use a new version number.

### Q: Where is the version stored?

**A**:
- **Git tag**: In git metadata (`.git/` folder, not a regular file)
- **Cargo.toml**: In `Cargo.toml` file, line 3

### Q: How do I update Cargo.toml version?

**A**: Edit line 3:
```bash
vim Cargo.toml
# Or
sed -i '' 's/version = "0.1.0"/version = "1.0.0"/' Cargo.toml
```

---

## Visual Guide

### Current State

```
Cargo.toml:
[package]
version = "0.1.0"  # ← Current package version

Git tags:
(none yet)  # ← No tags pushed yet
```

### After First Test

```bash
git tag v0.1.0-beta.1
git push origin v0.1.0-beta.1
```

**Result**:
```
Cargo.toml:
version = "0.1.0"  # ← Unchanged

Git tags:
v0.1.0-beta.1  # ← New tag

GitHub Release:
v0.1.0-beta.1  # ← Created automatically

Archives:
velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz  # ← From tag name

Binary --version:
Velostream 0.1.0  # ← From Cargo.toml
```

### After Production Release

```bash
# 1. Update Cargo.toml to 1.0.0
# 2. Commit
# 3. Tag v1.0.0
```

**Result**:
```
Cargo.toml:
version = "1.0.0"  # ← Updated

Git tags:
v0.1.0-beta.1
v0.1.0-beta.2
v1.0.0  # ← Production release

GitHub Release:
v1.0.0  # ← Latest

Binary --version:
Velostream 1.0.0  # ← Matches tag ✅
```

---

## Summary

### For ALL Releases (Beta and Production)

**ALWAYS update Cargo.toml to match git tag:**

```bash
# Step 1: Edit Cargo.toml to match desired version
vim Cargo.toml
# Change version = "X.Y.Z" to match tag

# Step 2: Commit the change
git add Cargo.toml
git commit -m "chore: bump version to vX.Y.Z"
git push

# Step 3: Create matching git tag
git tag vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

**Examples**:
- Beta 1: Update to "0.1.0-beta.1", then tag v0.1.0-beta.1
- Beta 2: Update to "0.1.0-beta.2", then tag v0.1.0-beta.2
- Release: Update to "1.0.0", then tag v1.0.0

### Key Takeaways

1. **Git tag name IS the release version** (used in archive names, release title)
2. **Cargo.toml version IS the binary version** (shown in `--version`)
3. **They should ALWAYS match** to avoid user confusion
4. **Update both for EVERY release** - beta testing included

**Why this matters**: When users download v0.1.0-beta.2 and run `--version`, they expect to see "0.1.0-beta.2", not something else. Mismatched versions create confusion and look unprofessional.

---

**Document Status**: Complete (Corrected)
**Recommendation**: Update Cargo.toml + git tag for ALL releases (beta and production)
**User Feedback Applied**: "It will be confusing if they do not match" ✅
