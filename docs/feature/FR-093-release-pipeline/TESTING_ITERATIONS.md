# Release Workflow Testing & Iteration Guide

## Question: Can I Re-run the Same Tag Multiple Times?

**Short answer**: Not easily - but you have better options!

---

## The Tag/Release Lifecycle

### What Happens When You Push a Tag

```bash
git tag v0.1.0-beta.1
git push origin v0.1.0-beta.1
```

**GitHub's behavior**:
1. ✅ Workflow triggers immediately (if tag matches `v*.*.*`)
2. ✅ Builds run in parallel
3. ✅ GitHub Release created with tag `v0.1.0-beta.1`
4. ✅ Binaries uploaded to that release

**The problem**:
- Tag `v0.1.0-beta.1` now exists on GitHub
- Release for `v0.1.0-beta.1` exists
- **Pushing same tag again won't trigger workflow** (tag already exists)
- **Workflow will fail if release already exists** (can't create duplicate)

---

## Re-Running Strategies

### Strategy 1: Delete and Re-Push (MESSY) ❌

**If you need to re-test the SAME version**:

```bash
# 1. Delete remote tag
git push origin --delete v0.1.0-beta.1

# 2. Delete local tag
git tag -d v0.1.0-beta.1

# 3. Delete GitHub Release manually
# Go to: https://github.com/velostreamai/velostream/releases
# Find release, click "Delete"

# 4. Re-create and push tag
git tag v0.1.0-beta.1 -m "Test release (retry)"
git push origin v0.1.0-beta.1
```

**Pros**: Same version number
**Cons**: ⚠️ Messy, manual steps, can confuse users who already downloaded

**When to use**: Only if you MUST keep the same version (rarely)

---

### Strategy 2: Increment Version (RECOMMENDED) ✅

**For iterative testing, use sequential versions**:

```bash
# First attempt
git tag v0.1.0-beta.1 -m "Test release: First attempt"
git push origin v0.1.0-beta.1

# Found an issue? Use beta.2
git tag v0.1.0-beta.2 -m "Test release: Fix issue X"
git push origin v0.1.0-beta.2

# Another issue? Use beta.3
git tag v0.1.0-beta.3 -m "Test release: Fix issue Y"
git push origin v0.1.0-beta.3

# All good? Release candidate
git tag v0.1.0-rc.1 -m "Release candidate 1"
git push origin v0.1.0-rc.1

# Final release
git tag v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

**Pros**:
- ✅ Clean history
- ✅ Each iteration documented
- ✅ No manual deletion
- ✅ Can compare versions
- ✅ Users can see progression

**Cons**: None really

**When to use**: **Always** for testing iterations

---

### Strategy 3: Re-run Workflow (LIMITED) ⏯️

**GitHub Actions allows re-running workflows**:

```bash
# Via GitHub UI
# Go to: https://github.com/velostreamai/velostream/actions
# Click on the workflow run
# Click "Re-run all jobs"

# Or via gh CLI
gh run list --workflow=release.yml
gh run rerun <run-id>
```

**Limitations**:
- ⚠️ Only works if workflow **failed**
- ⚠️ Won't work if release already exists
- ⚠️ Doesn't help if you changed code (still uses old commit)

**When to use**: Only if workflow failed due to transient error (network, runner issue)

---

## Recommended Testing Flow

### Phase 1: Initial Testing

**Goal**: Verify workflow executes successfully

```bash
# Day 1: First test
git tag v0.1.0-beta.1 -m "Test release: Multi-platform builds (first run)"
git push origin v0.1.0-beta.1

# Monitor: https://github.com/velostreamai/velostream/actions
# Expected: ~25-30 min for all builds
```

**What to check**:
- [ ] All 5 build jobs start
- [ ] Builds complete successfully
- [ ] Release created
- [ ] All 10 assets uploaded (5 archives + 5 checksums)

---

### Phase 2: Fix Issues (If Any)

**If builds fail or have issues**:

```bash
# Fix the issue in code
# Commit the fix
git add .
git commit -m "fix: resolve issue X in release workflow"
git push origin feat/release-pipeline

# Create new beta version
git tag v0.1.0-beta.2 -m "Test release: Fixed issue X"
git push origin v0.1.0-beta.2
```

**Benefits**:
- ✅ Clean version history
- ✅ Can compare v0.1.0-beta.1 vs v0.1.0-beta.2
- ✅ Clear documentation of what changed

---

### Phase 3: Platform Testing

**After successful builds, test binaries**:

```bash
# Download beta.1 (or latest beta)
wget https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz

# Test...
# Find issue? Document and fix

# Create new beta with fix
git tag v0.1.0-beta.3 -m "Test release: Fixed binary issue Y"
git push origin v0.1.0-beta.3
```

---

### Phase 4: Release Candidate

**When beta testing is successful**:

```bash
git tag v0.1.0-rc.1 -m "Release candidate 1"
git push origin v0.1.0-rc.1

# Final testing
# If all good, proceed to production
```

---

### Phase 5: Production Release

**Final release**:

```bash
git tag v1.0.0 -m "Release v1.0.0: Multi-platform binary builds

Features:
- 5-platform builds (Linux x86/ARM, macOS Intel/ARM, Windows)
- Demo packaging with out-of-box functionality
- SHA256 checksums for verification
- Complete documentation"

git push origin v1.0.0
```

---

## Version Naming Conventions

### Pre-Release Versions (Testing)

| Format | Use Case | Example |
|--------|----------|---------|
| `v0.1.0-alpha.N` | Early testing, unstable | v0.1.0-alpha.1 |
| `v0.1.0-beta.N` | Feature complete, testing | v0.1.0-beta.1 |
| `v0.1.0-rc.N` | Release candidate, final testing | v0.1.0-rc.1 |

**Pattern**: Increment N for each iteration (beta.1 → beta.2 → beta.3)

### Production Versions

| Format | Use Case | Example |
|--------|----------|---------|
| `vMAJOR.MINOR.PATCH` | Production release | v1.0.0 |
| `vMAJOR.MINOR.PATCH-hotfix.N` | Urgent fix | v1.0.1-hotfix.1 |

---

## Deleting Tags/Releases (Emergency)

### When You MUST Delete

**Only do this if**:
- ⚠️ Accidentally tagged wrong commit
- ⚠️ Critical security issue in release
- ⚠️ Legal/compliance issue

**Never do this if**:
- ❌ Just want to re-test (use new version instead)
- ❌ Found a bug (fix in next version)
- ❌ Workflow failed (fix and use new version)

### How to Delete Completely

```bash
# 1. Delete GitHub Release (manually)
# Go to: https://github.com/velostreamai/velostream/releases
# Click release → "Delete release" button

# 2. Delete remote tag
git push origin --delete v0.1.0-beta.1

# 3. Delete local tag
git tag -d v0.1.0-beta.1

# 4. Verify deletion
git ls-remote --tags origin  # Should not show deleted tag
```

---

## Pre-Release Flags

### What is a Pre-Release?

GitHub automatically marks releases as "pre-release" if version contains:
- `alpha`
- `beta`
- `rc`

**Benefits**:
- ⚠️ Shown with "Pre-release" badge
- ⚠️ Not marked as "Latest"
- ⚠️ Users know it's for testing

**Our workflow** (from release.yml):
```yaml
prerelease: ${{ contains(github.ref_name, 'alpha') || contains(github.ref_name, 'beta') || contains(github.ref_name, 'rc') }}
```

**Examples**:
- `v0.1.0-beta.1` → Pre-release ✅
- `v0.1.0-rc.1` → Pre-release ✅
- `v1.0.0` → Production release ✅

---

## Testing Iteration Examples

### Example 1: Workflow Fix

```bash
# Test 1: First run fails (YAML error)
git tag v0.1.0-beta.1
git push origin v0.1.0-beta.1
# → Fails after 2 minutes (YAML syntax error)

# Fix YAML
git add .github/workflows/release.yml
git commit -m "fix: correct YAML syntax in release workflow"
git push

# Test 2: New version
git tag v0.1.0-beta.2 -m "Test release: Fixed YAML syntax"
git push origin v0.1.0-beta.2
# → Succeeds! All builds complete
```

### Example 2: Binary Issue

```bash
# Test 1: Builds succeed, but binary doesn't run on Linux
git tag v0.1.0-beta.1
git push origin v0.1.0-beta.1
# → Builds complete, download and test
# → Find issue: missing shared library

# Fix linking issue
git add .cargo/config.toml
git commit -m "fix: use musl static linking for Linux"
git push

# Test 2: New version
git tag v0.1.0-beta.2 -m "Test release: Fixed Linux static linking"
git push origin v0.1.0-beta.2
# → Download and test
# → Works! Binary runs on Alpine/Ubuntu/CentOS
```

### Example 3: Demo Packaging

```bash
# Test 1: Binaries work, but demos don't
git tag v0.1.0-beta.1
git push origin v0.1.0-beta.1
# → Extract archive, try to run demo
# → Demo script fails: ./target/release/velo-1brc not found

# Fix demo packaging
git add .github/workflows/release.yml
git commit -m "fix: add target/release symlinks for demo compatibility"
git push

# Test 2: New version
git tag v0.1.0-beta.3 -m "Test release: Fixed demo packaging"
git push origin v0.1.0-beta.3
# → Extract archive, try demo
# → Works! Demo runs out-of-box
```

---

## Workflow Behavior Summary

### Tag Already Exists

**Scenario**: Push tag that already exists on GitHub

```bash
git push origin v0.1.0-beta.1  # Tag already exists
```

**Result**: ❌ Git rejects the push
```
! [rejected]        v0.1.0-beta.1 -> v0.1.0-beta.1 (already exists)
error: failed to push some refs to 'github.com:velostreamai/velostream.git'
```

**Action**: Use new version or delete old tag first

---

### Release Already Exists

**Scenario**: Workflow runs, but release already exists for tag

**Result**: ❌ Workflow fails at "Create GitHub Release" step
```
Error: Release for tag v0.1.0-beta.1 already exists
```

**Action**: Delete release first or use new version

---

### Code Changed Since Tag

**Scenario**: You tagged commit A, then committed fix B, want to re-release with same version

**Problem**: Tag still points to commit A (old code)

**Solution**:
```bash
# Option 1: Delete and re-create tag (MESSY)
git tag -d v0.1.0-beta.1
git tag v0.1.0-beta.1 -m "Updated"
git push origin v0.1.0-beta.1 --force

# Option 2: Use new version (CLEAN)
git tag v0.1.0-beta.2 -m "Updated with fix"
git push origin v0.1.0-beta.2
```

**Recommendation**: Always use new version (Option 2)

---

## Best Practices

### ✅ DO

1. **Increment versions for iterations**
   - beta.1 → beta.2 → beta.3
   - Clear progression, clean history

2. **Use pre-release versions for testing**
   - Start with `v0.1.0-beta.1`
   - Iterate until stable
   - Then `v1.0.0` for production

3. **Document what changed**
   - Tag message explains what was fixed
   - Easy to track testing progress

4. **Keep failed releases**
   - Don't delete unless critical issue
   - Useful for debugging and comparison

5. **Use gh CLI for monitoring**
   ```bash
   gh run list --workflow=release.yml
   gh run watch
   ```

### ❌ DON'T

1. **Don't delete and recreate same tag repeatedly**
   - Confusing for users
   - Breaks download links
   - Messy history

2. **Don't force-push tags**
   - Can break existing downloads
   - Dangerous (easy to make mistakes)

3. **Don't use production versions for testing**
   - Don't tag `v1.0.0` for testing
   - Use beta/rc versions instead

4. **Don't try to re-run successful workflows**
   - Won't help if release already exists
   - Use new version instead

---

## Quick Reference

### First Test

```bash
git tag v0.1.0-beta.1 -m "Test release: First run"
git push origin v0.1.0-beta.1
```

### Found Issue, Need to Re-Test

```bash
# Fix code
git add .
git commit -m "fix: issue description"
git push

# New version
git tag v0.1.0-beta.2 -m "Test release: Fixed issue X"
git push origin v0.1.0-beta.2
```

### All Testing Complete

```bash
git tag v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

### Emergency: Delete Everything

```bash
# 1. Delete release (GitHub web UI)
# 2. Delete tags
git push origin --delete v0.1.0-beta.1
git tag -d v0.1.0-beta.1
```

---

## Conclusion

**Question**: Can I run the same action multiple times without changing version?

**Answer**:
- **Technically**: Yes, but requires manual deletion of tags/releases (messy)
- **Recommended**: No, increment version instead (clean)

**Best approach**:
1. Start with `v0.1.0-beta.1`
2. If issues found, fix and use `v0.1.0-beta.2`
3. Continue iterating (beta.3, beta.4, etc.)
4. Final testing with `v0.1.0-rc.1`
5. Production release `v1.0.0`

**Benefits**:
- ✅ Clean version history
- ✅ No manual deletion needed
- ✅ Easy to track progress
- ✅ Can compare versions
- ✅ Professional appearance

**Versioning is cheap** - use it generously during testing! 🎯

---

**Document Status**: Complete
**Recommended Strategy**: Increment versions (beta.1 → beta.2 → beta.3)
**Next Step**: Push v0.1.0-beta.1 and iterate as needed
