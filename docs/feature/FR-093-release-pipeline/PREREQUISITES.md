# FR-093 Release Pipeline - Prerequisites & Setup

## Phase 1: Binary Build Pipeline (CURRENT)

### ✅ Already Available (No Action Needed)

| Requirement | Status | Notes |
|-------------|--------|-------|
| **GitHub Repository** | ✅ Exists | velostreamai/velostream |
| **GitHub Actions** | ✅ Enabled | Free for public repos |
| **GitHub-hosted runners** | ✅ Available | ubuntu-latest, macos-13/14, windows-latest |
| **GitHub Releases** | ✅ Available | Default permission |
| **Repository write access** | ✅ Available | For creating releases |

**Verdict**: **NO additional accounts or setup needed for Phase 1!** 🎉

### ⏳ Actions Required Before First Release

#### 1. Verify GitHub Actions Permissions

**Action**: Check repository settings
**Command**:
```bash
# Visit: https://github.com/velostreamai/velostream/settings/actions
```

**Required settings**:
- ✅ Actions: "Allow all actions and reusable workflows" (or at least allow specific actions used)
- ✅ Workflow permissions: "Read and write permissions" (for creating releases)

**Expected**: Should already be enabled for public repos. Verify before first test release.

#### 2. Create Test Release Tag (Trigger Workflow)

**Action**: Push a test tag to trigger the workflow

**Commands**:
```bash
# From feat/release-pipeline branch
git tag v0.1.0-beta.1 -m "Test release: Multi-platform binary builds"
git push origin v0.1.0-beta.1
```

**What happens**:
1. GitHub Actions detects tag push matching `v*.*.*`
2. Triggers `.github/workflows/release.yml`
3. Builds binaries for 5 platforms in parallel
4. Creates GitHub Release with artifacts

**Monitoring**:
```bash
# View in browser
open https://github.com/velostreamai/velostream/actions

# Or use gh CLI
gh run list --workflow=release.yml
gh run watch  # Follow latest run
```

#### 3. Download and Test Binaries (Post-Build)

**Action**: Verify binaries work on target platforms

**Commands**:
```bash
# Example: Linux x86_64
wget https://github.com/velostreamai/velostream/releases/download/v0.1.0-beta.1/velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz
sha256sum -c velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz.sha256
tar xzf velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl.tar.gz
cd velostream-v0.1.0-beta.1-x86_64-unknown-linux-musl
./bin/velo-sql --version
cd demo/1brc && ./run-1brc.sh
```

**Platforms to test**:
- [ ] Linux x86_64 (Ubuntu, CentOS, Alpine)
- [ ] Linux ARM64 (Raspberry Pi, AWS Graviton)
- [ ] macOS Intel (x86_64)
- [ ] macOS Apple Silicon (ARM64)
- [ ] Windows x86_64 (Windows 10/11)

See `TESTING_CHECKLIST.md` for complete testing guide.

---

## Phase 2: Docker Image Pipeline (FUTURE)

### ❌ Not Yet Required (Phase 2)

Phase 2 will require Docker registry accounts, but **these are NOT needed for Phase 1**.

#### Docker Hub Account (Phase 2)

**When needed**: Phase 2 implementation
**Purpose**: Publish multi-architecture Docker images

**Setup steps (FUTURE)**:
1. Create Docker Hub account: https://hub.docker.com/signup
2. Create organization or use personal namespace
3. Create access token: Account Settings → Security → New Access Token
4. Add to GitHub Secrets: `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`

**Repository**: `velostream/velostream` or `<username>/velostream`

**Images planned**:
- `velostream:latest` - Latest stable release
- `velostream:v1.0.0` - Specific version
- `velostream:v1.0.0-alpine` - Minimal Alpine variant
- `velostream:dev` - Development builds

#### GitHub Container Registry (GHCR) - Alternative (Phase 2)

**When needed**: Phase 2 implementation (optional alternative to Docker Hub)
**Purpose**: Publish images to GitHub Packages

**Advantages**:
- ✅ No additional account needed (uses GitHub)
- ✅ Tightly integrated with repository
- ✅ Automatic cleanup policies
- ✅ Better rate limits for authenticated users

**Setup steps (FUTURE)**:
1. Create Personal Access Token: Settings → Developer settings → Personal access tokens
2. Scope required: `write:packages`, `delete:packages`
3. Add to GitHub Secrets: `GHCR_TOKEN`

**Repository**: `ghcr.io/velostreamai/velostream`

**Recommendation**: Use GHCR for Phase 2 (simpler, no external account needed)

---

## Outstanding Actions Summary

### Required NOW (Phase 1 Testing)

| Action | Status | Owner | Timeline |
|--------|--------|-------|----------|
| Verify GitHub Actions permissions | ⏳ Todo | Maintainer | 5 min |
| Push test tag v0.1.0-beta.1 | ⏳ Todo | Maintainer | 1 min |
| Monitor workflow execution | ⏳ Todo | Maintainer | 30 min |
| Download test binaries | ⏳ Todo | Maintainer | 5 min |
| Test on Linux x86_64 | ⏳ Todo | Team | 30 min |
| Test on macOS (optional) | ⏳ Todo | Team | 30 min |
| Test on Windows (optional) | ⏳ Todo | Team | 30 min |
| Document any issues | ⏳ Todo | Team | As needed |

**Total time**: 1-2 hours (if no issues)

### Optional NOW (Nice to Have)

| Action | Status | Owner | Timeline |
|--------|--------|-------|----------|
| Test on Linux ARM64 | ⏳ Optional | Team | 30 min |
| Test all demos (1brc, trading) | ⏳ Optional | Team | 1 hour |
| Verify binary sizes | ⏳ Optional | Team | 5 min |
| Test on multiple Linux distros | ⏳ Optional | Team | 1 hour |

### Required LATER (Phase 2)

| Action | Status | Owner | Timeline |
|--------|--------|-------|----------|
| Create Docker Hub account | 📅 Phase 2 | Maintainer | 10 min |
| Set up Docker Hub org | 📅 Phase 2 | Maintainer | 5 min |
| Generate Docker Hub token | 📅 Phase 2 | Maintainer | 2 min |
| Add GitHub Secrets | 📅 Phase 2 | Maintainer | 5 min |
| Implement Docker workflow | 📅 Phase 2 | Developer | 3-4 hours |
| Test Docker images | 📅 Phase 2 | Team | 1 hour |

---

## GitHub Secrets Configuration

### Phase 1: NO SECRETS NEEDED ✅

The current implementation uses only built-in GitHub capabilities:
- ✅ `GITHUB_TOKEN` - Automatically provided by GitHub Actions
- ✅ No external services
- ✅ No credentials required

**Security**: Minimal attack surface, no secret management needed.

### Phase 2: Docker Registry Secrets

**When implementing Phase 2**, add these secrets:

#### For Docker Hub:
```bash
# Navigate to: https://github.com/velostreamai/velostream/settings/secrets/actions

Secret name: DOCKERHUB_USERNAME
Secret value: <your-dockerhub-username>

Secret name: DOCKERHUB_TOKEN
Secret value: <your-dockerhub-access-token>
```

#### For GHCR (Alternative):
```bash
Secret name: GHCR_TOKEN
Secret value: <github-personal-access-token>
```

**Workflow usage**:
```yaml
- name: Login to Docker Hub
  uses: docker/login-action@v3
  with:
    username: ${{ secrets.DOCKERHUB_USERNAME }}
    password: ${{ secrets.DOCKERHUB_TOKEN }}
```

---

## Repository Settings Checklist

### Current Settings (Verify)

**Actions** (`Settings → Actions → General`):
- [ ] Actions permissions: "Allow all actions and reusable workflows"
  - Or: "Allow select actions" → Allow `actions/*`, `docker/*`, `softprops/*`
- [ ] Workflow permissions: "Read and write permissions"
- [ ] Allow GitHub Actions to create pull requests: ✅ (optional)

**Security** (`Settings → Security`):
- [ ] Dependabot alerts: ✅ Enabled (recommended)
- [ ] Dependabot security updates: ✅ Enabled (recommended)

**Releases** (Default, no config needed):
- ✅ Anyone with write access can create releases
- ✅ Releases are public for public repos

### Optional Settings (Recommended)

**Branch Protection** (`Settings → Branches`):
- Consider protecting `master` branch
- Require status checks (CI) before merge
- Don't block release workflow (releases use tags, not branches)

**Pages** (Optional):
- Could host documentation
- Not required for release pipeline

---

## Testing Prerequisites

### Tools Needed on Test Machines

**Linux**:
```bash
# Required
wget or curl       # Download binaries
tar                # Extract archives
sha256sum          # Verify checksums

# Optional
docker            # For trading demo (Kafka)
```

**macOS**:
```bash
# Required
curl              # Download binaries
tar               # Extract archives
shasum            # Verify checksums (built-in)

# Optional
docker            # For trading demo (Kafka)
brew              # For installing dependencies
```

**Windows**:
```powershell
# Required (built-in)
Invoke-WebRequest  # Download binaries
Expand-Archive     # Extract archives
Get-FileHash       # Verify checksums

# Optional
docker             # For trading demo (Kafka)
```

### Platform Access

**Minimum required**:
- [ ] Linux x86_64 machine (VM, container, or bare metal)

**Recommended**:
- [ ] Linux x86_64 (Ubuntu 22.04 LTS) - Primary platform
- [ ] macOS (Intel or Apple Silicon) - Secondary platform
- [ ] Windows 10/11 - Tertiary platform

**Optional**:
- [ ] Linux ARM64 (Raspberry Pi, AWS Graviton)
- [ ] Multiple Linux distros (CentOS, Alpine, Debian)

**Cloud options for testing**:
- AWS EC2 free tier (Linux x86_64)
- AWS Graviton instances (Linux ARM64)
- GitHub Codespaces (Linux x86_64)
- Local VMs (VirtualBox, VMware)

---

## Workflow Permissions Deep Dive

### What the Release Workflow Needs

**Permissions used** (from `.github/workflows/release.yml`):
```yaml
permissions:
  contents: write  # Create releases and upload assets
```

**Why this is safe**:
- ✅ Only triggered on tag push (explicit action)
- ✅ Only writes to Releases (doesn't modify code)
- ✅ No access to secrets (none configured)
- ✅ No access to pull requests
- ✅ No access to issues

**What it does**:
1. Reads repository code (public)
2. Builds binaries (on GitHub runners)
3. Creates GitHub Release (with `contents: write`)
4. Uploads binary archives (part of release creation)

**What it does NOT do**:
- ❌ Modify code or branches
- ❌ Push commits
- ❌ Access external services
- ❌ Require authentication to external systems

---

## First Run Checklist

Before pushing the first test tag, verify:

### Pre-Flight Checks

- [ ] Code is on `feat/release-pipeline` branch
- [ ] All files committed: `.github/workflows/release.yml`, etc.
- [ ] Code compiles: `cargo check --all-targets --no-default-features`
- [ ] YAML is valid: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"`
- [ ] GitHub Actions enabled in repository settings
- [ ] Workflow permissions set to "Read and write"

### Tag and Push

```bash
# 1. Ensure you're on the right branch
git checkout feat/release-pipeline
git status  # Verify clean state

# 2. Create test tag (annotated)
git tag -a v0.1.0-beta.1 -m "Test release: Multi-platform binary builds

Features:
- 5-platform matrix builds (Linux x86/ARM, macOS Intel/ARM, Windows)
- Demo packaging with target/release/ symlinks
- Auto-generated release notes
- SHA256 checksums

Testing: First workflow execution"

# 3. Push tag to trigger workflow
git push origin v0.1.0-beta.1

# 4. Monitor workflow
open https://github.com/velostreamai/velostream/actions
# or
gh run watch
```

### Post-Push Monitoring

**What to watch**:
1. Workflow starts (within 10 seconds of tag push)
2. Build jobs start (5 parallel jobs in matrix)
3. Each job completes (15-30 min per platform)
4. Release job creates GitHub Release
5. Assets uploaded (10 files: 5 archives + 5 checksums)

**Common issues**:
- **Workflow doesn't start**: Check Actions are enabled
- **Permission denied**: Check workflow permissions
- **Build fails**: Check logs, may need to adjust workflow
- **Release creation fails**: Check `contents: write` permission

---

## Quick Reference

### No Action Needed (Already Available)

✅ GitHub repository
✅ GitHub Actions
✅ GitHub Releases
✅ GitHub-hosted runners (Linux, macOS, Windows)
✅ Workflow implementation complete

### Action Needed NOW (Phase 1)

1. ⏳ Push test tag: `v0.1.0-beta.1`
2. ⏳ Monitor workflow execution
3. ⏳ Test downloaded binaries

### Action Needed LATER (Phase 2)

1. 📅 Create Docker Hub account (or use GHCR)
2. 📅 Configure Docker credentials in GitHub Secrets
3. 📅 Implement Docker workflow

### Timeline

- **Phase 1 Testing**: 1-2 hours (this week)
- **Phase 1 Production**: 2-3 days (next week)
- **Phase 2 Planning**: TBD (after Phase 1 validated)

---

## Conclusion

**Good news**: **NO additional accounts or external services needed for Phase 1!** 🎉

Everything required is already available:
- ✅ GitHub repository
- ✅ GitHub Actions (free for public repos)
- ✅ GitHub Releases (default feature)

**Next action**: Simply push a test tag to trigger the workflow:
```bash
git tag v0.1.0-beta.1 -m "Test release: Multi-platform builds"
git push origin v0.1.0-beta.1
```

Then monitor, test, and iterate based on results.

**Phase 2 (Docker)** will require Docker Hub or GHCR setup, but that's **future work** after Phase 1 is validated.

---

**Document Status**: Complete
**Last Updated**: 2024-02-11
**Phase**: 1 - Binary Build Pipeline
**Required Actions**: Push test tag, monitor, test binaries
