# GHCR vs Docker Hub: Decision Analysis for Velostream

## Executive Summary

**Recommendation**: Use **GitHub Container Registry (GHCR)** for Velostream Phase 2.

**Confidence**: HIGH - GHCR is the obvious choice for GitHub-native projects.

**Rationale**:
- ✅ Zero external accounts/setup
- ✅ Better GitHub Actions integration
- ✅ No rate limit concerns
- ✅ Free unlimited public images
- ⚠️ Only downside: Slightly less discoverable than Docker Hub

**Can switch later**: Yes, can publish to both registries if needed.

---

## GHCR (GitHub Container Registry)

### What Is It?

GitHub's native container registry, launched in 2020, now generally available and recommended for GitHub-hosted projects.

**URL Pattern**: `ghcr.io/velostreamai/velostream:latest`

### Advantages ✅

| Feature | Benefit | Impact |
|---------|---------|--------|
| **No external account** | Uses GitHub account | ⭐⭐⭐ Huge |
| **Native integration** | GITHUB_TOKEN auto-available | ⭐⭐⭐ Huge |
| **Unlimited public images** | No storage/bandwidth limits | ⭐⭐⭐ Huge |
| **Better rate limits** | 500 pulls/hr (vs 100/6hr Docker Hub) | ⭐⭐ High |
| **Same namespace** | `ghcr.io/velostreamai/*` matches repo | ⭐⭐ High |
| **Auto-cleanup** | Built-in retention policies | ⭐ Medium |
| **No secrets needed** | GITHUB_TOKEN has permissions | ⭐⭐⭐ Huge |
| **Better security** | Fine-grained permissions | ⭐⭐ High |

### Disadvantages ⚠️

| Issue | Impact | Mitigation |
|-------|--------|------------|
| **Less discoverable** | People search Docker Hub first | ⭐ Low - Can document both |
| **Newer/less known** | Some users unfamiliar | ⭐ Low - Growing adoption |
| **Requires GitHub** | Private pulls need GitHub login | ⭐ Low - Public images don't |
| **Corporate blocklists** | Some orgs block ghcr.io | ⭐ Low - Rare for public images |

### Setup Complexity: ⭐ VERY SIMPLE

**Workflow configuration**:
```yaml
- name: Login to GitHub Container Registry
  uses: docker/login-action@v3
  with:
    registry: ghcr.io
    username: ${{ github.actor }}
    password: ${{ secrets.GITHUB_TOKEN }}  # ← Auto-available!

- name: Build and push
  uses: docker/build-push-action@v5
  with:
    push: true
    tags: |
      ghcr.io/velostreamai/velostream:latest
      ghcr.io/velostreamai/velostream:${{ github.ref_name }}
```

**GitHub Secrets needed**: ❌ NONE! (GITHUB_TOKEN is automatic)

**Total setup time**: **5 minutes** (just write the workflow)

---

## Docker Hub

### What Is It?

The original and most popular container registry, owned by Docker Inc. Industry standard since 2013.

**URL Pattern**: `docker.io/velostream/velostream:latest` (or just `velostream/velostream:latest`)

### Advantages ✅

| Feature | Benefit | Impact |
|---------|---------|--------|
| **Most popular** | First place people look | ⭐⭐⭐ Huge |
| **Best SEO** | Shows up in Google searches | ⭐⭐ High |
| **Industry standard** | Everyone knows it | ⭐⭐ High |
| **Corporate-friendly** | Often pre-whitelisted | ⭐⭐ High |
| **Mature ecosystem** | Webhooks, integrations | ⭐ Medium |

### Disadvantages ⚠️

| Issue | Impact | Mitigation |
|-------|--------|------------|
| **External account required** | Extra setup step | ⭐⭐ High - Not hard, but adds work |
| **Rate limits (anonymous)** | 100 pulls/6 hours | ⭐⭐⭐ Huge - Can affect users |
| **Secrets management** | Need DOCKERHUB_TOKEN | ⭐⭐ High - Security concern |
| **Retention policies** | 6 months inactive = deleted | ⭐ Low - Active projects safe |
| **No free private repos** | Only 1 free private repo | ⭐ Low - Velostream is public |

### Setup Complexity: ⭐⭐ MODERATE

**Prerequisites**:
1. Create Docker Hub account: https://hub.docker.com/signup
2. Create organization or use personal namespace
3. Generate access token: Account Settings → Security → New Access Token
4. Add to GitHub Secrets: `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`

**Workflow configuration**:
```yaml
- name: Login to Docker Hub
  uses: docker/login-action@v3
  with:
    username: ${{ secrets.DOCKERHUB_USERNAME }}  # ← Need to configure
    password: ${{ secrets.DOCKERHUB_TOKEN }}     # ← Need to configure

- name: Build and push
  uses: docker/build-push-action@v5
  with:
    push: true
    tags: |
      velostream/velostream:latest
      velostream/velostream:${{ github.ref_name }}
```

**GitHub Secrets needed**: ✅ YES (DOCKERHUB_USERNAME, DOCKERHUB_TOKEN)

**Total setup time**: **15 minutes** (account + org + token + secrets)

---

## Feature Comparison Matrix

| Feature | GHCR | Docker Hub | Winner |
|---------|------|------------|--------|
| **Setup complexity** | 5 min, no account | 15 min, external account | 🏆 GHCR |
| **GitHub integration** | Native, GITHUB_TOKEN | Via secrets | 🏆 GHCR |
| **Rate limits (public)** | 500 pulls/hour | 100 pulls/6 hours | 🏆 GHCR |
| **Rate limits (auth)** | 5000 pulls/hour | 200 pulls/6 hours | 🏆 GHCR |
| **Storage (public)** | Unlimited | Unlimited | 🤝 Tie |
| **Discoverability** | Medium | High | 🏆 Docker Hub |
| **Industry adoption** | Growing | Standard | 🏆 Docker Hub |
| **Secrets needed** | None | 2 secrets | 🏆 GHCR |
| **Security** | Fine-grained perms | Token-based | 🏆 GHCR |
| **Cost** | Free | Free (with limits) | 🏆 GHCR |

**Score**: GHCR wins 8/10 categories

---

## Real-World Constraints

### GHCR Constraints

**1. Discoverability** ⚠️
- **Issue**: People search Docker Hub first, not GHCR
- **Impact**: Lower initial adoption for new projects
- **Mitigation**:
  - Document both installation methods in README
  - GHCR URL is simple: `ghcr.io/velostreamai/velostream`
  - Add to awesome-lists, documentation

**2. Corporate Firewalls** ⚠️
- **Issue**: Some enterprises block `ghcr.io` domain
- **Impact**: Rare but possible
- **Mitigation**:
  - Publish to both registries (easy to do)
  - Most enterprises allow ghcr.io for public images

**3. Authentication for Private Images** ⚠️
- **Issue**: Requires GitHub account to pull private images
- **Impact**: None for public Velostream images
- **Mitigation**: N/A - Velostream will be public

**4. Ecosystem Maturity** ⚠️
- **Issue**: Fewer third-party integrations than Docker Hub
- **Impact**: Low - Most tools support both
- **Mitigation**: GHCR supports OCI standard (works everywhere)

### Docker Hub Constraints

**1. Rate Limits** 🚨
- **Issue**: Anonymous pulls limited to 100 per 6 hours per IP
- **Impact**: HIGH - Can break CI/CD for users
- **Example**: Company with 50 developers behind one NAT = rate limit hit quickly
- **Mitigation**:
  - Users must authenticate (annoying)
  - Or use Docker Hub Pro ($5/month)

**2. Account Deletion** ⚠️
- **Issue**: Inactive repos (6 months) get deleted on free tier
- **Impact**: Medium - Active projects safe, but risky for side projects
- **Mitigation**: Regular pushes (automated releases solve this)

**3. Credential Management** ⚠️
- **Issue**: Need to secure Docker Hub token in GitHub Secrets
- **Impact**: Medium - One more secret to rotate/manage
- **Mitigation**: Use fine-grained tokens, rotate regularly

**4. External Dependency** ⚠️
- **Issue**: Relies on Docker Hub uptime/policies
- **Impact**: Low - Very reliable, but not GitHub-controlled
- **Mitigation**: Can always migrate to GHCR if needed

---

## Industry Trends

### GHCR Adoption (2024)

**Major projects using GHCR**:
- ✅ GitHub Actions official images
- ✅ Microsoft projects (VS Code, .NET)
- ✅ Many major open-source projects
- ✅ Recommended for GitHub-native workflows

**Growth**: 📈 Rapidly increasing adoption

### Docker Hub Status

**Still dominant for**:
- Enterprise software
- Commercial products
- Legacy projects
- Maximum discoverability

**Trends**: 📊 Stable but losing ground to GHCR for GitHub projects

---

## Recommendation for Velostream

### Primary Registry: GHCR 🏆

**Why**:
1. ✅ Velostream is GitHub-native (repo, CI/CD, releases all on GitHub)
2. ✅ Zero external setup (no accounts, no secrets)
3. ✅ Better rate limits (users won't hit limits)
4. ✅ Simpler workflow (GITHUB_TOKEN just works)
5. ✅ Free unlimited public images
6. ✅ Better security (fine-grained permissions)

**URL**: `ghcr.io/velostreamai/velostream:latest`

**Installation**:
```bash
docker pull ghcr.io/velostreamai/velostream:latest
```

### Optional: Dual Publishing

**Can publish to BOTH registries** if discoverability is critical:

```yaml
- name: Build and push
  uses: docker/build-push-action@v5
  with:
    push: true
    tags: |
      ghcr.io/velostreamai/velostream:latest
      ghcr.io/velostreamai/velostream:${{ github.ref_name }}
      velostream/velostream:latest
      velostream/velostream:${{ github.ref_name }}
```

**Pros**:
- ✅ Best of both worlds
- ✅ Maximum discoverability (Docker Hub)
- ✅ Better rate limits (GHCR)

**Cons**:
- ⚠️ Need Docker Hub account + secrets
- ⚠️ Slightly more complex workflow
- ⚠️ Two registries to monitor

**Recommendation**: Start with GHCR only, add Docker Hub later if users request it.

---

## Migration Path

### Start with GHCR (Phase 2)

**Implementation**:
1. Add GHCR publishing to workflow (5 min)
2. Test multi-arch images (1 hour)
3. Document installation in README
4. Announce: "Docker images available on GHCR"

### Add Docker Hub Later (If Needed)

**Triggers to add Docker Hub**:
- User requests for Docker Hub
- Corporate users report GHCR blocked
- Want maximum discoverability

**Implementation**:
1. Create Docker Hub account (10 min)
2. Add secrets to GitHub (5 min)
3. Update workflow to push to both (5 min)
4. Total: 20 minutes to add

**Flexibility**: Can always add Docker Hub later without disrupting GHCR users.

---

## Comparison to Similar Projects

### Projects Using GHCR

| Project | Registry | Notes |
|---------|----------|-------|
| **GitHub Actions** | GHCR only | ghcr.io/actions/* |
| **VS Code** | GHCR primary | Microsoft projects |
| **Many Rust projects** | GHCR growing | Rust ecosystem trend |

### Projects Using Docker Hub

| Project | Registry | Notes |
|---------|----------|-------|
| **nginx** | Docker Hub | Legacy, established |
| **postgres** | Docker Hub | Official images |
| **redis** | Docker Hub | Official images |

### Projects Using Both

| Project | Strategy | Notes |
|---------|----------|-------|
| **Gitpod** | Both | GHCR primary, Docker Hub mirror |
| **Pulumi** | Both | Maximum reach |

**Trend**: GitHub-native projects increasingly choosing GHCR as primary.

---

## Decision Matrix

### Choose GHCR if:

- ✅ Project is GitHub-native (Velostream: YES)
- ✅ Want simplest setup (Velostream: YES)
- ✅ Rate limits are concern (Velostream: YES - users may CI/CD pull)
- ✅ Want free unlimited hosting (Velostream: YES)
- ✅ Open-source project (Velostream: YES)

**Velostream matches 5/5 criteria** → **GHCR is obvious choice**

### Choose Docker Hub if:

- ⚠️ Maximum discoverability critical (Velostream: MEDIUM - nice but not critical)
- ⚠️ Enterprise target audience (Velostream: NO - developers/open-source)
- ⚠️ Legacy integration requirements (Velostream: NO)
- ⚠️ Commercial product (Velostream: NO - open-source)

**Velostream matches 0/4 criteria** → **Docker Hub not necessary**

---

## Implementation Plan

### Phase 2A: GHCR Publishing (Recommended)

**Timeline**: 3-4 hours
**Prerequisites**: None (uses GITHUB_TOKEN)

**Files**:
- `.github/workflows/docker.yml` (new)
- `Dockerfile` (new)
- `.dockerignore` (new)

**Workflow snippet**:
```yaml
name: Docker Images

on:
  push:
    tags:
      - 'v*.*.*'

jobs:
  docker:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write  # For GHCR
    steps:
      - uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Login to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: |
            ghcr.io/velostreamai/velostream:latest
            ghcr.io/velostreamai/velostream:${{ github.ref_name }}
```

### Phase 2B: Add Docker Hub (Optional Later)

**If needed**, add Docker Hub support:

1. Create Docker Hub account
2. Add secrets
3. Add one login step to workflow
4. Add Docker Hub tags to build-push-action

**Time**: 20 minutes

---

## Security Considerations

### GHCR Security ✅

**Advantages**:
- ✅ Uses GITHUB_TOKEN (auto-rotated, scoped)
- ✅ Fine-grained permissions (packages: write)
- ✅ Inherits GitHub security (2FA, audit logs)
- ✅ No secret management needed
- ✅ Automatic token rotation

**Best Practice**:
```yaml
permissions:
  contents: read
  packages: write  # Minimal scope
```

### Docker Hub Security ⚠️

**Concerns**:
- ⚠️ Long-lived access tokens (manual rotation)
- ⚠️ Broad permissions (can delete all images)
- ⚠️ GitHub Secrets exposure risk
- ⚠️ External service dependency

**Best Practice**:
- Use fine-grained tokens (if available)
- Rotate tokens regularly (90 days)
- Use organization accounts, not personal

---

## Cost Analysis

### GHCR

| Feature | Free Tier | Cost |
|---------|-----------|------|
| Public images | Unlimited | $0 |
| Storage | Unlimited | $0 |
| Bandwidth | Unlimited | $0 |
| Rate limits | 5000/hr (auth) | $0 |

**Total**: **$0/month** ✅

### Docker Hub

| Feature | Free Tier | Cost |
|---------|-----------|------|
| Public repos | Unlimited | $0 |
| Storage | Unlimited | $0 |
| Pulls (anonymous) | 100/6hr | $0 (but limited) |
| Pulls (authenticated) | 200/6hr | $0 (but limited) |
| Pro tier (better limits) | N/A | $5/month |

**Total**: **$0/month** (with rate limit constraints)

**Winner**: GHCR (no practical limits)

---

## Final Recommendation

### ✅ Use GHCR for Velostream Phase 2

**Confidence**: 95%

**Rationale**:
1. ✅ Velostream is GitHub-native
2. ✅ Zero setup/accounts needed
3. ✅ Better rate limits for users
4. ✅ Simpler workflow
5. ✅ Industry trend for GitHub projects

**URL**: `ghcr.io/velostreamai/velostream:latest`

**Documentation**:
```bash
# Pull latest image
docker pull ghcr.io/velostreamai/velostream:latest

# Run Velostream
docker run -it ghcr.io/velostreamai/velostream:latest --version
```

### ⏳ Consider Docker Hub Later (If Needed)

**Only add Docker Hub if**:
- Users specifically request it
- Corporate environments block GHCR
- Want maximum discoverability

**Easy to add later**: 20 minutes of work, no disruption to GHCR users.

---

## Conclusion

**Question**: Should we use GHCR instead of Docker Hub?

**Answer**: **YES** - It's the smart and obvious choice for Velostream.

**Is it obvious?**: **YES** - For GitHub-native projects, GHCR is increasingly the default choice.

**Constraints?**: **Minor** - Slightly less discoverable, but rate limits and simplicity far outweigh this.

**Action**: Implement Phase 2 with GHCR, add Docker Hub later only if users request it.

---

**Document Status**: Complete
**Recommendation**: GHCR (GitHub Container Registry)
**Confidence**: HIGH (95%)
**Next Step**: Implement Phase 2 with GHCR after Phase 1 validated
