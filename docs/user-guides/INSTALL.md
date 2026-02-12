# Welcome to Velostream!

Thank you for downloading Velostream - the high-performance streaming SQL engine.

## 🚀 Quick Start

### 1. Verify Installation

```bash
./bin/velo-sql --version
```

You should see version information printed to the console.

### 2. Run Your First Demo

```bash
# One Billion Row Challenge - processes 1B rows
cd demo/1brc
./run-1brc.sh
```

**Expected**: Processing completes in seconds with aggregation results.

### 3. Try More Demos

```bash
# Complete pipeline demo
cd demo/datasource-demo
../../bin/complete_pipeline_demo

# File processing demo
../../bin/file_processing_demo
```

### 4. Add to PATH (Optional)

**Linux/macOS**:
```bash
source setup-env.sh
velo-sql --version  # Now works from any directory
```

**Windows**:
```powershell
.\setup-env.bat
velo-sql --version
```

## 📁 What's Inside

```
velostream-<version>-<platform>/
├── bin/                      # All Velostream binaries
│   ├── velo-sql              # Main streaming SQL server
│   ├── velo-cli              # Management CLI
│   ├── velo-test             # Test harness runner
│   ├── velo-sql-batch        # Batch query executor
│   ├── velo-1brc             # 1BRC benchmark
│   └── complete_pipeline_demo, file_processing_demo
│
├── demo/                     # Working demos with SQL files
│   ├── 1brc/                 # One Billion Row Challenge
│   ├── trading/              # Financial trading demo
│   ├── datasource-demo/      # Data source examples
│   └── test_harness_examples/ # Test harness tiers
│
├── configs/                  # Example configurations
├── setup-env.sh (or .bat)    # PATH setup helper
├── README.md                 # Project overview
├── LICENSE                   # Apache 2.0 license
└── INSTALL.md                # This file
```

## 📚 Full Documentation

- **Installation Guide**: https://github.com/velostreamai/velostream/blob/master/docs/user-guides/INSTALLATION.md
- **SQL Reference**: https://github.com/velostreamai/velostream/tree/master/docs/sql
- **Demo Guides**: https://github.com/velostreamai/velostream/tree/master/demo
- **Full Documentation**: https://github.com/velostreamai/velostream/tree/master/docs

## ⚙️ System Requirements

| Platform | Requirement |
|----------|-------------|
| **Linux** | Kernel 3.2+ (any distro) |
| **macOS** | macOS 10.15+ (Catalina or later) |
| **Windows** | Windows 10 or 11 (64-bit) |
| **Memory** | 512 MB minimum, 2 GB+ recommended |

**No runtime dependencies** - binaries are fully static (Linux) or self-contained.

## 🐛 Troubleshooting

### "Permission denied" error (Linux/macOS)

```bash
chmod +x ./bin/velo-sql
# Or make all binaries executable:
chmod +x ./bin/*
```

### "Command not found" error

Either use the full path:
```bash
./bin/velo-sql --version
```

Or add to PATH:
```bash
source setup-env.sh  # Linux/macOS
.\setup-env.bat      # Windows
```

### macOS security warning

**"velo-sql cannot be opened because it is from an unidentified developer"**

**Solution**:
1. Right-click the binary → "Open" → Click "Open" again
2. Or: System Preferences → Security & Privacy → Click "Allow Anyway"
3. Or remove quarantine: `xattr -d com.apple.quarantine ./bin/velo-sql`

### More help needed?

- **GitHub Issues**: https://github.com/velostreamai/velostream/issues
- **Discussions**: https://github.com/velostreamai/velostream/discussions
- **Full Troubleshooting Guide**: See `INSTALLATION.md` in docs/user-guides/

## 🎯 Next Steps

1. **Run demos** to see Velostream in action
2. **Read SQL reference** to learn query syntax
3. **Start building** your streaming applications
4. **Join the community** to share your experience

## 📝 Feedback

Found a bug? Have a feature request? Want to contribute?

- Report issues: https://github.com/velostreamai/velostream/issues/new
- Contribute: https://github.com/velostreamai/velostream/blob/master/CONTRIBUTING.md

---

**Happy streaming!** 🚀

**Velostream** - Real-time SQL for the modern data stack
