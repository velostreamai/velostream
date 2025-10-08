# Velostream Trading Demo Makefile
# Builds main project and runs trading demo

.PHONY: all build run run-short clean help

# Default target
all: build

# Build main project and demo components
build:
	@echo "🔨 Building Velostream main project..."
	@cd ../.. && cargo build --release
	@ln -sf ../../target/release/velo-cli velo-cli
	@echo "🔨 Building trading demo components..."
	@cargo build --release
	@echo "✅ Demo ready!"

# Run the trading demo
run: build
	@echo "🚀 Starting Velostream Trading Demo..."
	./start-demo.sh

# Run a short demo (1 minute)
run-short: build
	@echo "🚀 Starting short Velostream Trading Demo (1 minute)..."
	./start-demo.sh -q

# Clean demo artifacts (leaves main project build alone)
clean:
	@echo "🧹 Cleaning demo artifacts..."
	@rm -f velo-cli
	@rm -rf target/
	@echo "✅ Clean complete"

# Show help
help:
	@echo "Velostream Trading Demo Build System"
	@echo "======================================="
	@echo ""
	@echo "Available targets:"
	@echo "  build       - Build main project (default)"
	@echo "  run         - Build and run the full demo"
	@echo "  run-short   - Build and run 1-minute demo"
	@echo "  clean       - Remove demo symlinks"
	@echo "  help        - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make                    # Build main project"
	@echo "  make run                # Build and run demo"
	@echo "  make run-short          # Quick 1-minute demo"
	@echo ""
	@echo "Note: This demo references the main project's built artifacts"
	@echo "      and does not duplicate binaries locally."