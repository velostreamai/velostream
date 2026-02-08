# Velostream Trading Demo Makefile
# Builds main project and runs trading demo

.PHONY: all build run run-short clean help test validate

# Default target
all: build

# Build main project
build:
	@echo "🔨 Building Velostream main project..."
	@cd ../.. && cargo build --release --bin velo-test --bin velo-sql
	@echo "✅ Build complete!"

# Run the trading demo
run: build
	@echo "🚀 Starting Velostream Trading Demo..."
	./start-demo.sh

# Run a short demo (1 minute)
run-short: build
	@echo "🚀 Starting short Velostream Trading Demo (1 minute)..."
	./start-demo.sh -q

# Validate SQL syntax
validate: build
	@echo "🔍 Validating SQL files..."
	@for sql in apps/app_*.sql; do \
		echo "  Validating $$sql..."; \
		../../target/release/velo-test validate "$$sql"; \
	done
	@echo "✅ Validation complete"

# Run tests
test: build
	@echo "🧪 Running tests..."
	@./velo-test.sh
	@echo "✅ Tests complete"

# Clean demo artifacts
clean:
	@echo "🧹 Cleaning demo artifacts..."
	@rm -f velo-cli
	@rm -rf deploy/
	@echo "✅ Clean complete"

# Show help
help:
	@echo "Velostream Trading Demo Build System"
	@echo "======================================="
	@echo ""
	@echo "Available targets:"
	@echo "  build       - Build main project binaries (default)"
	@echo "  run         - Build and run the full demo"
	@echo "  run-short   - Build and run 1-minute demo"
	@echo "  validate    - Validate SQL syntax"
	@echo "  test        - Run test suite"
	@echo "  clean       - Remove artifacts"
	@echo "  help        - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make                    # Build main project"
	@echo "  make run                # Build and run demo"
	@echo "  make run-short          # Quick 1-minute demo"
	@echo "  make validate           # Check SQL syntax"
	@echo ""
	@echo "Note: This demo uses velo-test for data generation via @data.* hints"
