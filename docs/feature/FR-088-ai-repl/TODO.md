# FR-088: AI REPL - Implementation Tracker

## Status: Planning

## Phase Summary

| Phase | Description | Status |
|-------|-------------|--------|
| **Phase 1** | Foundation (REPL, session, prompts) | Not Started |
| **Phase 2** | Schema & Data (BYO, infer, generate) | Not Started |
| **Phase 3** | Query Development (AI gen, execute) | Not Started |
| **Phase 4** | File Operations (save, load, edit) | Not Started |
| **Phase 5** | Polish (history, completion, colors) | Not Started |

---

## Phase 1: Foundation

### REPL Infrastructure
- [ ] Add `rustyline` dependency for readline support
- [ ] Create `ai_chat/repl.rs` with main loop
- [ ] Implement signal handling (Ctrl+C graceful exit)
- [ ] Add `--no-color` flag support

### Session State Management
- [ ] Create `ai_chat/session.rs`
- [ ] Define `SessionState` struct (topics, schemas, jobs, etc.)
- [ ] Implement conversation history tracking
- [ ] Add "unsaved changes" tracking for exit prompt

### Command Parsing
- [ ] Create `ai_chat/commands.rs`
- [ ] Implement command tokenizer
- [ ] Define `Command` enum for all supported commands
- [ ] Add `help` command with categorized output

### System Prompt
- [ ] Create `ai_chat/system_prompt.rs`
- [ ] Define static context (SQL grammar, annotations, patterns)
- [ ] Implement dynamic state injection
- [ ] Add tool definitions section

### Entry Point
- [ ] Add `Commands::Ai` variant to `velo-test` CLI
- [ ] Implement CLI argument parsing (--kafka, --schemas, --load, etc.)
- [ ] Wire up REPL startup

---

## Phase 2: Schema & Data

### BYO Schema Loading
- [ ] Implement `load schemas from <dir>` command
- [ ] Validate loaded schemas
- [ ] Display schema summary on load
- [ ] Track schemas in session state

### Schema Inference
- [ ] Implement `infer schema from <topic>` command
- [ ] Sample N messages from topic
- [ ] Detect field types from JSON structure
- [ ] Generate `.schema.yaml` format output

### AI Schema Creation
- [ ] Implement `create schema for <domain>` command
- [ ] Domain templates (e-commerce, IoT, financial, etc.)
- [ ] Interactive Q&A for customization
- [ ] Save to schemas directory

### Data Generation
- [ ] Implement `generate <N> <source>` command
- [ ] Support constraint overrides (`with <constraints>`)
- [ ] Progress bar for large generations
- [ ] Publish to Kafka topic

### Topic Exploration
- [ ] Implement `topics` command (list with counts)
- [ ] Implement `messages <topic>` command (peek)
- [ ] Cache topic list for numbered references

---

## Phase 3: Query Development

### AI Query Generation
- [ ] Create `ai_chat/chat_handler.rs`
- [ ] Implement Claude API calls with system prompt
- [ ] Parse SQL from AI response
- [ ] Extract annotations from generated SQL

### Response Parsing
- [ ] Create `ai_chat/response_parser.rs`
- [ ] Detect SQL code blocks in response
- [ ] Extract tool calls (if using tool format)
- [ ] Parse action suggestions [r], [e], [n]

### Query Execution
- [ ] Integrate with test harness `QueryExecutor`
- [ ] Implement `run` / `r` command
- [ ] Capture and display output records
- [ ] Handle execution errors gracefully

### Metrics Display
- [ ] Collect metrics during execution
- [ ] Format throughput, latency, memory
- [ ] Show window state size for windowed queries
- [ ] Implement `@metrics: enabled` annotation

### Profiling
- [ ] Implement `@profiling: detailed` support
- [ ] Capture stage-level timing
- [ ] Format profiling report table
- [ ] AI suggestions based on profiling

---

## Phase 4: File Operations

### Save Session
- [ ] Implement `save` command (interactive)
- [ ] Prompt for app name, description
- [ ] Generate project structure:
  - `sql/<app>.sql`
  - `schemas/*.schema.yaml`
  - `test_spec.yaml`
  - `configs/kafka.yaml`
- [ ] Auto-generate test assertions from metrics

### Load Existing File
- [ ] Implement `load <file>` command
- [ ] Parse SQL file into jobs
- [ ] Extract annotations
- [ ] Display job list

### Modify Jobs
- [ ] Implement `show <N>` command
- [ ] Implement `append` command
- [ ] Implement `replace <N>` command
- [ ] Implement `delete <N>` command
- [ ] Track modifications for save prompt

### Test Spec Generation
- [ ] Generate assertions based on query patterns
- [ ] Use execution metrics for realistic thresholds
- [ ] Include schema references
- [ ] Support `--no-tests` flag to skip

---

## Phase 5: Polish

### Command History
- [ ] Persist history to `~/.velostream/ai_history`
- [ ] Load history on startup
- [ ] Implement `--history <file>` flag
- [ ] Up/down arrow navigation

### Tab Completion
- [ ] Complete commands
- [ ] Complete schema names
- [ ] Complete topic names
- [ ] Complete job numbers

### Colored Output
- [ ] SQL syntax highlighting
- [ ] Success/error colors
- [ ] Metrics highlighting (good/warning/bad)
- [ ] Table formatting

### Error Handling
- [ ] Graceful SQL validation errors
- [ ] Kafka connection errors
- [ ] AI API errors (rate limit, etc.)
- [ ] Recovery suggestions

### Testing
- [ ] Unit tests for command parsing
- [ ] Unit tests for response parsing
- [ ] Integration tests with MockAiProvider
- [ ] E2E tests with testcontainers

---

## Files to Create

```
src/velostream/test_harness/
├── ai_chat/
│   ├── mod.rs
│   ├── repl.rs
│   ├── session.rs
│   ├── system_prompt.rs
│   ├── chat_handler.rs
│   ├── response_parser.rs
│   ├── tool_executor.rs
│   ├── commands.rs
│   └── display.rs
```

## Files to Modify

```
src/bin/velo-test.rs           # Add Commands::Ai
src/velostream/test_harness/mod.rs  # Export ai_chat module
Cargo.toml                     # Add rustyline, colored, indicatif
```

---

## Dependencies to Add

```toml
rustyline = "14"
colored = "2"
indicatif = "0.17"
```

---

## Open Questions

1. **Conversation persistence**: Save/load conversation history between sessions?
2. **Multi-file projects**: Support editing multiple SQL files in one session?
3. **Git integration**: Auto-commit changes? Branch per session?
4. **Remote Kafka**: How to handle schema registry authentication?
5. **Cost tracking**: Display Claude API token usage?
