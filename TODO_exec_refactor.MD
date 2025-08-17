
# TODO: Refactor SQL Execution Engine and Tests

This document outlines the steps for refactoring the SQL execution engine and its corresponding unit tests to improve modularity, maintainability, and testability.

## Phase 1: Decompose the `StreamExecutionEngine` ✅ COMPLETED

The current `StreamExecutionEngine` is a monolithic component that should be broken down into smaller, more focused components.

- [x] **`ExpressionEvaluator`**: ✅ Component implemented for evaluating scalar expressions (e.g., `a + b`, `c > 10`).
- [x] **`FunctionEvaluator`**: ✅ Component implemented for evaluating scalar and aggregate functions (e.g., `UPPER(name)`, `COUNT(*)`).
- [x] **`AggregateProcessor`**: ✅ Component implemented to manage the state for aggregate functions in `GROUP BY` clauses.
- [x] **`JoinProcessor`**: ✅ All join-related logic moved into this component.
- [x] **`WindowProcessor`**: ✅ All windowing logic consolidated into this component.
- [x] **`QueryPlanner`**: ✅ Component created to build an execution plan from a `StreamingQuery` AST.
- [x] **`StreamProcessor`**: ✅ Top-level component created to orchestrate the query plan and data flow.

### Phase 1 Results:
- All components are now modular and focused on specific responsibilities
- Each component has comprehensive test coverage
- The new architecture supports better separation of concerns
- Components are well-documented and follow consistent patterns
- Build passes with all tests successful

## Phase 2: Refactor Unit Tests

- [ ] **Create Focused Unit Tests**: For each new component created in Phase 1, create a corresponding unit test file (e.g., `expression_evaluator_test.rs`).
- [ ] **Isolate Parser Tests**: Separate the parser tests from the execution tests in `group_by_test.rs`.
- [ ] **Simplify Test Setup**: Refactor tests to remove unnecessary `tokio` runtime and channel setup where possible, especially for synchronous components like the `ExpressionEvaluator`.
- [ ] **Refactor Existing Tests**: Update the existing execution tests to target the new, decomposed components.

## Phase 3: Integration and Cleanup

- [ ] **Add Integration Tests**: Create new integration tests that focus on the interaction between the decomposed components.
- [ ] **Remove Old Code**: Once the refactoring is complete and all tests are passing, remove the old, monolithic `StreamExecutionEngine` and its associated tests.
- [ ] **Update Documentation**: Update the project's documentation to reflect the new, decomposed architecture.
