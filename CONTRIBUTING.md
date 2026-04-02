# Contributing to Arneb

Thank you for your interest in contributing to Arneb! This guide will help you get started.

## Development Setup

### Prerequisites

- Rust 1.74.0 or later
- Git

### Building

```bash
git clone https://github.com/formosa-ventures/trino-alt.git
cd trino-alt
cargo build
```

### Running Tests

```bash
cargo test                           # all tests
cargo test -- --nocapture            # with stdout
cargo test <test_name>               # single test
```

### Linting and Formatting

All code must pass these checks before merging:

```bash
cargo fmt -- --check                 # check formatting
cargo fmt                            # auto-format
cargo clippy -- -D warnings          # lint
```

## Making Changes

1. **Fork and branch**: Create a feature branch from `main`.
2. **Write tests**: All new functionality must include tests.
3. **Keep commits atomic**: Each commit should be a single logical change.
4. **Follow conventions**: See the coding conventions below.

## Commit Message Format

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `style` | Formatting, no code change |
| `refactor` | Code restructuring, no behavior change |
| `perf` | Performance improvement |
| `test` | Adding or correcting tests |
| `chore` | Build process or auxiliary tool changes |

### Examples

```
feat(planner): add support for LATERAL joins

fix(protocol): handle NULL values in extended query parameters

refactor(execution): extract common aggregate logic
```

## Pull Request Process

1. Ensure all CI checks pass (format, clippy, tests, build).
2. Update documentation if your change affects public APIs.
3. Fill out the PR template with a clear description.
4. Request a review from a maintainer.

## Coding Conventions

- Use `thiserror` for library error types, `anyhow` only in the server binary.
- Prefer `Arc<dyn Trait>` for polymorphic plan nodes and operators.
- All public APIs get doc comments.
- Use `tracing` (not `log`) for instrumentation.
- Tests live in `#[cfg(test)] mod tests` within source files.

## Architecture Overview

See the [README](README.md) for the full architecture description. Key data flow:

```
SQL → Parser → AST → Planner → LogicalPlan → Optimizer → PhysicalPlan → Arrow streams
```

## Getting Help

- Open an issue for bug reports or feature requests.
- Use discussions for questions about architecture or design.

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
