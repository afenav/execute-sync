# Repository Guidelines

## Development Environment
- This project uses [mise](https://mise.jdx.dev/) to manage development tools. Run `mise install` to install the pinned toolchain, and trust the repo config with `mise trust` if prompted.
- Preferred workflows should use the tasks defined in `mise.toml` when available (e.g., `mise run launch`, `mise run upgrade-deps`).
- Primary language is Go; ensure you are using the Go version provided by `mise`.

## Coding Standards
- Keep the code straightforward and well-documented—avoid overly clever constructs.
- Add Go doc comments for any exported types, functions, methods, or package-level variables you introduce or modify.
- Follow idiomatic Go practices: meaningful naming, early returns for error handling, and minimal global state.
- Format all Go code with `gofmt` (or `go fmt ./...`) before committing.
- Maintain clear separation of concerns between CLI command definitions (`src/cmd_*.go`), shared logic under `src/internal`, and configuration handling.

## Testing & Verification
- Run `go test ./...` before submitting changes whenever Go code is touched.
- If you modify command-line behavior, update or add usage examples and ensure `execute-sync --help` reflects the new options.
- Document manual testing steps in your PR description when automated coverage is not feasible.

## Documentation & Samples
- Keep the README and any relevant docs up-to-date with behavior changes.
- When adding new functionality, provide inline comments or examples that help readers understand typical usage.
- Treat this repository as a sample-quality project: prioritize clarity, approachability, and maintainability over micro-optimizations.
