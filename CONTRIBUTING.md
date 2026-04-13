# Contributing to Mur-Mur

Thanks for your interest in contributing! This guide will help you get started.

## Prerequisites

- **Node.js 22+** (LTS recommended)
- **NATS server** running locally (for integration tests)
- A working knowledge of TypeScript

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/<you>/mur-mur-v2.git`
3. Create a feature branch: `git checkout -b feat/my-feature`
4. Install dependencies and verify everything works:

```bash
npm install
npm run build
npm test
```

## Development Workflow

- Run unit tests only: `npm run test:unit`
- Type-check without emitting: `npm run typecheck`
- Build all packages: `npm run build`

## Code Style

- TypeScript **strict** mode is enabled project-wide
- Avoid `any` -- use `unknown` and narrow with type guards
- Prefer `readonly` for properties and parameters that should not be mutated
- Keep functions small and focused
- Use explicit return types on exported functions

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation only
- `test:` adding or updating tests
- `refactor:` code change that neither fixes a bug nor adds a feature
- `chore:` build process, dependencies, CI

Examples:
```
feat(core): add message deduplication by sender+conversationId
fix(mcp-server): handle NATS reconnect during flush
docs: update operations guide with retry policy
```

## Pull Requests

1. Ensure all tests pass and the build is clean
2. Update documentation if your change affects public APIs
3. One logical change per PR -- keep them focused
4. Fill in the PR template; reference any related issues

## Reporting Issues

Use the GitHub issue templates:

- **Bug Report** -- for something broken or unexpected
- **Feature Request** -- for new ideas or enhancements

## Questions?

Open a discussion or reach out via an issue. We are happy to help.
