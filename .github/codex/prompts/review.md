# Codex Pull Request Review

Review the current pull request and produce concrete, actionable feedback.

Context:
- Base branch: `${GITHUB_BASE_REF}`
- Head branch: `${GITHUB_HEAD_REF}`
- Repository: `${GITHUB_REPOSITORY}`

What to do:
1. Inspect the changed files and diff against the base branch.
2. Focus on:
   - correctness and behavioral regressions
   - race conditions and data consistency
   - API compatibility and error handling
   - performance risks
   - missing or weak tests
3. Prioritize findings by severity.
4. If evidence is insufficient, raise a question instead of guessing.

Repository-specific rules to enforce:
- Go function names should use camelCase and avoid `_`.
- Variables should use lowerCamelCase.
- Third-party/library errors should be wrapped with `errors.Trace(err)` or `errors.WrapError(...)` at the boundary.
- Log messages should avoid function names and avoid `-` in message text.

Output format:
- `### Findings`
  - For each finding: `[SEV-<level>] <title>`
  - Include impacted file path and line reference when possible.
  - Explain why it matters and suggest a concrete fix.
- `### Questions`
  - List open uncertainties that need human input.
- `### Summary`
  - 2-4 short bullets.

If there are no significant findings, say: `No blocking issues found in this PR.`
