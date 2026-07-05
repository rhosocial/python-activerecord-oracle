# Changelog Fragments

We use Towncrier to manage our changelog. Each significant change should have a corresponding fragment file.

## Creating a Fragment

1. **Filename**: `{issue_number}.{type}.md`
   - Example: `123.added.md`

2. **Types**:
   - `security` - Security fixes (always significant)
   - `removed` - Removed features (breaking changes)
   - `deprecated` - Deprecation notices
   - `added` - New features
   - `changed` - Behavior changes
   - `fixed` - Bug fixes
   - `performance` - Performance improvements
   - `docs` - Documentation (significant changes only)
   - `internal` - Internal changes (optional)

3. **Content**:
   - Write in past tense
   - Be specific but concise
   - Focus on user impact
   - One change per fragment

## Fragment Lifecycle

- **Created**: When feature/fix branch is created
- **Merged**: Fragment merges with the code
- **Compiled**: During final release (not pre-releases)
- **Deleted**: Automatically removed after compilation
- **Abandoned**: Manually deleted if feature is abandoned

## Good Examples

```markdown
<!-- 123.added.md -->
Added support for Oracle JSON operations in query builder.
```

```markdown
<!-- 456.fixed.md -->
Fixed connection leak that occurred when Oracle connections were not properly released after query timeout.
```

```markdown
<!-- 789.security.md -->
**SECURITY**: Fixed SQL injection vulnerability in parameterized queries when using list parameters. CVE-2024-XXXXX.
```

## Bad Examples

```markdown
<!-- Bad: Too vague -->
Fixed bug in queries.
```

```markdown
<!-- Bad: Too technical, no user impact -->
Refactored QueryBuilder._build_where() method to use visitor pattern.
```

```markdown
<!-- Bad: Multiple unrelated changes -->
Added CTE support, fixed connection leak, updated documentation.
```

## When to Skip

- Internal refactoring (no behavior change)
- Trivial fixes (typos in comments)
- Work-in-progress (fragment in final PR)

## If Feature is Abandoned

Simply delete the fragment file:

```bash
rm changelog.d/123.added.md
```

The fragment never enters CHANGELOG.md, so no cleanup needed there.

## Commands

```bash
# Preview changelog
towncrier build --draft --version X.Y.Z

# Build changelog (removes fragments)
towncrier build --version X.Y.Z --yes
```

## Fragment Types in Detail

### Security (`security`)
Use for any security vulnerability fixes. Always include CVE number if available.

Example:
```markdown
**SECURITY**: Fixed SQL injection vulnerability in query parameter handling. All users should upgrade immediately. CVE-2024-XXXXX.
```

### Removed (`removed`)
Use for removed features or APIs (breaking changes). Explain what was removed and what to use instead.

Example:
```markdown
**BREAKING**: Removed deprecated `Model.find_by()` method. Use `Model.where()` instead. This method was deprecated in v1.1.0.
```

### Deprecated (`deprecated`)
Use for deprecation notices. Include timeline for removal and recommended alternative.

Example:
```markdown
Deprecated `QueryBuilder.filter()` method in favor of `QueryBuilder.where()`. The old method will be removed in v2.0.0.
```

### Added (`added`)
Use for new features. Describe what users can now do that they couldn't before.

Example:
```markdown
Added support for window functions (ROW_NUMBER, RANK, LAG, LEAD) in Oracle. See documentation for usage examples.
```

### Changed (`changed`)
Use for changes to existing functionality (non-breaking). Explain what changed and why.

Example:
```markdown
Changed default connection pool size from 5 to 10 for better concurrency. Override with `pool_size` configuration parameter.
```

### Fixed (`fixed`)
Use for bug fixes. Describe the problem that was fixed.

Example:
```markdown
Fixed race condition in connection pool that could cause deadlocks under high concurrent load.
```

### Performance (`performance`)
Use for performance improvements. Include metrics if possible.

Example:
```markdown
Improved bulk insert performance by 3x through batched operations. Inserts of 1000+ records now use database-specific bulk insert syntax.
```

### Documentation (`docs`)
Use only for significant documentation changes (new guides, major reorganization).

Example:
```markdown
Added comprehensive guide for implementing custom type converters with examples for JSON, UUID, and encrypted fields.
```

### Internal (`internal`)
Optional. Use for internal changes that don't affect users directly.

Example:
```markdown
Refactored query builder to use visitor pattern for better maintainability.
```

## Multiple Issues

If a change affects multiple issues, use `+` in the filename:

```bash
# Change affects issues #123 and #456
123+456.fixed.md
```

## Review Checklist

Before committing your fragment:

- [ ] Filename follows `{issue}.{type}.md` format
- [ ] Content is in past tense
- [ ] Describes user impact, not implementation
- [ ] One logical change per fragment
- [ ] Appropriate type selected
- [ ] Security issues marked with **SECURITY** prefix
- [ ] Breaking changes marked with **BREAKING** prefix
