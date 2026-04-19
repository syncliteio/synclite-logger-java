# PR Description

## Summary
This PR fixes INSERT validation behavior when a database connection is available but the SQL logger instance is not yet resolved.

The validation now:
- continues semantic INSERT validation when `conn` is present,
- uses logger-backed table column count lookup when logger is available,
- falls back to direct `PRAGMA table_info(...)` column count lookup when logger is null.

This preserves the current strict requirement that INSERT statements must provide values for all table columns in DBLogger/Appender/Streaming validation paths.

## Problem
In the previous implementation, validation returned early when either `conn` or `logger` was null.

That meant table-column count checks were skipped in cases where:
- `conn` was available,
- `logger` was temporarily null.

As a result, statements could bypass strict column count enforcement in that window.

## Root Cause
Early return condition was too broad:
- `if (conn == null || logger == null) return;`

The logger is only required for cached metadata lookup, not for performing the validation itself when connection metadata is accessible.

## What Changed
### Code changes
- Updated early-return condition to require only null connection for short-circuit:
  - from `if (conn == null || logger == null) return;`
  - to `if (conn == null) return;`
- Added fallback path in column count lookup:
  - use `logger.getOrLoadTableColumnCount(...)` when logger is present,
  - otherwise execute `PRAGMA table_info(<TABLE>)` on the active connection and count columns.

## Files Changed
- `logger/src/main/java/io/synclite/logger/SyncLiteUtils.java`
- `PR_DESCRIPTION.md`

## Behavior Impact
- INSERT validation remains strict and consistent even when logger lookup is unavailable.
- No behavior change when logger is available.

## Risk Assessment
Low to medium:
- Touches validation path only.
- Fallback query is read-only metadata query (`PRAGMA table_info`).
- Potential edge case: quoted/special table names continue to rely on existing parser/extractor behavior.

## Testing
Manual/observed:
- Verified no compilation errors in modified file via IDE diagnostics.

Not run in this change set:
- Full Maven test suite.

## Suggested Reviewer Checks
- Validate strict INSERT enforcement with and without logger availability.
- Verify behavior on table names across casing/schema qualifiers.
