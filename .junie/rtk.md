# RTK - Rust Token Killer (Codex CLI)

**Usage**: Token-optimized CLI proxy for shell commands.

## Rule

Always prefix shell commands with `rtk`.

Examples:

```bash
rtk git status
rtk cargo test
rtk go build -o app .
rtk pytest -q
```

## Meta Commands

```bash
rtk gain            # Token savings analytics
rtk gain --history  # Recent command savings history
rtk proxy <cmd>     # Run raw command without filtering
rtk smart <file>    # High-density code analysis (symbols/deps only)
rtk read -l aggr    # Aggressive token filtering for large files
tkn -c -m gpt-4      # Count tokens for a file/stdin
sqz gain            # Detailed compression stats and session history
```

## Token Optimization Strategies

1. **Token Budgeting**:
   - Before reading large files, check their cost: `tkn -c -m gpt-4 <file>`.
   - Aim to keep individual file reads under 2,000 tokens.

2. **Hierarchy of Reading**:
   - Level 0: `rtk graphify query` (Locate target)
   - Level 1: `rtk smart <file>` (Inspect symbols/logic)
   - Level 2: `rtk read -l aggr <file>` (Filtered content)
   - Level 3: `sqz compress <file>` (Aggressively compressed content)
   - Level 4: `rtk read <file>` (Full content - use sparingly)

3. **Ultra-Compact Mode**:
   - Use `--ultra-compact` for commands with large output (e.g. `rtk go test ./...`).

4. **Dedup & Reference**:
   - If `sqz` returns a `§ref:HASH§`, use `sqz expand HASH` to resolve it if needed.
   - Use `sqz gain` to track your total session savings.

5. **Smart Filters**:
   - Avoid `cat`, `grep`, `ls` raw commands. Always use `rtk` equivalents to trigger specialized output filters.

## Verification

```bash
rtk --version
rtk gain
which rtk
```
