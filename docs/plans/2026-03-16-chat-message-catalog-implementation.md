# Chat Message Catalog Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Generate a reusable Bronze-based chat message catalog report that enumerates all observed chat message variants for `93101` and `93102`, including notice-like chat rows.

**Architecture:** Add a dedicated catalog generator that reads Bronze Delta rows, filters chat message cmds, groups them by semantic and structural keys relevant to chat payloads, and writes Markdown, CSV, and JSON outputs. Reuse the existing payload expansion and semantic classification helpers so the catalog stays aligned with current profiling logic.

**Tech Stack:** Python, `deltalake`, existing profiling helpers, `pytest`

---

### Task 1: Add failing tests for catalog grouping

**Files:**
- Create: `tests/test_chat_message_catalog.py`
- Test: `tests/test_chat_message_catalog.py`

**Step 1: Write the failing test**

Add tests that:
- verify only `93101` and `93102` rows are included from a Bronze-like row list
- verify `profile_present` and `extras_keys` differences create separate variants
- verify `93102` notice rows are retained in the catalog

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: FAIL because `chat_message_catalog` module/functions do not exist yet

**Step 3: Write minimal implementation**

Create the smallest implementation needed to build an in-memory catalog from Bronze-style rows.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: PASS

### Task 2: Add output writer tests

**Files:**
- Modify: `tests/test_chat_message_catalog.py`
- Test: `tests/test_chat_message_catalog.py`

**Step 1: Write the failing test**

Add tests that validate:
- JSON report includes metadata and variant fields
- Markdown report includes summary and per-variant sections
- CSV output includes variant-level columns for `profile_present` and `extras_keys`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: FAIL on missing writer functions or wrong output schema

**Step 3: Write minimal implementation**

Implement writer helpers for JSON, Markdown, and CSV outputs.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: PASS

### Task 3: Add CLI and generate the report

**Files:**
- Create: `chat_message_catalog.py`
- Modify: `requirements.txt` if needed

**Step 1: Write the failing test**

Add a CLI-level test or direct function test that uses temporary output paths and confirms files are written.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: FAIL because the CLI entry path or generation wrapper is incomplete

**Step 3: Write minimal implementation**

Add:
- Bronze Delta reader
- report builder
- file writers
- CLI arguments for input and output paths

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: PASS

### Task 4: Verify with real Bronze data and produce deliverables

**Files:**
- Create: `data/bronze_chat_message_catalog.json`
- Create: `data/bronze_chat_message_catalog.csv`
- Create: `data/bronze_chat_message_catalog.md`

**Step 1: Run focused tests**

Run: `pytest tests/test_chat_message_catalog.py -q`
Expected: PASS

**Step 2: Generate the real report**

Run: `.venv/bin/python chat_message_catalog.py --input-root data/bronze/events`
Expected: Output files written under `data/`

**Step 3: Verify generated files**

Run: `ls -lh data/bronze_chat_message_catalog.*`
Expected: JSON, CSV, Markdown files exist

**Step 4: Inspect summary header**

Run: `sed -n '1,80p' data/bronze_chat_message_catalog.md`
Expected: Snapshot metadata and top catalog sections are present
