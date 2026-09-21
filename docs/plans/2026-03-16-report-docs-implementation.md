# Report Docs Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the report and catalog outputs discoverable from README and add a durable CMD message-type reference with examples.

**Architecture:** Update README with a compact index of generated analysis files and their purpose, then add a dedicated documentation page for `cmd` message types that references the latest catalog outputs and explains representative structures.

**Tech Stack:** Markdown, existing generated report files

---

### Task 1: Add report index to README

**Files:**
- Modify: `README.md`

**Step 1: Write the content**

Add a section that lists:
- coarse Bronze report files
- chat message catalog files
- historical json_raw report files

For each file, explain what information a reader can get from it.

**Step 2: Verify the paths**

Run: `sed -n '1,260p' README.md`
Expected: New section is present and links point to real files

### Task 2: Add CMD message reference document

**Files:**
- Create: `docs/chzzk_cmd_message_types.md`

**Step 1: Write the content**

Document each major observed `cmd` with:
- what type of message it is
- which report to inspect for more detail
- simplified example structure
- notable discriminator fields

**Step 2: Verify the file**

Run: `sed -n '1,260p' docs/chzzk_cmd_message_types.md`
Expected: CMD sections and examples are present

### Task 3: Verify final documentation set

**Files:**
- Modify: `README.md`
- Create: `docs/chzzk_cmd_message_types.md`

**Step 1: Read back the new docs**

Run: `sed -n '1,260p' README.md`
Run: `sed -n '1,260p' docs/chzzk_cmd_message_types.md`
Expected: Content is coherent and references the generated files correctly
