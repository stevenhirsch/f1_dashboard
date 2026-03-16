# GitHub Pages Deployment Plan

## Overview

The existing fitness dashboard workflow is a good starting point but requires three
changes before it will work for this project. Two are trivial; one requires a code
change to the API client.

---

## Required Changes to the Workflow YAML

### 1. File name
```diff
- marimo export html-wasm fitness_dashboard.py -o dist --mode run
+ marimo export html-wasm dashboard.py -o dist --mode run
```

### 2. Dependencies
The F1 dashboard uses additional packages. The install step needs them all so that
`marimo export` can parse and analyse the notebook:
```diff
- pip install marimo matplotlib pandas numpy
+ pip install marimo matplotlib pandas numpy requests plotly altair
```

### 3. Bundle local modules (critical)
The dashboard imports from `api/` and `plots/` — local directories that are not on
PyPI. The `--include-local-files` flag tells marimo to copy those directories into
the `dist/` output so they are available at runtime inside the browser:
```diff
- marimo export html-wasm dashboard.py -o dist --mode run
+ marimo export html-wasm dashboard.py -o dist --mode run \
+   --include-local-files api plots
```

---

## Code Change Required: `api/openf1.py`

This is the only source-code change needed. The `requests` library uses synchronous
blocking I/O, which does not work inside a browser's JavaScript runtime (Pyodide).
All network calls must go through `urllib`, which Pyodide patches to work via
`XMLHttpRequest`.

The change is a drop-in replacement inside `_get()`:

| Before | After |
|--------|-------|
| `import requests` | `import json, urllib.parse, urllib.request, urllib.error` |
| `requests.get(url, params=..., timeout=30)` | `urllib.request.urlopen(url, timeout=30)` |
| `resp.status_code == 429` | `except urllib.error.HTTPError as exc: if exc.code == 429` |
| `resp.raise_for_status()` | `raise` (re-raise the HTTPError) |
| `resp.json()` | `json.loads(resp.read().decode())` |

The query string must be built manually with `urllib.parse.urlencode()` before
passing to `urlopen`, since `urlopen` does not accept a `params` keyword argument.
Everything else in the file (caching, retry logic, all the `get_*` functions)
stays exactly the same.

---

## Final Workflow File

```yaml
name: Deploy F1 Dashboard to GitHub Pages

on:
  push:
    branches: ["main", "master"]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install marimo matplotlib pandas numpy requests plotly altair

      - name: Export notebook to HTML
        run: |
          marimo export html-wasm dashboard.py -o dist --mode run \
            --include-local-files api plots
          touch dist/.nojekyll

      - name: Upload Pages Artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: dist

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    permissions:
      pages: write
      id-token: write
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
        with:
          artifact_name: github-pages
```

---

## What Stays the Same

- The two-job structure (build → deploy) is correct and unchanged.
- `actions/upload-pages-artifact@v3` and `actions/deploy-pages@v4` are the right
  actions for this pattern.
- The `concurrency` block preventing overlapping deployments is appropriate.
- The `touch dist/.nojekyll` line is still needed to prevent GitHub Pages from
  trying to process the output as a Jekyll site.
