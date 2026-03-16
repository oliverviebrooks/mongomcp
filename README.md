# mongo-mcp-gen

Generate a ready-to-run [FastMCP](https://github.com/jlowin/fastmcp) server and Markdown reference document from any MongoDB database. Point it at a database, and it samples your collections, infers their schemas, and emits a working Python MCP server with search, lookup, count, and sample tools for every collection.

---

## Install

```bash
pip install -e .
```

Dependencies: `pymongo`, `mcp[cli]`, `pyyaml`.

---

## Quick start

```bash
# Minimal — generates <db>_server.py and <db>_server.md
mongo-mcp-gen --uri mongodb://localhost:27017 --db mydb

# Full options
mongo-mcp-gen \
    --uri mongodb://host:27017 \
    --db mydb \
    --name mydb-mcp \
    --output server.py \
    --exclude logs audit tmp \
    --samples 100 \
    --config extra_tools.yaml
```

---

## CLI options

| Flag | Default | Description |
|------|---------|-------------|
| `--uri` | *(required)* | MongoDB connection URI |
| `--db` | *(required)* | Database name |
| `--name` | `<db>-mcp` | Name embedded in the generated MCP server |
| `--output` | `<db>_server.py` | Output Python filename |
| `--exclude` | none | Collection names to skip, space-separated |
| `--samples` | `50` | Documents to sample per collection for schema inference |
| `--config` | none | YAML file defining extra custom tools (see below) |

A Markdown reference document is always written alongside the `.py` with the same base name.

---

## What gets generated

Running the generator connects to MongoDB, lists collections, samples each one, and emits a Python file structured like this:

```
header
  imports, MongoClient setup, FastMCP instance, to_json() helper

per-collection block (one per non-excluded collection)
  find_{collection}(...)
  get_{collection}_by_id(...)   # only if an ID field is detected
  count_{collection}(...)
  sample_{collection}(...)

custom tools block              # only if --config is supplied
  tools defined in the YAML config

footer
  main() { mcp.run() }
```

The generated server runs standalone: `python server.py`

---

## Schema inference

For each collection the generator samples up to `--samples` documents using MongoDB's `$sample` aggregation stage (falls back to `find().limit()` for small collections). It then:

1. Collects every top-level field that appears in the sample
2. Determines the dominant Python type per field: `str`, `int`, `float`, `bool`, `ObjectId`, or `mixed`
3. Calculates presence (fraction of sampled docs that contain the field)
4. Selects up to **5 string fields** (by presence) as regex search params
5. Selects up to **3 numeric fields** (by presence, excluding the ID field) as equality filter params
6. Detects the integer ID field — any `int` field named `ID`, `id`, or ending in `ID` with ≥ 90% presence
7. Detects ObjectId `_id` for document lookup

Fields with presence below 10% are not promoted to named parameters. Python keywords used as field names are prefixed with `field_` in the generated parameter names.

---

## Auto-generated tools

### `find_{collection}`

Searches the collection. All parameters are optional and combine with AND.

**String parameters** use `$regex` with the `i` (case-insensitive) flag — partial matches work:

```python
find_users(name="alice")
# generates: query["name"] = {"$regex": "alice", "$options": "i"}
# matches: "Alice", "alice smith", "Malice"
```

**Numeric parameters** use exact equality:

```python
find_users(age=30)
# generates: query["age"] = 30
```

**`limit`** — default `10`, hard-capped at `100` in the generated code.

**`filter_json`** — accepts any valid MongoDB filter as a JSON string. Lets you query any field, use any operator, and work around the fixed named-parameter limitation. Merged with named params; named params win on key conflicts.

```python
# Range query
find_users(filter_json='{"age": {"$gt": 25, "$lt": 40}}')

# Field not in the named params
find_users(filter_json='{"status": "active"}')

# Array membership
find_users(filter_json='{"tags": {"$in": ["admin", "moderator"]}}')

# Combine with a named param
find_users(country="sweden", filter_json='{"score": {"$gt": 100}}')

# Existence check
find_users(filter_json='{"deleted_at": {"$exists": false}}')
```

Invalid JSON in `filter_json` returns `{"error": "..."}` rather than raising.

Returns `{"count": N, "results": [...]}`.

---

### `get_{collection}_by_id`

Fetches exactly one document by its ID. Only generated when an ID field is detected:

- Integer ID field (`ID`, `id`, or `*ID` with ≥ 90% presence) → `id: int` param
- MongoDB `ObjectId` `_id` → `id: str` param (24-character hex string)

Returns the document dict or `{"error": "..."}` if not found.

---

### `count_{collection}`

Same parameters as `find_` (named fields + `filter_json`) but returns `{"count": N}` instead of documents. Useful for checking result size before fetching.

---

### `sample_{collection}`

Returns up to `n` random documents (default `5`, max `20`) using `$sample`. No filters. Use this to explore what a collection looks like at query time — especially useful when the schema table in the `.md` file doesn't tell the full story about nested structures.

---

## Custom tools via `--config`

Pass a YAML config file to generate tools beyond the automatic per-collection set. The config has a single top-level key `tools`, each entry having a `type` field.

```bash
mongo-mcp-gen --uri mongodb://localhost:27017 --db mydb --config tools.yaml
```

An example config demonstrating all types is in [examples/csdb_tools.yaml](examples/csdb_tools.yaml).

---

### `type: find`

A named find tool with explicit field paths and operators. Use this when you need to filter on nested dot-path fields that the auto-sampler wouldn't surface (e.g. `Credits.Credit.Handle.HandleText`).

```yaml
tools:
  - type: find
    name: posts_by_author
    description: "Find posts by a specific author"
    collection: posts
    params:
      - name: author_name
        type: str
        required: true
        description: "Author display name (partial, case-insensitive)"
        match: "author.display_name"    # dot-path to the MongoDB field
        operator: regex                  # regex (default for str) or eq
      - name: status
        type: str
        required: false
        match: "meta.status"
        operator: eq
    limit: 20                            # default limit, still capped at 100
```

**Param fields:**

| Key | Required | Description |
|-----|----------|-------------|
| `name` | yes | Python parameter name |
| `type` | no | `str` (default) or `int` / `float` |
| `required` | no | `true` (default) or `false`; optional params are only added to the query when provided |
| `match` | no | MongoDB field path to filter on; defaults to the param name |
| `operator` | no | `regex` (default for `str`) or `eq` |
| `description` | no | Appears in the generated docstring and `.md` doc |

---

### `type: aggregate`

Runs a MongoDB aggregation pipeline. Use `{param_name}` anywhere in the pipeline as a placeholder — it becomes a bare Python variable reference in the generated code at the point where the param value is used.

```yaml
  - type: aggregate
    name: orders_by_status
    description: "Count orders grouped by status"
    collection: orders
    params:
      - name: customer_id
        type: int
        required: true
    pipeline:
      - "$match":
          customer_id: "{customer_id}"
      - "$group":
          "_id": "$status"
          count:
            "$sum": 1
      - "$sort":
          count: -1
```

**Placeholder substitution:** `"{customer_id}"` in the YAML (a string whose entire value is `{param_name}`) becomes the bare Python variable `customer_id` in the generated code. This works correctly for any type — strings, integers, lists, whatever the variable holds at runtime.

For embedded substitution within a larger string (e.g. `"prefix_{param}_suffix"`), the generated code uses an f-string.

Returns a list of documents.

---

### `type: lookup`

Two-step cross-collection tool:

1. `find_one` on a source collection using param-driven match
2. Extract a value from the found document (e.g. pluck a list of IDs from a nested array)
3. Run an aggregation pipeline on a second collection using the extracted value

```yaml
  - type: lookup
    name: customer_order_countries
    description: "For a customer, find which countries their orders shipped to"
    params:
      - name: customer_email
        type: str
        required: true
    steps:
      - collection: customers
        find_one: true
        result_name: customer_doc          # variable name for the found document
        match:
          email:
            "$regex": "{customer_email}"
            "$options": "i"
        not_found: "No customer found matching '{customer_email}'"
        extract:
          order_ids:                       # extracted variable name
            path: "orders"                 # dot-path to navigate in the document
            field: "id"                    # field to pluck from each list item

      - collection: orders
        pipeline:
          - "$match":
              "_id":
                "$in": "{order_ids}"       # references the extracted var
          - "$group":
              "_id": "$shipping.country"
              count:
                "$sum": 1
          - "$sort":
              count: -1
        return_as: shipping_countries      # variable name for pipeline results
```

**Step 1 — `find_one` keys:**

| Key | Required | Description |
|-----|----------|-------------|
| `collection` | yes | Collection to query |
| `find_one` | yes | Must be `true` |
| `result_name` | no | Python variable name for the found doc (default: `{collection}_doc`) |
| `match` | yes | MongoDB filter dict; use `{param}` placeholders |
| `not_found` | no | Error message if `find_one` returns nothing; supports `{param}` |
| `extract` | no | Map of variable names to path/field specs (see below) |

**`extract` path resolution:**

`path: "orders"` with `field: "id"` generates:
```python
order_ids = [_item.get("id") for _item in (customer_doc.get("orders") or [])
             if _item.get("id") is not None]
```

Dot-separated paths navigate nested dicts: `path: "billing.addresses"` generates
`customer_doc.get("billing", {}).get("addresses", [])`. Missing keys and nulls are handled gracefully.

**Step 2 — pipeline keys:**

| Key | Required | Description |
|-----|----------|-------------|
| `collection` | yes | Collection to aggregate |
| `pipeline` | yes | MongoDB pipeline; use `{var}` for any variable from previous steps |
| `return_as` | no | Python variable name for results (default: `results_{i}`) |

**Return value:** the generated function returns a dict containing all named outputs — `result_name` doc, all `extract` variable names, and all `return_as` values.

---

### `type: raw`

Pastes a Python `@mcp.tool()` function verbatim into the generated server. Use this for anything too complex to express with the structured types.

```yaml
  - type: raw
    name: my_complex_tool
    code: |
      @mcp.tool()
      def my_complex_tool(param: str) -> dict:
          """Does something that needs custom logic."""
          pipeline = [
              {"$match": {"field": param}},
              {"$lookup": {
                  "from": "other_collection",
                  "localField": "ref_id",
                  "foreignField": "_id",
                  "as": "joined",
              }},
          ]
          rows = list(db["collection"].aggregate(pipeline))
          return {"results": [to_json(r) for r in rows]}
```

The code block has access to everything in the generated server's global scope: `db`, `to_json`, `Optional`, `json`, `ObjectId`, and `mcp`.

---

## Generated Markdown doc

Alongside every `.py` output the generator writes a `.md` file (same base name) containing:

- Database name, generation date
- Per-collection schema table (field names, types, presence %)
- Full tool listing with all parameters and descriptions
- Custom tools section (if `--config` was supplied)

This document is useful as a reference for anyone building prompts or agents on top of the server.

---

## Running the generated server

```bash
python server.py
```

The server runs on stdio by default (MCP standard for Claude Desktop, Cursor, etc.). It connects to MongoDB on startup using the URI baked in at generation time.

To change the MongoDB URI or database after generation, edit `MONGO_URI` and `DB_NAME` at the top of the generated file, or regenerate with new `--uri` / `--db` values.

---

## Updating after schema changes

The generated server reflects the schema at the time of generation. If your collections grow new fields, re-run the generator with the same arguments to pick them up. Any `--config` custom tools are preserved across regeneration since they come from the YAML file, not the database sample.
