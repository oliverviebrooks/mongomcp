#!/usr/bin/env python3
"""
mongo-mcp-gen: Introspect a MongoDB database and emit a ready-to-run
FastMCP server plus a Markdown reference document.

Usage:
    python generate.py --uri mongodb://localhost:27017 --db mydb
    python generate.py --uri mongodb://host:27017 --db mydb \\
        --name mydb-mcp --output server.py --exclude logs audit tmp
"""

import argparse
import json
import re
import sys
import textwrap
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pymongo
import yaml
from bson import ObjectId


# ── Data structures ───────────────────────────────────────────────────────────

PYTHON_KEYWORDS = frozenset(
    ["and", "as", "assert", "async", "await", "break", "class", "continue",
     "def", "del", "elif", "else", "except", "finally", "for", "from",
     "global", "if", "import", "in", "is", "lambda", "nonlocal", "not",
     "or", "pass", "raise", "return", "try", "type", "while", "with", "yield"]
)


@dataclass
class FieldSchema:
    name: str         # raw MongoDB field name
    py_type: str      # "str" | "int" | "float" | "bool" | "ObjectId" | "mixed"
    presence: float   # fraction of sampled docs that have this field
    param_name: str   # safe Python parameter name derived from name


@dataclass
class CollectionSchema:
    name: str                           # raw collection name
    safe_name: str                      # valid Python identifier
    fields: list                        # all FieldSchema, sorted by presence desc
    string_fields: list                 # up to 5 str fields for regex params
    numeric_fields: list                # up to 3 int/float fields for equality params
    int_id_field: str | None            # integer domain ID field name, or None
    has_objectid_id: bool               # True if _id is ObjectId
    doc_count_estimate: int
    sample_count: int


# ── Introspection ─────────────────────────────────────────────────────────────

def _safe_name(name: str) -> str:
    s = re.sub(r"\W+", "_", name).lower().strip("_")
    if s and s[0].isdigit():
        s = "col_" + s
    return s or "collection"


def _safe_param(field_name: str) -> str:
    s = re.sub(r"\W+", "_", field_name).lower().strip("_")
    if s in PYTHON_KEYWORDS:
        s = "field_" + s
    return s or "value"


def _infer_type(values: list) -> str:
    counts: Counter = Counter()
    for v in values:
        if v is None:
            continue
        elif isinstance(v, bool):
            counts["bool"] += 1
        elif isinstance(v, ObjectId):
            counts["ObjectId"] += 1
        elif isinstance(v, int):
            counts["int"] += 1
        elif isinstance(v, float):
            counts["float"] += 1
        elif isinstance(v, str):
            counts["str"] += 1
        elif isinstance(v, (dict, list)):
            counts["nested"] += 1
        else:
            counts["mixed"] += 1
    if not counts:
        return "mixed"
    dominant = counts.most_common(1)[0][0]
    return dominant


def build_schema(coll: pymongo.collection.Collection, n_samples: int) -> CollectionSchema:
    name = coll.name

    # Sample: try $sample, fall back to find().limit()
    try:
        docs = list(coll.aggregate([{"$sample": {"size": n_samples}}]))
    except Exception:
        docs = list(coll.find().limit(n_samples))

    # Collect field values
    field_values: dict[str, list] = defaultdict(list)
    for doc in docs:
        for k, v in doc.items():
            field_values[k].append(v)

    all_fields = []
    for fname, values in field_values.items():
        py_type = _infer_type(values)
        presence = len(values) / len(docs) if docs else 0.0
        all_fields.append(FieldSchema(
            name=fname,
            py_type=py_type,
            presence=round(presence, 2),
            param_name=_safe_param(fname),
        ))

    all_fields.sort(key=lambda f: -f.presence)

    # String fields: non-_id, str type, presence >= 0.1
    string_fields = [
        f for f in all_fields
        if f.py_type == "str" and f.name != "_id" and f.presence >= 0.1
    ][:5]

    # Numeric fields: non-_id, int/float, presence >= 0.1
    # Also exclude fields that will become the int_id_field
    numeric_fields_raw = [
        f for f in all_fields
        if f.py_type in ("int", "float") and f.name != "_id" and f.presence >= 0.1
    ]

    # Detect integer ID field
    int_id_field = None
    for candidate in ("ID", "id", "Id"):
        match = next((f for f in numeric_fields_raw if f.name == candidate and f.presence >= 0.9), None)
        if match:
            int_id_field = match.name
            break
    if int_id_field is None:
        # Try any field ending in "ID" or "Id" with high presence
        for f in numeric_fields_raw:
            if (f.name.endswith("ID") or f.name.endswith("Id")) and f.presence >= 0.9:
                int_id_field = f.name
                break

    numeric_fields = [
        f for f in numeric_fields_raw if f.name != int_id_field
    ][:3]

    # Check if _id is ObjectId
    id_field = next((f for f in all_fields if f.name == "_id"), None)
    has_objectid_id = id_field is not None and id_field.py_type == "ObjectId"

    try:
        doc_count = coll.estimated_document_count()
    except Exception:
        doc_count = 0

    return CollectionSchema(
        name=name,
        safe_name=_safe_name(name),
        fields=[f for f in all_fields if f.name != "_id"],
        string_fields=string_fields,
        numeric_fields=numeric_fields,
        int_id_field=int_id_field,
        has_objectid_id=has_objectid_id,
        doc_count_estimate=doc_count,
        sample_count=len(docs),
    )


# ── Code generation ───────────────────────────────────────────────────────────

_HEADER_TEMPLATE = textwrap.dedent("""\
    \"\"\"
    {server_name} — auto-generated MCP server for the {db_name} MongoDB database.
    Generated by mongo-mcp-gen on {date}.
    Collections: {collections}
    \"\"\"

    import json
    from typing import Optional

    import pymongo
    from bson import ObjectId, json_util
    from mcp.server.fastmcp import FastMCP

    MONGO_URI = "{uri}"
    DB_NAME = "{db_name}"

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    mcp = FastMCP("{server_name}")


    def to_json(doc) -> dict:
        \"\"\"Serialize a MongoDB document, resolving ObjectId, datetime, etc.\"\"\"
        return json.loads(json_util.dumps(doc))

""")

_FOOTER_TEMPLATE = textwrap.dedent("""\

    def main():
        mcp.run()


    if __name__ == "__main__":
        main()
""")


def _build_find_params(schema: CollectionSchema) -> tuple[str, list[str], list[str]]:
    """Return (signature_string, find_body_lines, count_body_lines)."""
    parts = []
    for f in schema.string_fields:
        parts.append(f"{f.param_name}: Optional[str] = None")
    for f in schema.numeric_fields:
        parts.append(f"{f.param_name}: Optional[{f.py_type}] = None")
    parts.append("limit: int = 10")
    parts.append("filter_json: Optional[str] = None")
    sig = ", ".join(parts)

    # Shared query-building lines (used by both find and count)
    shared = ['    query: dict = {}']
    for f in schema.string_fields:
        shared.append(f'    if {f.param_name} is not None:')
        shared.append(f'        query["{f.name}"] = ' + '{"$regex": ' + f.param_name + ', "$options": "i"}')
    for f in schema.numeric_fields:
        shared.append(f'    if {f.param_name} is not None:')
        shared.append(f'        query["{f.name}"] = {f.param_name}')
    shared += [
        '    if filter_json is not None:',
        '        try:',
        '            extra = json.loads(filter_json)',
        '            query.update({k: v for k, v in extra.items() if k not in query})',
        '        except Exception as e:',
        '            return {"error": f"Invalid filter_json: {e}"}',
    ]

    find_body = shared + [
        f'    docs = list(db["{schema.name}"].find(query).limit(min(limit, 100)))',
        '    return {"count": len(docs), "results": [to_json(d) for d in docs]}',
    ]
    count_body = shared + [
        f'    return ' + '{"count": db["' + schema.name + '"].count_documents(query)}',
    ]

    return sig, find_body, count_body


def render_find_tool(schema: CollectionSchema) -> list[str]:
    sig, find_body, _ = _build_find_params(schema)
    searchable = ", ".join(f.name for f in schema.string_fields) or "none detected"
    lines = [
        "@mcp.tool()",
        f"def find_{schema.safe_name}({sig}) -> dict:",
        f'    """',
        f'    Find documents in {schema.name}.',
        f'    String params use partial, case-insensitive regex matching.',
        f'    Searchable fields: {searchable}',
        f'    Use filter_json to query any field with full MongoDB syntax,',
        f'    e.g. filter_json=\'{{"age": {{"$gt": 25}}}}\' ',
        f'    """',
    ]
    lines += find_body
    lines.append("")
    return lines


def render_get_by_id_tool(schema: CollectionSchema) -> list[str]:
    if schema.int_id_field:
        field_name = schema.int_id_field
        lines = [
            "@mcp.tool()",
            f"def get_{schema.safe_name}_by_id(id: int) -> dict:",
            f'    """Get a single {schema.name} document by its {field_name} field."""',
            f'    doc = db["{schema.name}"].find_one(' + '{' + f'"{field_name}": id' + '})',
            f'    return to_json(doc) if doc else ' + '{"error": f"No ' + schema.name + ' with ' + field_name + ' {id}"}',
            "",
        ]
        return lines

    if schema.has_objectid_id:
        lines = [
            "@mcp.tool()",
            f"def get_{schema.safe_name}_by_id(id: str) -> dict:",
            f'    """Get a single {schema.name} document by its _id (ObjectId as hex string)."""',
            '    try:',
            '        oid = ObjectId(id)',
            '    except Exception:',
            '        return {"error": f"Invalid ObjectId: {id}"}',
            f'    doc = db["{schema.name}"].find_one(' + '{"_id": oid})',
            '    return to_json(doc) if doc else ' + '{"error": f"No document with _id {id}"}',
            "",
        ]
        return lines

    return []


def render_count_tool(schema: CollectionSchema) -> list[str]:
    sig, _, count_body = _build_find_params(schema)
    lines = [
        "@mcp.tool()",
        f"def count_{schema.safe_name}({sig}) -> dict:",
        f'    """Count documents in {schema.name} with optional filters (same params as find_{schema.safe_name})."""',
    ]
    lines += count_body
    lines.append("")
    return lines


def render_sample_tool(schema: CollectionSchema) -> list[str]:
    return [
        "@mcp.tool()",
        f"def sample_{schema.safe_name}(n: int = 5) -> list:",
        f'    """Return a random sample of {schema.name} documents. Useful for schema discovery."""',
        f'    docs = list(db["{schema.name}"].aggregate([' + '{"$sample": {"size": min(n, 20)}}]))',
        "    return [to_json(d) for d in docs]",
        "",
    ]


def render_collection(schema: CollectionSchema) -> list[str]:
    bar = "─" * max(0, 70 - len(schema.name))
    lines = [f"# ── {schema.name} {bar}", ""]
    lines += render_find_tool(schema)
    lines += render_get_by_id_tool(schema)
    lines += render_count_tool(schema)
    lines += render_sample_tool(schema)
    lines.append("")
    return lines


# ── Custom tools (from --config) ─────────────────────────────────────────────

def _to_python_expr(value, param_names: set) -> str:
    """
    Recursively convert a YAML-parsed value to a Python expression string.
    Strings that are exactly "{param_name}" become bare variable references.
    Strings containing "{param_name}" become f-strings.
    Everything else becomes its repr.
    """
    if isinstance(value, str):
        # Exact placeholder → bare variable
        if value.startswith("{") and value.endswith("}"):
            inner = value[1:-1]
            if inner in param_names:
                return inner
        # Embedded placeholder(s) → f-string
        if any(f"{{{p}}}" in value for p in param_names):
            escaped = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'f"{escaped}"'
        return repr(value)
    elif isinstance(value, dict):
        pairs = [f"{repr(str(k))}: {_to_python_expr(v, param_names)}" for k, v in value.items()]
        return "{" + ", ".join(pairs) + "}"
    elif isinstance(value, list):
        items = [_to_python_expr(item, param_names) for item in value]
        return "[" + ", ".join(items) + "]"
    elif isinstance(value, bool):
        return "True" if value else "False"
    elif value is None:
        return "None"
    else:
        return repr(value)


def _params_sig(params: list[dict]) -> str:
    """Build a function signature string from a params list."""
    parts = []
    for p in params:
        name = p["name"]
        ptype = p.get("type", "str")
        required = p.get("required", True)
        default = p.get("default")
        if not required or default is not None:
            parts.append(f"{name}: Optional[{ptype}] = {repr(default)}")
        else:
            parts.append(f"{name}: {ptype}")
    return ", ".join(parts)


def _render_custom_find(tool: dict) -> list[str]:
    """
    type: find
    Generates a find tool with explicit match criteria per param.
    Each param specifies which MongoDB field path to filter on and how.
    """
    name = tool["name"]
    desc = tool.get("description", f"Query the {tool['collection']} collection.")
    collection = tool["collection"]
    params = tool.get("params", [])
    limit_default = tool.get("limit", 20)

    sig = _params_sig(params)
    if sig:
        sig += ", limit: int = " + str(limit_default)
    else:
        sig = "limit: int = " + str(limit_default)

    lines = [
        "@mcp.tool()",
        f"def {name}({sig}) -> dict:",
        f'    """{desc}"""',
        "    query: dict = {}",
    ]

    for p in params:
        pname = p["name"]
        field_path = p.get("match", pname)
        op = p.get("operator", "regex" if p.get("type", "str") == "str" else "eq")
        required = p.get("required", True)
        indent = "    " if required else "        "

        if not required:
            lines.append(f"    if {pname} is not None:")

        if op == "regex":
            lines.append(f'{indent}query["{field_path}"] = ' + '{"$regex": ' + pname + ', "$options": "i"}')
        else:
            lines.append(f'{indent}query["{field_path}"] = {pname}')

    lines += [
        f'    docs = list(db["{collection}"].find(query).limit(min(limit, 100)))',
        '    return {"count": len(docs), "results": [to_json(d) for d in docs]}',
        "",
    ]
    return lines


def _render_custom_aggregate(tool: dict) -> list[str]:
    """
    type: aggregate
    Generates a tool that runs a MongoDB aggregation pipeline.
    Use {param_name} in pipeline values as placeholders; they become
    Python variable references at runtime.
    """
    name = tool["name"]
    desc = tool.get("description", f"Aggregate the {tool['collection']} collection.")
    collection = tool["collection"]
    params = tool.get("params", [])
    pipeline = tool.get("pipeline", [])

    param_names = {p["name"] for p in params}
    sig = _params_sig(params)
    pipeline_expr = _to_python_expr(pipeline, param_names)

    lines = [
        "@mcp.tool()",
        f"def {name}({sig}) -> list:",
        f'    """{desc}"""',
        f"    pipeline = {pipeline_expr}",
        f'    rows = list(db["{collection}"].aggregate(pipeline))',
        "    return [to_json(r) for r in rows]",
        "",
    ]
    return lines


def _extract_nested(doc_var: str, path: str, field: str | None) -> str:
    """
    Generate a Python expression that walks a dot-path through a doc dict
    and optionally extracts a named field from each item in the final list.

    path="Group.Group", field="Name" on doc variable "bbs_doc" generates:
        [item.get("Name") for item in
         ((bbs_doc.get("Group") or {}).get("Group") or [])
         if item.get("Name") is not None]
    """
    parts = path.split(".")
    expr = doc_var
    for i, part in enumerate(parts):
        if i == 0:
            expr = f'({expr}.get("{part}") or {{}})'
        else:
            expr = f'({expr}.get("{part}") or [])'
    if field:
        return (f'[_item.get("{field}") for _item in ({expr} or []) '
                f'if _item.get("{field}") is not None]')
    return expr


def _render_custom_lookup(tool: dict) -> list[str]:
    """
    type: lookup
    Two-step cross-collection tool:
      1. find_one from a source collection (with param-driven match)
      2. extract a value from the found doc
      3. run an aggregation pipeline on a target collection using that value

    Config keys per step:
      find_one step: collection, match, not_found, result_name, extract
        extract: {var_name: {path: "dot.path", field: "FieldName"}}
      pipeline step: collection, pipeline, return_as
    """
    name = tool["name"]
    desc = tool.get("description", "Cross-collection lookup.")
    params = tool.get("params", [])
    steps = tool.get("steps", [])

    param_names = {p["name"] for p in params}
    sig = _params_sig(params)

    lines = [
        "@mcp.tool()",
        f"def {name}({sig}) -> dict:",
        f'    """{desc}"""',
    ]

    result_vars: list[str] = []  # variables to include in the final return dict

    for i, step in enumerate(steps):
        collection = step["collection"]

        if step.get("find_one"):
            result_name = step.get("result_name", f"{_safe_name(collection)}_doc")
            match = step.get("match", {})
            match_expr = _to_python_expr(match, param_names)

            not_found = step.get("not_found", f"No {collection} document found.")
            nf_has_param = any(f"{{{p}}}" in not_found for p in param_names)
            nf_repr = f'f"{not_found}"' if nf_has_param else repr(not_found)

            lines += [
                f'    {result_name} = db["{collection}"].find_one({match_expr})',
                f'    if not {result_name}:',
                f'        return ' + '{' + f'"error": {nf_repr}' + '}',
                f'    {result_name} = to_json({result_name})',
            ]

            # Handle extract
            for var_name, extract_def in step.get("extract", {}).items():
                path = extract_def["path"]
                field = extract_def.get("field")
                extract_expr = _extract_nested(result_name, path, field)
                lines.append(f"    {var_name} = {extract_expr}")
                # Make extracted var available for downstream steps
                param_names.add(var_name)
                result_vars.append(var_name)

            result_vars.append(result_name)

        elif "pipeline" in step:
            pipeline = step["pipeline"]
            pipeline_expr = _to_python_expr(pipeline, param_names)
            return_as = step.get("return_as", f"results_{i}")
            lines += [
                f"    pipeline = {pipeline_expr}",
                f'    {return_as} = [to_json(r) for r in db["{collection}"].aggregate(pipeline)]',
            ]
            result_vars.append(return_as)

    # Build return dict from all collected result vars
    ret_pairs = ", ".join(f'"{v}": {v}' for v in result_vars)
    lines += [
        "    return {" + ret_pairs + "}",
        "",
    ]
    return lines


def _render_raw_tool(tool: dict) -> list[str]:
    """
    type: raw
    Pastes the 'code' block verbatim into the generated server.
    Use for anything too complex to express in the structured types.
    """
    code = tool.get("code", "")
    lines = textwrap.dedent(code).splitlines()
    lines.append("")
    return lines


def render_custom_tools(config: dict) -> list[str]:
    """Generate all custom tools from a loaded config dict."""
    tools = config.get("tools", [])
    if not tools:
        return []

    lines = ["# ── Custom tools ─────────────────────────────────────────────────────────────", ""]
    for tool in tools:
        tool_type = tool.get("type", "find")
        try:
            if tool_type == "find":
                lines += _render_custom_find(tool)
            elif tool_type == "aggregate":
                lines += _render_custom_aggregate(tool)
            elif tool_type == "lookup":
                lines += _render_custom_lookup(tool)
            elif tool_type == "raw":
                lines += _render_raw_tool(tool)
            else:
                print(f"Warning: unknown tool type '{tool_type}' for '{tool.get('name')}', skipping",
                      file=sys.stderr)
        except Exception as e:
            print(f"Warning: failed to render tool '{tool.get('name')}': {e}", file=sys.stderr)
    return lines


def _custom_tools_doc(config: dict) -> list[str]:
    """Generate a Markdown section documenting all custom tools."""
    tools = config.get("tools", [])
    if not tools:
        return []

    lines = ["## Custom tools", ""]
    for tool in tools:
        tool_type = tool.get("type", "find")
        name = tool.get("name", "unknown")
        desc = tool.get("description", "")
        params = tool.get("params", [])

        lines.append(f"### `{name}`")
        lines.append("")
        if desc:
            lines.append(desc)
            lines.append("")
        lines.append(f"**Type:** `{tool_type}`")
        lines.append("")

        if params:
            lines += ["**Parameters:**", ""]
            lines += ["| Name | Type | Required | Description |",
                      "|------|------|----------|-------------|"]
            for p in params:
                req = "yes" if p.get("required", True) else "no"
                pdesc = p.get("description", "")
                default = p.get("default")
                if default is not None:
                    pdesc = f"{pdesc} (default: `{default}`)".strip()
                lines.append(f"| `{p['name']}` | {p.get('type','str')} | {req} | {pdesc} |")
            lines.append("")

        if tool_type == "lookup":
            colls = [s.get("collection", "?") for s in tool.get("steps", [])]
            lines.append(f"**Collections queried:** {' → '.join(colls)}")
            lines.append("")
        elif tool_type in ("find", "aggregate"):
            lines.append(f"**Collection:** `{tool.get('collection', '?')}`")
            lines.append("")

    return lines


# ── Server + docs generation ──────────────────────────────────────────────────

def generate_server(
    uri: str,
    db_name: str,
    server_name: str,
    schemas: list[CollectionSchema],
    config: dict | None = None,
) -> str:
    date = datetime.now().strftime("%Y-%m-%d")
    collection_names = ", ".join(s.name for s in schemas)
    header = _HEADER_TEMPLATE.format(
        server_name=server_name,
        db_name=db_name,
        uri=uri,
        date=date,
        collections=collection_names,
    )
    body_lines: list[str] = []
    for schema in schemas:
        body_lines += render_collection(schema)

    if config:
        body_lines += render_custom_tools(config)

    return header + "\n".join(body_lines) + _FOOTER_TEMPLATE


# ── Markdown generation ───────────────────────────────────────────────────────

def generate_docs(
    uri: str,
    db_name: str,
    server_name: str,
    output_file: str,
    schemas: list[CollectionSchema],
    config: dict | None = None,
) -> str:
    date = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"# {server_name}",
        "",
        f"Auto-generated MCP server for the **{db_name}** MongoDB database.  ",
        f"Generated by `mongo-mcp-gen` on {date}.",
        "",
        "## Running",
        "",
        "```bash",
        f"python {output_file}",
        "```",
        "",
        "## Collections",
        "",
    ]

    for schema in schemas:
        lines += [
            f"### `{schema.name}`",
            "",
            f"~{schema.doc_count_estimate:,} documents estimated "
            f"(schema inferred from {schema.sample_count} samples)",
            "",
        ]

        if schema.fields:
            lines += ["| Field | Type | Presence |", "|-------|------|----------|"]
            for f in schema.fields[:20]:  # cap table height
                lines.append(f"| `{f.name}` | {f.py_type} | {f.presence*100:.0f}% |")
            if len(schema.fields) > 20:
                lines.append(f"| *(+ {len(schema.fields) - 20} more fields)* | | |")
            lines.append("")

        lines.append("**Tools:**")
        lines.append("")

        # find
        sig_parts = []
        for f in schema.string_fields:
            sig_parts.append(f"`{f.param_name}: str` (regex)")
        for f in schema.numeric_fields:
            sig_parts.append(f"`{f.param_name}: {f.py_type}` (equality)")
        sig_parts.append("`limit: int` (default 10, max 100)")
        sig_parts.append("`filter_json: str` (any MongoDB filter as JSON)")
        lines.append(f"- **`find_{schema.safe_name}`** — search documents  ")
        lines.append(f"  Params: {', '.join(sig_parts)}  ")
        lines.append(f"  All params optional. String params are partial case-insensitive regex. "
                     f"`filter_json` merges with named params (named params take precedence on key conflicts).")
        lines.append("")

        # get_by_id
        if schema.int_id_field:
            lines.append(f"- **`get_{schema.safe_name}_by_id`** — fetch one document  ")
            lines.append(f"  Params: `id: int` (matches `{schema.int_id_field}` field)")
            lines.append("")
        elif schema.has_objectid_id:
            lines.append(f"- **`get_{schema.safe_name}_by_id`** — fetch one document  ")
            lines.append(f"  Params: `id: str` (24-char hex ObjectId string)")
            lines.append("")

        # count
        lines.append(f"- **`count_{schema.safe_name}`** — count documents with optional filters  ")
        lines.append(f"  Same params as `find_{schema.safe_name}`. Returns `{{\"count\": N}}`.")
        lines.append("")

        # sample
        lines.append(f"- **`sample_{schema.safe_name}`** — random document sample  ")
        lines.append(f"  Params: `n: int` (default 5, max 20). No filters.")
        lines.append("")

    if config:
        lines += _custom_tools_doc(config)

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate a FastMCP server for a MongoDB database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python generate.py --uri mongodb://localhost:27017 --db mydb
              python generate.py --uri mongodb://host:27017 --db csdb \\
                  --name csdb-mcp --output server.py --exclude images files
        """),
    )
    p.add_argument("--uri", required=True, help="MongoDB connection URI")
    p.add_argument("--db", required=True, help="Database name")
    p.add_argument("--name", default=None, help="MCP server name (default: <db>-mcp)")
    p.add_argument("--output", default=None, help="Output .py filename (default: <db>_server.py)")
    p.add_argument("--exclude", nargs="*", default=[], metavar="COLLECTION",
                   help="Collection names to exclude")
    p.add_argument("--samples", type=int, default=50,
                   help="Documents to sample per collection for schema inference (default: 50)")
    p.add_argument("--config", default=None, metavar="FILE",
                   help="YAML config file defining extra custom tools (find/aggregate/lookup/raw)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    server_name = args.name or f"{args.db}-mcp"
    output_file = args.output or f"{args.db}_server.py"
    docs_file = Path(output_file).with_suffix(".md")

    print(f"Connecting to {args.uri} / {args.db} ...")
    try:
        mongo_client = pymongo.MongoClient(args.uri, serverSelectionTimeoutMS=5000)
        mongo_client.server_info()  # force connection
        database = mongo_client[args.db]
    except Exception as e:
        print(f"Error: could not connect to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    all_collections = sorted(database.list_collection_names())
    excluded = set(args.exclude or [])
    collections_to_process = [n for n in all_collections if n not in excluded]

    if not collections_to_process:
        print("Error: no collections to process (all excluded or database is empty).", file=sys.stderr)
        sys.exit(1)

    print(f"Collections: {', '.join(collections_to_process)}")
    if excluded:
        print(f"Excluded: {', '.join(sorted(excluded))}")
    print()

    schemas: list[CollectionSchema] = []
    for coll_name in collections_to_process:
        print(f"  Sampling {coll_name} ...", end=" ", flush=True)
        schema = build_schema(database[coll_name], args.samples)
        print(f"{schema.sample_count} docs, {len(schema.fields)} fields, "
              f"~{schema.doc_count_estimate:,} total")
        schemas.append(schema)

    config: dict = {}
    if args.config:
        try:
            with open(args.config) as f:
                config = yaml.safe_load(f) or {}
            n_tools = len(config.get("tools", []))
            print(f"Config: {args.config} ({n_tools} custom tool(s))")
        except Exception as e:
            print(f"Error: could not load config '{args.config}': {e}", file=sys.stderr)
            sys.exit(1)

    print()
    server_code = generate_server(args.uri, args.db, server_name, schemas, config=config)
    docs_text = generate_docs(args.uri, args.db, server_name, output_file, schemas, config=config)

    Path(output_file).write_text(server_code)
    print(f"Server  → {output_file}")

    Path(docs_file).write_text(docs_text)
    print(f"Docs    → {docs_file}")

    total_tools = sum(
        4 if (s.int_id_field or s.has_objectid_id) else 3
        for s in schemas
    )
    print(f"\nGenerated {total_tools} tools across {len(schemas)} collections.")


if __name__ == "__main__":
    main()
