"""
Tests for mongo-mcp-gen (generate.py).

Covers:
  - _safe_name / _safe_param
  - _infer_type
  - build_schema  (pymongo mocked)
  - _to_python_expr
  - render_find_tool / render_get_by_id_tool / render_count_tool / render_sample_tool
  - _extract_nested
  - _render_custom_find / _render_custom_aggregate / _render_custom_lookup / _render_raw_tool
  - render_custom_tools
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from bson import ObjectId

from generate import (
    CollectionSchema,
    FieldSchema,
    _extract_nested,
    _infer_type,
    _render_custom_aggregate,
    _render_custom_find,
    _render_custom_lookup,
    _render_raw_tool,
    _safe_name,
    _safe_param,
    _to_python_expr,
    build_schema,
    render_count_tool,
    render_custom_tools,
    render_find_tool,
    render_get_by_id_tool,
    render_sample_tool,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def code(lines: list) -> str:
    return "\n".join(lines)


def make_schema(
    name="users",
    string_fields=None,
    numeric_fields=None,
    int_id_field=None,
    has_objectid_id=False,
) -> CollectionSchema:
    if string_fields is None:
        string_fields = [FieldSchema("name", "str", 1.0, "name")]
    if numeric_fields is None:
        numeric_fields = []
    return CollectionSchema(
        name=name,
        safe_name=_safe_name(name),
        fields=string_fields + numeric_fields,
        string_fields=string_fields,
        numeric_fields=numeric_fields,
        int_id_field=int_id_field,
        has_objectid_id=has_objectid_id,
        doc_count_estimate=100,
        sample_count=10,
    )


def make_mock_coll(name: str, docs: list, doc_count: int = 100) -> MagicMock:
    coll = MagicMock()
    coll.name = name
    coll.aggregate.return_value = docs
    coll.estimated_document_count.return_value = doc_count
    return coll


# ── _safe_name ────────────────────────────────────────────────────────────────

class TestSafeName:
    def test_simple(self):
        assert _safe_name("users") == "users"

    def test_uppercase(self):
        assert _safe_name("MyCollection") == "mycollection"

    def test_hyphens(self):
        assert _safe_name("my-collection") == "my_collection"

    def test_leading_digit(self):
        assert _safe_name("123items").startswith("col_")

    def test_empty(self):
        assert _safe_name("") == "collection"

    def test_all_non_word(self):
        # "---" → "_" → strip("_") → "" → "collection"
        assert _safe_name("---") == "collection"

    def test_leading_trailing_underscores(self):
        assert _safe_name("_internal_") == "internal"


# ── _safe_param ───────────────────────────────────────────────────────────────

class TestSafeParam:
    def test_simple(self):
        assert _safe_param("userId") == "userid"

    def test_python_keyword_type(self):
        assert _safe_param("type") == "field_type"

    def test_python_keyword_from(self):
        assert _safe_param("from") == "field_from"

    def test_special_chars(self):
        assert _safe_param("user-id") == "user_id"

    def test_uppercase(self):
        assert _safe_param("HandleText") == "handletext"

    def test_empty(self):
        assert _safe_param("") == "value"


# ── _infer_type ───────────────────────────────────────────────────────────────

class TestInferType:
    def test_str(self):
        assert _infer_type(["a", "b", "c"]) == "str"

    def test_int(self):
        assert _infer_type([1, 2, 3]) == "int"

    def test_float(self):
        assert _infer_type([1.0, 2.0]) == "float"

    def test_bool(self):
        assert _infer_type([True, False]) == "bool"

    def test_empty(self):
        assert _infer_type([]) == "mixed"

    def test_all_none(self):
        assert _infer_type([None, None]) == "mixed"

    def test_dominant(self):
        # 3 ints, 1 str → int wins
        assert _infer_type([1, 2, 3, "x"]) == "int"

    def test_nested_dict(self):
        assert _infer_type([{"a": 1}]) == "nested"

    def test_nested_list(self):
        assert _infer_type([[1, 2]]) == "nested"

    def test_objectid(self):
        assert _infer_type([ObjectId()]) == "ObjectId"

    def test_none_ignored(self):
        # None values are skipped; remaining ints dominate
        assert _infer_type([None, 1, 2, 3]) == "int"


# ── build_schema ──────────────────────────────────────────────────────────────

class TestBuildSchema:
    def test_string_fields_detected(self):
        docs = [{"ID": i, "Name": f"user{i}", "Email": f"u{i}@x.com"} for i in range(10)]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        names = [f.name for f in schema.string_fields]
        assert "Name" in names
        assert "Email" in names

    def test_int_id_detected(self):
        docs = [{"ID": i, "Name": f"user{i}"} for i in range(10)]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        assert schema.int_id_field == "ID"

    def test_id_excluded_from_numeric_fields(self):
        docs = [{"ID": i, "score": i * 10} for i in range(10)]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        assert schema.int_id_field == "ID"
        assert "ID" not in [f.name for f in schema.numeric_fields]

    def test_string_fields_capped_at_5(self):
        docs = [
            {"ID": i, "a": "x", "b": "x", "c": "x", "d": "x", "e": "x", "f": "x"}
            for i in range(10)
        ]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        assert len(schema.string_fields) <= 5

    def test_numeric_fields_capped_at_3(self):
        docs = [{"ID": i, "s1": i, "s2": i, "s3": i, "s4": i} for i in range(10)]
        coll = make_mock_coll("nums", docs)
        schema = build_schema(coll, 10)
        assert len(schema.numeric_fields) <= 3

    def test_low_presence_excluded_from_string_fields(self):
        # field "rare" appears in 0 of 10 docs (0% presence < 10% threshold)
        docs = [{"ID": i, "Name": f"n{i}"} for i in range(10)]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        assert "rare" not in [f.name for f in schema.string_fields]

    def test_doc_count_estimate(self):
        docs = [{"ID": i} for i in range(5)]
        coll = make_mock_coll("items", docs, doc_count=9999)
        schema = build_schema(coll, 5)
        assert schema.doc_count_estimate == 9999

    def test_sample_count(self):
        docs = [{"ID": i} for i in range(7)]
        coll = make_mock_coll("items", docs)
        schema = build_schema(coll, 50)
        assert schema.sample_count == 7

    def test_objectid_id_detected(self):
        oid = ObjectId()
        docs = [{"_id": oid, "Name": f"item{i}"} for i in range(5)]
        coll = make_mock_coll("items", docs)
        schema = build_schema(coll, 5)
        assert schema.has_objectid_id is True

    def test_no_int_id_when_absent(self):
        docs = [{"Name": f"user{i}", "score": i} for i in range(10)]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        assert schema.int_id_field is None

    def test_id_field_trailing(self):
        # "userID" ending in ID with ≥90% presence should be detected
        docs = [{"userID": i, "Name": f"n{i}"} for i in range(10)]
        coll = make_mock_coll("users", docs)
        schema = build_schema(coll, 10)
        assert schema.int_id_field == "userID"

    def test_fallback_to_find_on_aggregate_error(self):
        docs = [{"ID": i} for i in range(5)]
        coll = MagicMock()
        coll.name = "users"
        coll.aggregate.side_effect = Exception("unsupported")
        coll.find.return_value.limit.return_value = docs
        coll.estimated_document_count.return_value = 5
        schema = build_schema(coll, 5)
        assert schema.sample_count == 5

    def test_objectid_id_field_excluded_from_fields_list(self):
        # _id should not appear in schema.fields (it's excluded)
        oid = ObjectId()
        docs = [{"_id": oid, "Name": f"item{i}"} for i in range(5)]
        coll = make_mock_coll("items", docs)
        schema = build_schema(coll, 5)
        assert "_id" not in [f.name for f in schema.fields]


# ── _to_python_expr ───────────────────────────────────────────────────────────

class TestToPythonExpr:
    def test_exact_placeholder(self):
        assert _to_python_expr("{customer_id}", {"customer_id"}) == "customer_id"

    def test_exact_placeholder_not_in_params(self):
        # Not a known param → treated as a literal string
        result = _to_python_expr("{unknown}", {"customer_id"})
        assert result == "'{unknown}'"

    def test_embedded_placeholder(self):
        result = _to_python_expr("prefix_{name}_suffix", {"name"})
        assert result.startswith('f"')
        assert "{name}" in result

    def test_plain_string(self):
        assert _to_python_expr("hello", set()) == "'hello'"

    def test_dict_with_placeholder(self):
        result = _to_python_expr({"key": "{val}"}, {"val"})
        assert "'key'" in result
        assert "val" in result
        assert '"{val}"' not in result  # should be bare variable

    def test_list_with_placeholder(self):
        result = _to_python_expr(["{x}", "literal"], {"x"})
        assert "x" in result
        assert "'literal'" in result

    def test_bool_true(self):
        assert _to_python_expr(True, set()) == "True"

    def test_bool_false(self):
        assert _to_python_expr(False, set()) == "False"

    def test_none(self):
        assert _to_python_expr(None, set()) == "None"

    def test_int(self):
        assert _to_python_expr(42, set()) == "42"

    def test_nested_dict(self):
        result = _to_python_expr({"$match": {"field": "{p}"}}, {"p"})
        assert "'$match'" in result
        assert "p" in result

    def test_nested_list_of_dicts(self):
        result = _to_python_expr([{"$match": {"x": "{p}"}}], {"p"})
        assert "'$match'" in result
        assert "p" in result


# ── render_find_tool ──────────────────────────────────────────────────────────

class TestRenderFindTool:
    def test_function_name(self):
        schema = make_schema()
        c = code(render_find_tool(schema))
        assert "def find_users(" in c

    def test_string_param_in_signature(self):
        schema = make_schema(string_fields=[FieldSchema("Name", "str", 1.0, "name")])
        c = code(render_find_tool(schema))
        assert "name: Optional[str] = None" in c

    def test_numeric_param_in_signature(self):
        schema = make_schema(
            string_fields=[],
            numeric_fields=[FieldSchema("score", "int", 1.0, "score")],
        )
        c = code(render_find_tool(schema))
        assert "score: Optional[int] = None" in c

    def test_filter_json_in_signature(self):
        schema = make_schema()
        c = code(render_find_tool(schema))
        assert "filter_json: Optional[str] = None" in c

    def test_regex_query_built(self):
        schema = make_schema(string_fields=[FieldSchema("Name", "str", 1.0, "name")])
        c = code(render_find_tool(schema))
        assert '"$regex"' in c
        assert '"$options": "i"' in c

    def test_equality_query_built(self):
        schema = make_schema(
            string_fields=[],
            numeric_fields=[FieldSchema("score", "int", 1.0, "score")],
        )
        c = code(render_find_tool(schema))
        assert 'query["score"] = score' in c

    def test_filter_json_handling(self):
        schema = make_schema()
        c = code(render_find_tool(schema))
        assert "json.loads(filter_json)" in c
        assert "query.update(" in c

    def test_limit_capped(self):
        schema = make_schema()
        c = code(render_find_tool(schema))
        assert "min(limit, 100)" in c

    def test_returns_count_and_results(self):
        schema = make_schema()
        c = code(render_find_tool(schema))
        assert '"count"' in c
        assert '"results"' in c

    def test_decorator(self):
        schema = make_schema()
        c = code(render_find_tool(schema))
        assert "@mcp.tool()" in c


# ── render_get_by_id_tool ─────────────────────────────────────────────────────

class TestRenderGetByIdTool:
    def test_int_id_signature(self):
        schema = make_schema(int_id_field="ID")
        c = code(render_get_by_id_tool(schema))
        assert "def get_users_by_id(id: int)" in c

    def test_int_id_query(self):
        schema = make_schema(int_id_field="ID")
        c = code(render_get_by_id_tool(schema))
        assert '"ID": id' in c

    def test_objectid_signature(self):
        schema = make_schema(has_objectid_id=True)
        c = code(render_get_by_id_tool(schema))
        assert "def get_users_by_id(id: str)" in c

    def test_objectid_conversion(self):
        schema = make_schema(has_objectid_id=True)
        c = code(render_get_by_id_tool(schema))
        assert "ObjectId(id)" in c

    def test_no_id_returns_empty(self):
        schema = make_schema()
        assert render_get_by_id_tool(schema) == []

    def test_int_id_takes_precedence_over_objectid(self):
        # int_id_field set + has_objectid_id=True → int path wins
        schema = make_schema(int_id_field="ID", has_objectid_id=True)
        c = code(render_get_by_id_tool(schema))
        assert "id: int" in c


# ── render_count_tool ─────────────────────────────────────────────────────────

class TestRenderCountTool:
    def test_function_name(self):
        schema = make_schema()
        c = code(render_count_tool(schema))
        assert "def count_users(" in c

    def test_uses_count_documents(self):
        schema = make_schema()
        c = code(render_count_tool(schema))
        assert "count_documents(query)" in c

    def test_no_find_call(self):
        schema = make_schema()
        c = code(render_count_tool(schema))
        assert ".find(" not in c

    def test_returns_count(self):
        schema = make_schema()
        c = code(render_count_tool(schema))
        assert '"count"' in c

    def test_filter_json_in_signature(self):
        schema = make_schema()
        c = code(render_count_tool(schema))
        assert "filter_json: Optional[str] = None" in c


# ── render_sample_tool ────────────────────────────────────────────────────────

class TestRenderSampleTool:
    def test_function_name(self):
        schema = make_schema()
        c = code(render_sample_tool(schema))
        assert "def sample_users(n: int = 5)" in c

    def test_sample_stage(self):
        schema = make_schema()
        c = code(render_sample_tool(schema))
        assert "$sample" in c

    def test_n_capped_at_20(self):
        schema = make_schema()
        c = code(render_sample_tool(schema))
        assert "min(n, 20)" in c


# ── _extract_nested ───────────────────────────────────────────────────────────

class TestExtractNested:
    def test_single_level_with_field(self):
        expr = _extract_nested("doc", "items", "id")
        assert 'doc.get("items")' in expr
        assert '_item.get("id")' in expr

    def test_multi_level_with_field(self):
        expr = _extract_nested("bbs_doc", "Group.Group", "Name")
        assert 'bbs_doc.get("Group")' in expr
        assert '.get("Group")' in expr
        assert '_item.get("Name")' in expr

    def test_single_level_no_field(self):
        expr = _extract_nested("doc", "orders", None)
        assert 'doc.get("orders")' in expr
        assert "_item" not in expr

    def test_result_is_list_comprehension_with_field(self):
        expr = _extract_nested("doc", "items", "id")
        assert "for _item in" in expr
        assert "is not None" in expr


# ── _render_custom_find ───────────────────────────────────────────────────────

class TestRenderCustomFind:
    def test_required_regex_param(self):
        tool = {
            "name": "find_by_name",
            "collection": "users",
            "params": [{"name": "username", "type": "str", "required": True}],
        }
        c = code(_render_custom_find(tool))
        assert "def find_by_name(username: str" in c
        assert '"$regex": username' in c
        assert "if username is not None:" not in c

    def test_optional_param_gets_none_check(self):
        tool = {
            "name": "find_by_name",
            "collection": "users",
            "params": [{"name": "username", "type": "str", "required": False}],
        }
        c = code(_render_custom_find(tool))
        assert "if username is not None:" in c

    def test_eq_operator(self):
        tool = {
            "name": "find_by_status",
            "collection": "orders",
            "params": [{"name": "status", "type": "str", "operator": "eq", "required": True}],
        }
        c = code(_render_custom_find(tool))
        assert '"$regex"' not in c
        assert 'query["status"] = status' in c

    def test_dot_path_match(self):
        tool = {
            "name": "find_nested",
            "collection": "docs",
            "params": [{"name": "author", "match": "meta.author", "required": True}],
        }
        c = code(_render_custom_find(tool))
        assert '"meta.author"' in c

    def test_custom_limit_default(self):
        tool = {
            "name": "find_things",
            "collection": "things",
            "params": [],
            "limit": 50,
        }
        c = code(_render_custom_find(tool))
        assert "limit: int = 50" in c

    def test_returns_count_and_results(self):
        tool = {"name": "find_x", "collection": "x", "params": []}
        c = code(_render_custom_find(tool))
        assert '"count"' in c
        assert '"results"' in c

    def test_int_param_defaults_to_eq(self):
        tool = {
            "name": "find_by_age",
            "collection": "users",
            "params": [{"name": "age", "type": "int", "required": True}],
        }
        c = code(_render_custom_find(tool))
        assert '"$regex"' not in c
        assert 'query["age"] = age' in c


# ── _render_custom_aggregate ──────────────────────────────────────────────────

class TestRenderCustomAggregate:
    def test_function_name_and_signature(self):
        tool = {
            "name": "orders_by_customer",
            "collection": "orders",
            "params": [{"name": "customer_id", "type": "int"}],
            "pipeline": [{"$match": {"customer_id": "{customer_id}"}}],
        }
        c = code(_render_custom_aggregate(tool))
        assert "def orders_by_customer(customer_id: int)" in c

    def test_param_placeholder_becomes_variable(self):
        tool = {
            "name": "orders_by_customer",
            "collection": "orders",
            "params": [{"name": "customer_id", "type": "int"}],
            "pipeline": [{"$match": {"customer_id": "{customer_id}"}}],
        }
        c = code(_render_custom_aggregate(tool))
        # The placeholder should appear as a bare Python variable
        assert '"{customer_id}"' not in c
        assert "customer_id" in c

    def test_returns_list(self):
        tool = {
            "name": "agg_tool",
            "collection": "items",
            "params": [],
            "pipeline": [{"$group": {"_id": "$type"}}],
        }
        c = code(_render_custom_aggregate(tool))
        assert "-> list:" in c

    def test_aggregate_call(self):
        tool = {
            "name": "agg_tool",
            "collection": "items",
            "params": [],
            "pipeline": [],
        }
        c = code(_render_custom_aggregate(tool))
        assert "aggregate(pipeline)" in c

    def test_collection_name(self):
        tool = {
            "name": "agg_tool",
            "collection": "my_collection",
            "params": [],
            "pipeline": [],
        }
        c = code(_render_custom_aggregate(tool))
        assert '"my_collection"' in c


# ── _render_custom_lookup ─────────────────────────────────────────────────────

class TestRenderCustomLookup:
    def _make_simple_lookup(self):
        return {
            "name": "customer_orders",
            "params": [{"name": "email", "type": "str"}],
            "steps": [
                {
                    "collection": "customers",
                    "find_one": True,
                    "result_name": "customer_doc",
                    "match": {"email": {"$regex": "{email}", "$options": "i"}},
                    "not_found": "No customer: {email}",
                }
            ],
        }

    def test_find_one_call(self):
        c = code(_render_custom_lookup(self._make_simple_lookup()))
        assert "find_one(" in c

    def test_result_var_name(self):
        c = code(_render_custom_lookup(self._make_simple_lookup()))
        assert "customer_doc" in c

    def test_not_found_with_param_becomes_fstring(self):
        c = code(_render_custom_lookup(self._make_simple_lookup()))
        assert 'f"No customer: {email}"' in c

    def test_extract_generates_list_comprehension(self):
        tool = {
            "name": "lookup_tool",
            "params": [{"name": "bbs_name", "type": "str"}],
            "steps": [
                {
                    "collection": "bbses",
                    "find_one": True,
                    "result_name": "bbs_doc",
                    "match": {"Name": {"$regex": "{bbs_name}", "$options": "i"}},
                    "extract": {
                        "group_names": {"path": "Group.Group", "field": "Name"}
                    },
                },
            ],
        }
        c = code(_render_custom_lookup(tool))
        assert "group_names" in c
        assert "for _item in" in c

    def test_pipeline_step_returns_named_var(self):
        tool = {
            "name": "lookup_tool",
            "params": [{"name": "bbs_name", "type": "str"}],
            "steps": [
                {
                    "collection": "bbses",
                    "find_one": True,
                    "result_name": "bbs_doc",
                    "match": {"Name": {"$regex": "{bbs_name}", "$options": "i"}},
                    "extract": {
                        "group_names": {"path": "Group.Group", "field": "Name"}
                    },
                },
                {
                    "collection": "handles",
                    "pipeline": [
                        {"$match": {"MemberOf.Group.Name": {"$in": "{group_names}"}}}
                    ],
                    "return_as": "members",
                },
            ],
        }
        c = code(_render_custom_lookup(tool))
        assert "members" in c
        assert '"bbs_doc": bbs_doc' in c
        assert '"members": members' in c

    def test_extracted_var_used_in_pipeline(self):
        tool = {
            "name": "lookup_tool",
            "params": [{"name": "bbs_name", "type": "str"}],
            "steps": [
                {
                    "collection": "bbses",
                    "find_one": True,
                    "result_name": "bbs_doc",
                    "match": {"Name": {"$regex": "{bbs_name}", "$options": "i"}},
                    "extract": {
                        "group_names": {"path": "Group.Group", "field": "Name"}
                    },
                },
                {
                    "collection": "handles",
                    "pipeline": [
                        {"$match": {"MemberOf.Group.Name": {"$in": "{group_names}"}}}
                    ],
                    "return_as": "members",
                },
            ],
        }
        c = code(_render_custom_lookup(tool))
        # group_names should appear as a bare variable in the pipeline, not as a string
        assert '"{group_names}"' not in c
        assert "group_names" in c


# ── _render_raw_tool ──────────────────────────────────────────────────────────

class TestRenderRawTool:
    def test_code_pasted_verbatim(self):
        tool = {
            "name": "my_tool",
            "code": "@mcp.tool()\ndef my_tool() -> dict:\n    return {}\n",
        }
        c = code(_render_raw_tool(tool))
        assert "@mcp.tool()" in c
        assert "def my_tool()" in c

    def test_code_is_dedented(self):
        tool = {
            "name": "my_tool",
            "code": "    @mcp.tool()\n    def my_tool() -> dict:\n        return {}\n",
        }
        c = code(_render_raw_tool(tool))
        # After dedent, decorator should have no leading spaces
        for line in c.splitlines():
            if "@mcp.tool()" in line:
                assert not line.startswith(" ")

    def test_empty_code(self):
        tool = {"name": "empty", "code": ""}
        lines = _render_raw_tool(tool)
        assert isinstance(lines, list)


# ── render_custom_tools ───────────────────────────────────────────────────────

class TestRenderCustomTools:
    def test_empty_config(self):
        assert render_custom_tools({}) == []

    def test_empty_tools_list(self):
        assert render_custom_tools({"tools": []}) == []

    def test_unknown_type_warns(self, capsys):
        config = {"tools": [{"type": "unknown_type", "name": "bad_tool"}]}
        render_custom_tools(config)
        captured = capsys.readouterr()
        assert "unknown tool type" in captured.err

    def test_find_tool_rendered(self):
        config = {
            "tools": [
                {
                    "type": "find",
                    "name": "my_find",
                    "collection": "things",
                    "params": [{"name": "title", "type": "str", "required": True}],
                }
            ]
        }
        lines = render_custom_tools(config)
        c = code(lines)
        assert "def my_find(" in c

    def test_aggregate_tool_rendered(self):
        config = {
            "tools": [
                {
                    "type": "aggregate",
                    "name": "my_agg",
                    "collection": "things",
                    "params": [],
                    "pipeline": [],
                }
            ]
        }
        lines = render_custom_tools(config)
        c = code(lines)
        assert "def my_agg(" in c

    def test_raw_tool_rendered(self):
        config = {
            "tools": [
                {
                    "type": "raw",
                    "name": "my_raw",
                    "code": "@mcp.tool()\ndef my_raw() -> dict:\n    return {}\n",
                }
            ]
        }
        lines = render_custom_tools(config)
        c = code(lines)
        assert "def my_raw()" in c
