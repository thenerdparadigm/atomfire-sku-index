import sqlite3
from pathlib import Path

# Import the functions from the dev workspace package path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from sku_index import init_schema, build_index, is_unique

# Minimal mock snapshot matching expected attributes
class MockSnapshot:
    def __init__(self, products, sku_to_variant_ids):
        self.products = products
        self.sku_to_variant_ids = sku_to_variant_ids
        # In original code retrieved_all_statuses is checked elsewhere; tests assume True
        self.retrieved_all_statuses = True


def test_basic_unique():
    # One product with a single variant
    prod = {"variants": {"nodes": [{"id": "var1", "sku": "ABC-123"}]}}
    snap = MockSnapshot(products=[prod], sku_to_variant_ids={})
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    build_index(conn, snap)
    assert is_unique(conn, "ABC-123", "var1")


def test_duplicate_variant():
    # Two variants share same SKU -> not unique
    prod = {"variants": {"nodes": [
        {"id": "var1", "sku": "XYZ"},
        {"id": "var2", "sku": "XYZ"}
    ]}}
    snap = MockSnapshot(products=[prod], sku_to_variant_ids={})
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    build_index(conn, snap)
    assert not is_unique(conn, "XYZ", "var1")
    assert not is_unique(conn, "XYZ", "var2")


def test_alias_mapping():
    # No variant records, but explicit alias map provides association
    prod = {"variants": {"nodes": []}}
    snap = MockSnapshot(products=[prod], sku_to_variant_ids={"ALIAS": ("varA",)})
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    build_index(conn, snap)
    assert is_unique(conn, "ALIAS", "varA")


def test_missing_sku():
    prod = {"variants": {"nodes": [{"id": "v1", "sku": None}]}}
    snap = MockSnapshot(products=[prod], sku_to_variant_ids={})
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    build_index(conn, snap)
    assert not is_unique(conn, "ANY", "v1")


def test_case_insensitivity():
    prod = {"variants": {"nodes": [{"id": "v1", "sku": "lower"}]}}
    snap = MockSnapshot(products=[prod], sku_to_variant_ids={})
    conn = sqlite3.connect(":memory:")
    init_schema(conn)
    build_index(conn, snap)
    assert is_unique(conn, "LOWER", "v1")
    assert is_unique(conn, "lower", "v1")
