# SQLite SKU index utilities for Atomfire Shopify worker
# Functions: init_schema, build_index, is_unique
# The index is an in‑memory SQLite table mapping SKU (upper‑cased) to a set of variant IDs.
# It must reflect the snapshot's products (including active, draft, archived) and any SKU aliases.
# snapshot.products is a tuple of product dicts; each product dict contains a "variants" key
# with a nested "nodes" list of variant dicts. Each variant dict has "id" and "sku" fields.
# snapshot.sku_to_variant_ids is a dict mapping SKU (as stored) to a tuple of variant IDs.
# We treat both sources as authoritative – the snapshot already deduplicates aliases.
# The index will store one row per (sku, variant_id) pair.

import sqlite3
from typing import Tuple, Dict, Any


def init_schema(conn: sqlite3.Connection) -> None:
    """Create the sku_index table.

    Columns:
      sku TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      PRIMARY KEY (sku, variant_id)
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sku_index (
            sku TEXT NOT NULL,
            variant_id TEXT NOT NULL,
            PRIMARY KEY (sku, variant_id)
        )
        """
    )
    conn.commit()


def _add_pair(conn: sqlite3.Connection, sku: str, variant_id: str) -> None:
    """Insert a (sku, variant_id) pair, ignoring duplicates."""
    conn.execute(
        "INSERT OR IGNORE INTO sku_index (sku, variant_id) VALUES (?, ?)",
        (sku.upper(), variant_id),
    )


def build_index(conn: sqlite3.Connection, snapshot: Any) -> None:
    """Populate the in‑memory index from a ShopifySnapshot.

    The function walks snapshot.products and snapshot.sku_to_variant_ids.
    It handles the three status scopes (active, draft, archived) automatically –
    the snapshot already contains the merged view.
    """
    # Clear any existing rows (in‑memory DB may be reused)
    conn.execute("DELETE FROM sku_index")
    # Walk products array
    for product in getattr(snapshot, "products", []):
        # Expected structure: product dict with "variants" -> {"nodes": [{"id":...,"sku":...}, ...]}
        variants = product.get("variants", {}).get("nodes", [])
        for var in variants:
            var_id = var.get("id")
            sku = var.get("sku")
            if isinstance(sku, str) and sku and isinstance(var_id, str):
                _add_pair(conn, sku, var_id)
    # Walk explicit sku_to_variant_ids mapping (covers aliases)
    mapping: Dict[str, Tuple[str, ...]] = getattr(snapshot, "sku_to_variant_ids", {})
    for sku, vids in mapping.items():
        if isinstance(sku, str) and sku:
            for vid in vids:
                if isinstance(vid, str):
                    _add_pair(conn, sku, vid)
    conn.commit()


def is_unique(conn: sqlite3.Connection, sku: str, variant_id: str) -> bool:
    """Return True iff *exactly* the supplied variant_id is the sole entry for sku.

    The check is case‑insensitive on sku (stored upper‑cased). If no rows for sku exist
    the function returns False (the SKU is not present). If multiple variant_ids are
    associated with the sku the function also returns False.
    """
    if not isinstance(sku, str) or not isinstance(variant_id, str):
        return False
    cur = conn.cursor()
    cur.execute(
        "SELECT variant_id FROM sku_index WHERE sku = ?",
        (sku.upper(),),
    )
    rows = [r[0] for r in cur.fetchall()]
    return rows == [variant_id]
