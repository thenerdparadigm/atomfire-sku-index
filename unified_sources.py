"""Fail-closed, READ-ONLY source adapters for the proposed unified onboarding.

No Shopify writes, supplier HTTP calls, inventory changes, DB migrations, or
checkout decisions. The live CML enrolment worker is not replaced by this module.

All inputs are untrusted until a complete snapshot has been validated.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Mapping
import math
import re
import sqlite3
import json
from unified_onboarding import SupplierProfile, Stock, PROFILES, VALID_STOCK, canonical_supplier_code
from sku_index import init_schema, build_index, is_unique

PRODUCT_GID = re.compile(r"gid://shopify/Product/[0-9]+\Z")
VARIANT_GID = re.compile(r"gid://shopify/ProductVariant/[0-9]+\Z")
MAX_PRODUCTS = 1500
MAX_VARIANTS = 3000
MAX_SKUS = 100000
CML_MIN_FEED = 20000
SHOPIFY_QUERY = '''query UnifiedSupplierCatalogue($cursor:String,$q:String!){
 products(first:50,after:$cursor,query:$q){
  nodes{id status title tags vendor productType descriptionHtml
    seo{title description}
    onboardReady:metafield(namespace:"atomfire",key:"supplier_onboarding_ready"){type value}
    manufacturerUrl:metafield(namespace:"atomfire",key:"manufacturer_url"){value}
    compatibilityMode:metafield(namespace:"atomfire",key:"compatibility_mode"){value}
    featuredMedia{... on MediaImage{id image{altText}}}
    variants(first:50){nodes{id sku price inventoryQuantity inventoryPolicy
      inventoryItem{tracked unitCost{amount currencyCode}}
    }pageInfo{hasNextPage}}
  }
  pageInfo{hasNextPage endCursor}
 }
}'''


class SourceAdapterError(ValueError):
    """Incomplete source evidence. No downstream publication permitted."""


# Diagnostic identifiers are closed, nonsecret constants, never Shopify values.
# Read-only rejection semantics remain unchanged; these codes cannot enable
# product creation, Shopify writes or checkout CONTINUE.
SHOPIFY_SNAPSHOT_REASONS = frozenset((
    "query_callback", "response_shape", "pagination_shape",
    "product_shape", "product_gid", "product_duplicate",
    "product_status", "product_tags", "variant_enumeration",
    "variant_gid", "variant_duplicate", "variant_sku",
    "variant_inventory_quantity", "variant_inventory_policy",
    "catalogue_capacity", "cursor_invalid", "pagination_unbounded",
))
SHOPIFY_SNAPSHOT_SCOPES = frozenset(("active", "draft", "archived", "none"))


class ShopifySnapshotValidationError(SourceAdapterError):
    """Only fixed reason/scope codes. Never include payloads or SKUs."""

    def __init__(self, reason_code: str, scope: str):
        if (reason_code not in SHOPIFY_SNAPSHOT_REASONS
                or scope not in SHOPIFY_SNAPSHOT_SCOPES):
            raise ValueError("unrecognised snapshot diagnostic")
        self.reason_code = reason_code
        self.scope = scope
        super().__init__("Shopify snapshot rejected")


def _reject_shopify_snapshot(reason_code: str, scope: str) -> None:
    raise ShopifySnapshotValidationError(reason_code, scope)


@dataclass(frozen=True)
class ShopifySnapshot:
    # Complete across ACTIVE, DRAFT and ARCHIVED, not just supplier-tagged.
    products: tuple[dict, ...]
    variant_to_product: dict[str, str]
    sku_to_variant_ids: dict[str, tuple[str, ...]]
    retrieved_all_statuses: bool


@dataclass(frozen=True)
class SourceFeed:
    supplier: str
    producer_status: str
    observed_at: float
    # Only observations with an independently validated, current producer
    # may have independently_verified=True.
    statuses: dict[str, Stock]
    complete: bool
    hold_reason: str | None


def _readonly(conn: sqlite3.Connection) -> None:
    if not isinstance(conn, sqlite3.Connection):
        raise SourceAdapterError("SQLite connection required")
    if conn.execute("PRAGMA query_only").fetchone() != (1,):
        raise SourceAdapterError("supplier source must be query-only")


def _clock(now: float, observed: float, maximum_age: int) -> bool:
    return (type(now) in (int, float) and type(observed) in (int, float)
            and math.isfinite(now) and math.isfinite(observed)
            and 0 <= now - observed < maximum_age)


def _row_data(row: object) -> tuple[str, str, float] | None:
    if not isinstance(row, tuple) or len(row) != 3:
        return None
    sku, status, checked_at = row
    if (not isinstance(sku, str) or not sku or
            status not in VALID_STOCK or type(checked_at) not in (int, float)
            or not math.isfinite(checked_at)):
        return None
    return sku, status, checked_at


def read_source_feed(conn: sqlite3.Connection, profile: SupplierProfile, *,
                     now: float) -> SourceFeed:
    """Use CML complete-feed proof or Schumacher per-SKU + producer proof.

    CML provider proof follows the existing deployed importer semantics.
    Schumacher is intentionally held whenever its RRP producer reports
    attention / pricing failure; do not infer safety just from status cache.
    """
    _readonly(conn)
    if not isinstance(profile, SupplierProfile) or not profile.enabled:
        raise SourceAdapterError("unknown or disabled supplier adapter")
    if type(now) not in (int, float) or not math.isfinite(now) or now <= 0:
        raise SourceAdapterError("invalid clock")
    if profile.id == "cml":
        health = conn.execute(
            "SELECT last_success,row_count,last_error FROM source_health WHERE supplier='cml'"
        ).fetchone()
        if (not health or not _clock(now, health[0], profile.stock_max_age_s)
                or type(health[1]) is not int or health[2] is not None):
            return SourceFeed("cml", "hold", 0.0, {}, False, "stale_or_failed_cml_import")
        rows = conn.execute(
            "SELECT sku,status,checked_at FROM availability WHERE supplier='cml'"
        ).fetchall()
        if not (CML_MIN_FEED <= len(rows) <= MAX_SKUS
                and CML_MIN_FEED <= health[1] <= MAX_SKUS
                and len(rows) <= health[1]
                and health[1] - len(rows) <= health[1] // 20):
            raise SourceAdapterError("incomplete CML CSV import")
        out: dict[str, Stock] = {}
        for row in rows:
            r = _row_data(row)
            if not r or r[0] in out or r[2] != health[0]:
                raise SourceAdapterError("untrusted CML status row")
            sku, status, stamp = r
            out[sku] = Stock("cml", sku, status, stamp, True)
        return SourceFeed("cml", "ok", health[0], out, True, None)

    if profile.id == "schumacher":
        health = conn.execute(
            "SELECT finished_at,status,summary_json FROM rrp_runtime_health "
            "WHERE component='stock-schumacher'"
        ).fetchone()
        if not health or not _clock(now, health[0], profile.stock_max_age_s):
            return SourceFeed("schumacher", "hold", 0.0, {}, False, "stale_schumacher_producer")
        try:
            summary = json.loads(health[2] or "")
        except (TypeError, ValueError):
            summary = {}
        a, v, u = (summary.get(k) for k in ("attempted", "validated", "unknown"))
        stock_proven = (summary.get("source") == "schumacher"
                        and all(type(x) is int and x >= 0 for x in (a, v, u))
                        and a > 0 and u == 0 and v == a)
        price_healthy = health[1] == "ok" and summary.get("pricing_ok") is True
        price_only_attention = (health[1] == "attention"
                                and summary.get("pricing_ok") is False)
        # The deployed stock poller records ATTENTION solely when optional
        # authenticated PRICE collection fails, *after stock commits*. A
        # successful full batch (attempted == validated, unknown == 0) can
        # therefore validate stock independently of the price observer.
        # Pricing still requires separately verified exact-SKU CostResult;
        # do not disable the existing upstream pricing ATTENTION alarm.
        if not stock_proven or not (price_healthy or price_only_attention):
            return SourceFeed("schumacher", str(health[1]), health[0], {},
                              False, "unverified_schumacher_stock_producer")
        rows = conn.execute(
            "SELECT sku,status,checked_at FROM availability WHERE supplier='schumacher'"
        ).fetchall()
        if not 1 <= len(rows) <= MAX_SKUS:
            raise SourceAdapterError("missing Schumacher source rows")
        out: dict[str, Stock] = {}
        for row in rows:
            r = _row_data(row)
            # One missing/unknown/outdated SKU does not certify it, but should
            # not manufacture a failure for independently fresh peer rows.
            if not r:
                continue
            sku, status, stamp = r
            if sku in out:
                raise SourceAdapterError("duplicate Schumacher SKU")
            if _clock(now, stamp, profile.stock_max_age_s):
                out[sku] = Stock("schumacher", sku, status, stamp, True)
        producer = "stock_ok_pricing_attention" if price_only_attention else "ok"
        return SourceFeed("schumacher", producer, health[0], out, True, None)

    raise SourceAdapterError("supplier requires explicitly reviewed data adapter")


def read_full_shopify_snapshot(query: Callable[[str, dict], Mapping]) -> ShopifySnapshot:
    """Fetch ALL product statuses for a whole-shop duplicate check.

    Every existing rejection remains fail-closed. Fixed reason codes and the
    public status scope are all that a separate diagnostic may log. No raw
    product IDs, SKUs, tags, response bodies or GraphQL errors are logged.
    """
    if not callable(query):
        _reject_shopify_snapshot("query_callback", "none")
    products: dict[str, dict] = {}
    variants: dict[str, str] = {}
    skus: dict[str, set[str]] = {}
    for state in ("active", "draft", "archived"):
        cursor = None
        seen_cursors: set[str] = set()
        for _ in range(50):
            response = query(SHOPIFY_QUERY, {"cursor": cursor, "q": "status:" + state})
            if (not isinstance(response, Mapping) or response.get("errors")
                    or not isinstance(response.get("data"), Mapping)
                    or not isinstance(response["data"].get("products"), Mapping)):
                _reject_shopify_snapshot("response_shape", state)
            connection = response["data"]["products"]
            nodes, page = connection.get("nodes"), connection.get("pageInfo")
            if (not isinstance(nodes, list) or len(nodes) > 50
                    or not isinstance(page, Mapping)
                    or type(page.get("hasNextPage")) is not bool):
                _reject_shopify_snapshot("pagination_shape", state)
            for p in nodes:
                if not isinstance(p, Mapping):
                    _reject_shopify_snapshot("product_shape", state)
                pid = p.get("id")
                if type(pid) is not str or not PRODUCT_GID.fullmatch(pid):
                    _reject_shopify_snapshot("product_gid", state)
                if pid in products:
                    _reject_shopify_snapshot("product_duplicate", state)
                if (type(p.get("status")) is not str
                        or p["status"].lower() != state):
                    _reject_shopify_snapshot("product_status", state)
                if (not isinstance(p.get("tags"), list)
                        or not all(type(t) is str for t in p["tags"])):
                    _reject_shopify_snapshot("product_tags", state)
                group = p.get("variants")
                if (not isinstance(group, Mapping)
                        or not isinstance(group.get("nodes"), list)
                        or len(group["nodes"]) > 50
                        or not isinstance(group.get("pageInfo"), Mapping)
                        or group["pageInfo"].get("hasNextPage") is not False):
                    _reject_shopify_snapshot("variant_enumeration", state)
                for v in group["nodes"]:
                    if not isinstance(v, Mapping):
                        _reject_shopify_snapshot("variant_gid", state)
                    vid, sku = v.get("id"), v.get("sku")
                    if type(vid) is not str or not VARIANT_GID.fullmatch(vid):
                        _reject_shopify_snapshot("variant_gid", state)
                    if vid in variants:
                        _reject_shopify_snapshot("variant_duplicate", state)
                    # Shopify Admin GraphQL sku is nullable (String, not String!).
                    # A missing response field or other JSON type is still invalid.
                    if "sku" not in v or (sku is not None and not isinstance(sku, str)):
                        _reject_shopify_snapshot("variant_sku", state)
                    if type(v.get("inventoryQuantity")) is not int:
                        _reject_shopify_snapshot("variant_inventory_quantity", state)
                    if v.get("inventoryPolicy") not in ("DENY", "CONTINUE"):
                        _reject_shopify_snapshot("variant_inventory_policy", state)
                    variants[vid] = pid
                    # Unassigned SKUs do not claim an identity or mask real duplicates.
                    if isinstance(sku, str) and sku.strip():
                        skus.setdefault(sku.strip().upper(), set()).add(vid)
                products[pid] = dict(p)
                if len(products) > MAX_PRODUCTS or len(variants) > MAX_VARIANTS:
                    _reject_shopify_snapshot("catalogue_capacity", state)
            if page["hasNextPage"] is False:
                break
            nxt = page.get("endCursor")
            if not isinstance(nxt, str) or not nxt or nxt in seen_cursors:
                _reject_shopify_snapshot("cursor_invalid", state)
            cursor = nxt
            seen_cursors.add(nxt)
        else:
            _reject_shopify_snapshot("pagination_unbounded", state)
    return ShopifySnapshot(tuple(products[k] for k in sorted(products)),
                           variants,
                           {sku: tuple(sorted(v)) for sku, v in sorted(skus.items())},
                           True)


def unique_product_sku(snapshot: ShopifySnapshot, sku: str, variant_id: str) -> bool:
    """No cross-status duplicate claim unless the *whole-shop* read succeeded."""
    if not isinstance(snapshot, ShopifySnapshot) or snapshot.retrieved_all_statuses is not True:
        return False
    if not isinstance(sku, str) or not isinstance(variant_id, str):
        return False
    return snapshot.sku_to_variant_ids.get(sku.strip().upper()) == (variant_id,)

def unique_canonical_supplier_sku(snapshot: ShopifySnapshot, profile: SupplierProfile,
                                   supplier_sku: str, variant_id: str) -> bool:
    """Require a single canonical supplier SKU across ALL Shopify statuses.

    This also detects an AS-prefixed variant colliding with an approved
    Team Associated or Reedy numeric alias. It cannot certify manufacturer
    provenance on its own: the caller needs independently verified evidence.
    """
    if (not isinstance(snapshot, ShopifySnapshot)
            or snapshot.retrieved_all_statuses is not True
            or not isinstance(profile, SupplierProfile)
            or not isinstance(supplier_sku,str) or not supplier_sku
            or not isinstance(variant_id,str)):
        return False
    claimed = []
    for p in snapshot.products:
        if not isinstance(p, Mapping):
            return False
        group=p.get("variants")
        if not isinstance(group,Mapping) or not isinstance(group.get("nodes"),list):
            return False
        for v in group["nodes"]:
            if not isinstance(v,Mapping):
                return False
            canonical=canonical_supplier_code(v.get("sku"),p.get("vendor"),p.get("title"),profile)
            if canonical == supplier_sku:
                claimed.append(v.get("id"))
                if len(claimed)>1:
                    return False
    return claimed == [variant_id]
