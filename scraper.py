#!/usr/bin/env python3
"""
CRE Transactions Tracker — Scraper
Pulls Denver parcel data from ArcGIS, diffs against previous snapshot,
and outputs new commercial real estate transactions above $2M.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import urllib.request
import urllib.parse
import urllib.error

# ── Config ──────────────────────────────────────────────────────────────────

ARCGIS_URL = (
    "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/ArcGIS/rest/services/"
    "ODC_PROP_PARCELS_A/FeatureServer/245/query"
)

MIN_SALE_PRICE = 2_000_000  # $2M threshold for tracker
PAGE_SIZE = 2000

# D_CLASS_CN values considered commercial real estate
CRE_CLASSES = {
    # Commercial
    "COMMERCIAL-CONDOMINIUM", "COMMERCIAL-FINANCIAL OFFICE", "COMMERCIAL-HOTEL",
    "COMMERCIAL-MEDICAL OFFICE", "COMMERCIAL-MISC IMPS", "COMMERCIAL-OFFICE",
    "COMMERCIAL-PARKING GARAGE", "COMMERCIAL-RESTAURANT", "COMMERCIAL-RETAIL",
    "COMMERCIAL-SHOPPING CENTER",
    # Industrial
    "INDUSTRIAL-AUTO DEALER", "INDUSTRIAL-AUTO SERVICE GARAGE", "INDUSTRIAL-CAR WASH",
    "INDUSTRIAL-CHURCH", "INDUSTRIAL-CONV STORE W/PUMPS", "INDUSTRIAL-DRY CLEANING",
    "INDUSTRIAL-FACTORY", "INDUSTRIAL-FOOD PROCESSING", "INDUSTRIAL-GRAIN ELEVATOR",
    "INDUSTRIAL-HEALTH CLUB", "INDUSTRIAL-MISC RECREATION", "INDUSTRIAL-SCHOOL",
    "INDUSTRIAL-SERVICE STATION", "INDUSTRIAL-SHIPPING TERMINAL", "INDUSTRIAL-WAREHOUSE",
    # Mixed use
    "AUTO DEALER W/MIXED USE", "HOTEL W/MIXED USE", "OFFICE W/MIXED USE",
    "RESTAURANT W/MIXED USE", "RETAIL W/MIXED USE",
    # Large multifamily (CRE)
    "RESIDENTIAL-MULTI UNIT APTS", "RESIDENTIAL-APARTMENT",
    "RESIDENTIAL-NURSING FACILITY", "RESIDENTIAL-SENIOR CITIZEN APT",
    # Vacant land (major land deals)
    "VACANT LAND",
}

OUT_FIELDS = [
    "OBJECTID", "SCHEDNUM", "OWNER_NAME",
    "OWNER_ADDRESS_LINE1", "OWNER_CITY", "OWNER_STATE", "OWNER_ZIP",
    "SITUS_ADDRESS_LINE1", "SITUS_CITY", "SITUS_STATE", "SITUS_ZIP",
    "SITUS_X_COORD", "SITUS_Y_COORD",
    "PROP_CLASS", "D_CLASS", "D_CLASS_CN",
    "ZONE_ID", "COM_STRUCTURE_TYPE", "COM_GROSS_AREA",
    "APPRAISED_TOTAL_VALUE", "ASSESSED_TOTAL_VALUE_LOCAL",
    "SALE_DATE", "SALE_MONTHDAY", "SALE_YEAR", "SALE_PRICE",
    "RECEPTION_NUM", "LAND_AREA", "TOT_UNITS",
]

SNAPSHOT_FILE = "cre-snapshot.json"
TRANSACTIONS_FILE = "cre-transactions.json"


# ── Coordinate conversion ──────────────────────────────────────────────────
# SITUS_X/Y are Colorado State Plane Central (NAD83 FIPS 0502, US feet)
# WKID 2877 — we convert to WGS84 using the ArcGIS geometry service

def convert_coords_batch(points):
    """Convert state plane coords to WGS84 via ArcGIS geometry service."""
    if not points:
        return {}

    results = {}
    # Process in batches of 200
    batch_size = 200
    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]
        geometries = {
            "geometryType": "esriGeometryPoint",
            "geometries": [{"x": x, "y": y} for x, y in batch]
        }
        params = urllib.parse.urlencode({
            "inSR": "2877",
            "outSR": "4326",
            "geometries": json.dumps(geometries),
            "f": "json"
        })
        url = f"https://tasks.arcgisonline.com/arcgis/rest/services/Geometry/GeometryServer/project?{params}"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            for j, geom in enumerate(data.get("geometries", [])):
                orig = batch[j]
                results[orig] = (geom["y"], geom["x"])  # lat, lng
        except Exception as e:
            print(f"  Coord conversion batch {i} failed: {e}")

        if i + batch_size < len(points):
            time.sleep(0.5)

    return results


# ── ArcGIS query ────────────────────────────────────────────────────────────

def query_arcgis(where, out_fields=None, order_by=None, max_records=None):
    """Query the ArcGIS feature service with pagination."""
    fields = ",".join(out_fields) if out_fields else "*"
    all_features = []
    offset = 0

    while True:
        params = {
            "where": where,
            "outFields": fields,
            "returnGeometry": "false",
            "resultRecordCount": PAGE_SIZE,
            "resultOffset": offset,
            "f": "json",
        }
        if order_by:
            params["orderByFields"] = order_by

        query_string = urllib.parse.urlencode(params)
        url = f"{ARCGIS_URL}?{query_string}"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            print(f"  Query failed at offset {offset}: {e}")
            break

        if "error" in data:
            print(f"  ArcGIS error: {data['error']}")
            break

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        print(f"  Fetched {len(all_features)} records...", end="\r")

        if len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

        if max_records and len(all_features) >= max_records:
            break

        time.sleep(0.3)

    print(f"  Fetched {len(all_features)} records total")
    return all_features


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)
    current_year = int(datetime.now().strftime("%Y"))
    print(f"CRE Tracker scrape — {now.strftime('%Y-%m-%d %H:%M UTC')}")

    # ── Step 1: Pull current-year parcels with any sale ──
    print(f"\n1. Querying parcels with SALE_YEAR >= {current_year - 1}...")
    where = f"SALE_YEAR >= {current_year - 1} AND SALE_PRICE > 0"
    features = query_arcgis(where, OUT_FIELDS, "SALE_DATE DESC")

    # Build lookup by SCHEDNUM (unique parcel ID)
    current = {}
    for f in features:
        a = f["attributes"]
        key = a.get("SCHEDNUM")
        if key:
            current[key] = a

    print(f"   {len(current)} unique parcels with recent sales")

    # ── Step 2: Load previous snapshot ──
    prev = {}
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE) as fh:
                prev = json.load(fh)
            print(f"\n2. Loaded previous snapshot: {len(prev)} parcels")
        except Exception as e:
            print(f"\n2. Could not load snapshot: {e}")
    else:
        print("\n2. No previous snapshot — first run, all sales treated as new")

    # ── Step 3: Diff ──
    new_transactions = []
    for schednum, attrs in current.items():
        sale_price = attrs.get("SALE_PRICE", 0) or 0
        sale_date = attrs.get("SALE_DATE")
        d_class_cn = attrs.get("D_CLASS_CN", "")

        # Check if this is a new or changed sale
        prev_attrs = prev.get(schednum, {})
        prev_price = prev_attrs.get("SALE_PRICE", 0) or 0
        prev_date = prev_attrs.get("SALE_DATE")

        is_new = (sale_price != prev_price or sale_date != prev_date)

        if not is_new:
            continue

        # Apply filters: CRE class + $2M threshold
        # Also include residential sales above $5M (luxury/notable)
        is_cre = d_class_cn in CRE_CLASSES
        is_luxury_residential = (d_class_cn not in CRE_CLASSES and sale_price >= 5_000_000)

        if not (is_cre and sale_price >= MIN_SALE_PRICE) and not is_luxury_residential:
            continue

        new_transactions.append({
            **attrs,
            "prev_sale_price": prev_price if prev_price else None,
            "prev_owner": prev_attrs.get("OWNER_NAME"),
            "detected_date": now.strftime("%Y-%m-%d"),
            "is_luxury_residential": is_luxury_residential,
        })

    print(f"\n3. Detected {len(new_transactions)} new CRE transactions")

    # ── Step 4: Convert coordinates ──
    if new_transactions:
        print("\n4. Converting coordinates to WGS84...")
        points_needed = set()
        for t in new_transactions:
            x = t.get("SITUS_X_COORD")
            y = t.get("SITUS_Y_COORD")
            if x and y:
                points_needed.add((x, y))

        coord_map = convert_coords_batch(list(points_needed))
        for t in new_transactions:
            x = t.get("SITUS_X_COORD")
            y = t.get("SITUS_Y_COORD")
            if x and y and (x, y) in coord_map:
                t["lat"], t["lng"] = coord_map[(x, y)]
            else:
                t["lat"], t["lng"] = None, None

        print(f"   Converted {len(coord_map)} coordinate pairs")
    else:
        print("\n4. No new transactions to geocode")

    # ── Step 5: Merge with existing transactions file ──
    existing = []
    if os.path.exists(TRANSACTIONS_FILE):
        try:
            with open(TRANSACTIONS_FILE) as fh:
                existing = json.load(fh)
        except Exception:
            existing = []

    # Deduplicate by SCHEDNUM + SALE_DATE
    seen = set()
    for t in existing:
        key = (t.get("SCHEDNUM"), t.get("SALE_DATE"))
        seen.add(key)

    added = 0
    for t in new_transactions:
        key = (t.get("SCHEDNUM"), t.get("SALE_DATE"))
        if key not in seen:
            existing.append(t)
            seen.add(key)
            added += 1

    # Sort by sale date descending
    existing.sort(key=lambda x: x.get("SALE_DATE") or 0, reverse=True)

    print(f"\n5. Added {added} new transactions (total: {len(existing)})")

    # ── Step 6: Save ──
    with open(TRANSACTIONS_FILE, "w") as fh:
        json.dump(existing, fh, indent=2)
    print(f"   Wrote {TRANSACTIONS_FILE}")

    # Save current snapshot for next diff
    with open(SNAPSHOT_FILE, "w") as fh:
        json.dump(current, fh)
    print(f"   Wrote {SNAPSHOT_FILE} ({len(current)} parcels)")

    # ── Summary ──
    if new_transactions:
        print(f"\n{'='*60}")
        print(f"NEW CRE TRANSACTIONS:")
        print(f"{'='*60}")
        for t in sorted(new_transactions, key=lambda x: x.get("SALE_PRICE", 0), reverse=True)[:20]:
            price = t.get("SALE_PRICE", 0)
            addr = t.get("SITUS_ADDRESS_LINE1", "Unknown")
            cls = t.get("D_CLASS_CN", "?")
            tag = " [LUXURY RES]" if t.get("is_luxury_residential") else ""
            print(f"  ${price:>14,.0f}  {addr:<35s}  {cls}{tag}")


if __name__ == "__main__":
    main()
