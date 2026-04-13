#!/usr/bin/env python3
"""
CRE Transactions Tracker — Scraper
Pulls ALL Denver parcel sales from ArcGIS, diffs against previous snapshot,
and stores every transaction. Filtering happens at the display layer.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import urllib.request
import urllib.parse
import urllib.error

ARCGIS_URL = (
    "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/ArcGIS/rest/services/"
    "ODC_PROP_PARCELS_A/FeatureServer/245/query"
)
PAGE_SIZE = 2000

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


def convert_coords_batch(points):
    if not points:
        return {}
    results = {}
    batch_size = 200
    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]
        geometries = {
            "geometryType": "esriGeometryPoint",
            "geometries": [{"x": x, "y": y} for x, y in batch]
        }
        params = urllib.parse.urlencode({
            "inSR": "2877", "outSR": "4326",
            "geometries": json.dumps(geometries), "f": "json"
        })
        url = f"https://tasks.arcgisonline.com/arcgis/rest/services/Geometry/GeometryServer/project?{params}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            for j, geom in enumerate(data.get("geometries", [])):
                results[batch[j]] = (geom["y"], geom["x"])
        except Exception as e:
            print(f"  Coord batch {i} failed: {e}")
        if i + batch_size < len(points):
            time.sleep(0.5)
    return results


def query_arcgis(where, out_fields=None, order_by=None):
    fields = ",".join(out_fields) if out_fields else "*"
    all_features = []
    offset = 0
    while True:
        params = {
            "where": where, "outFields": fields,
            "returnGeometry": "false", "resultRecordCount": PAGE_SIZE,
            "resultOffset": offset, "f": "json",
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
        time.sleep(0.3)
    print(f"  Fetched {len(all_features)} records total")
    return all_features


def main():
    now = datetime.now(timezone.utc)
    current_year = int(datetime.now().strftime("%Y"))
    print(f"CRE Tracker scrape — {now.strftime('%Y-%m-%d %H:%M UTC')}")

    print(f"\n1. Querying ALL parcels with SALE_YEAR >= {current_year - 1}...")
    where = f"SALE_YEAR >= {current_year - 1} AND SALE_PRICE > 0"
    features = query_arcgis(where, OUT_FIELDS, "SALE_DATE DESC")

    current = {}
    for f in features:
        a = f["attributes"]
        key = a.get("SCHEDNUM")
        if key:
            current[key] = a
    print(f"   {len(current)} unique parcels with recent sales")

    prev = {}
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE) as fh:
                prev = json.load(fh)
            print(f"\n2. Loaded previous snapshot: {len(prev)} parcels")
        except Exception as e:
            print(f"\n2. Could not load snapshot: {e}")
    else:
        print("\n2. No previous snapshot — first run")

    new_transactions = []
    for schednum, attrs in current.items():
        sale_price = attrs.get("SALE_PRICE", 0) or 0
        sale_date = attrs.get("SALE_DATE")
        if sale_price <= 0:
            continue
        prev_attrs = prev.get(schednum, {})
        prev_price = prev_attrs.get("SALE_PRICE", 0) or 0
        prev_date = prev_attrs.get("SALE_DATE")
        if sale_price == prev_price and sale_date == prev_date:
            continue
        new_transactions.append({
            **attrs,
            "prev_sale_price": prev_price if prev_price else None,
            "prev_owner": prev_attrs.get("OWNER_NAME"),
            "detected_date": now.strftime("%Y-%m-%d"),
        })

    print(f"\n3. Detected {len(new_transactions)} new transactions")

    if new_transactions:
        print("\n4. Converting coordinates...")
        points_needed = set()
        for t in new_transactions:
            x, y = t.get("SITUS_X_COORD"), t.get("SITUS_Y_COORD")
            if x and y:
                points_needed.add((x, y))
        coord_map = convert_coords_batch(list(points_needed))
        for t in new_transactions:
            x, y = t.get("SITUS_X_COORD"), t.get("SITUS_Y_COORD")
            if x and y and (x, y) in coord_map:
                t["lat"], t["lng"] = coord_map[(x, y)]
            else:
                t["lat"], t["lng"] = None, None
        print(f"   Converted {len(coord_map)} pairs")

    existing = []
    if os.path.exists(TRANSACTIONS_FILE):
        try:
            with open(TRANSACTIONS_FILE) as fh:
                existing = json.load(fh)
        except Exception:
            existing = []

    seen = set()
    for t in existing:
        seen.add((t.get("SCHEDNUM"), t.get("SALE_DATE")))
    added = 0
    for t in new_transactions:
        key = (t.get("SCHEDNUM"), t.get("SALE_DATE"))
        if key not in seen:
            existing.append(t)
            seen.add(key)
            added += 1

    existing.sort(key=lambda x: x.get("SALE_DATE") or 0, reverse=True)
    print(f"\n5. Added {added} new (total: {len(existing)})")

    with open(TRANSACTIONS_FILE, "w") as fh:
        json.dump(existing, fh)
    with open(SNAPSHOT_FILE, "w") as fh:
        json.dump(current, fh)
    print(f"   Wrote {TRANSACTIONS_FILE} + {SNAPSHOT_FILE}")

    big = [t for t in new_transactions if (t.get("SALE_PRICE", 0) or 0) >= 10_000_000]
    if big:
        print(f"\n{'='*60}\n$10M+ DEALS ({len(big)}):\n{'='*60}")
        for t in sorted(big, key=lambda x: x.get("SALE_PRICE", 0), reverse=True)[:10]:
            print(f"  ${t.get('SALE_PRICE',0):>14,.0f}  {t.get('SITUS_ADDRESS_LINE1','?'):<35s}  {t.get('D_CLASS_CN','?')}")


if __name__ == "__main__":
    main()
