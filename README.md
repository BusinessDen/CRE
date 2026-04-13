# CRE Transactions Tracker

Denver metro commercial real estate transactions tracker. Part of the [Dreck Suite](https://github.com/BusinessDen).

## Data Source

Denver Assessor parcel data via ArcGIS Feature Service:
```
ODC_PROP_PARCELS_A/FeatureServer/245
```

240,000+ parcels with sale price, date, owner, property classification, and coordinates.

## How It Works

1. **Nightly scrape** pulls all parcels with `SALE_YEAR >= current year - 1` from Denver's ArcGIS service
2. **Diff detection** compares each parcel's `SALE_PRICE + SALE_DATE` against the previous snapshot
3. **Filters** to commercial/industrial/multifamily types above $2M (luxury residential at $5M+)
4. **Coordinates** converted from Colorado State Plane (WKID 2877) to WGS84 via ArcGIS geometry service
5. **Deploys** updated data to GitHub Pages

## Property Types Tracked

- **Commercial**: Office, Retail, Hotel, Restaurant, Shopping Center, Medical Office, Parking, Financial
- **Industrial**: Warehouse, Factory, Auto Dealer/Service, Food Processing, Shipping Terminal
- **Mixed Use**: Retail, Office, Hotel, Restaurant w/ Mixed Use
- **Multifamily**: Apartments, Multi-Unit, Nursing Facility, Senior Housing
- **Vacant Land**: Major land deals at $2M+
- **Luxury Residential**: SFR/Condo/Rowhouse at $5M+ (flagged separately)

## Files

| File | Purpose |
|------|---------|
| `index.html` | Frontend — map, table, filters |
| `auth.js` | Shared Dreck Suite authentication |
| `scraper.py` | Nightly data pull + diff detection |
| `cre-transactions.json` | Detected transactions (cumulative) |
| `cre-snapshot.json` | Previous day's parcel snapshot (for diffing) |
| `.github/workflows/scrape.yml` | GitHub Actions — DST-aware daily cron |

## Data Freshness

Denver Assessor data has a ~3–4 week lag from sale closing to appearance in the ArcGIS service. Transactions surface as the assessor processes deed recordings.

## Value Tiers (Map Markers)

| Color | Range |
|-------|-------|
| Gold | $2M – $5M |
| Orange | $5M – $10M |
| Red | $10M – $25M |
| Pink | $25M+ |

## Running Locally

```bash
python3 scraper.py
# Then open index.html
```

No API keys required — the Denver ArcGIS service is public.
