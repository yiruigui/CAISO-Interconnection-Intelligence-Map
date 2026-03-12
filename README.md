# CAISO Interconnection Intelligence Map

An interactive map for analyzing CAISO transmission interconnection opportunities, built for **Antora Energy** (Thermal Battery Power Supply).

**[🗺️ Live Map →](https://yiruigui.github.io/CAISO-Interconnection-Intelligence-Map/caiso_tx_intelligence_v4.html)**

---

## What it shows

Four independent analysis layers — toggle any combination on/off:

| Layer | Visual channel | Metric |
|-------|---------------|--------|
| **01 Transmission Capacity** | Dot color (green → red) | Available TPD MW vs. Plan Capability |
| **02 Queue Congestion** | Dot size | Active queue MW at each constraint |
| **03 Arbitrage Opportunity** | Choropleth fill | LMP spread & volatility score by zone |
| **04 Industrial Heat Clusters** | Blue triangle icons | EPA GHGRP industrial heat demand (MMBtu/yr) |

Layers 01 and 02 use **bivariate encoding** — toggling one never affects the other's visual channel.

## Data sources

- **CAISO TPD data** — Attachment A/B transmission capability estimates (CPUC IRP 2024)
- **CAISO Interconnection Queue** — Gridstatus.io API (pre-computed snapshot embedded)
- **CAISO LMP data** — Top 4 congestion hours, 3 CA pricing zones (NP15 / SP15 / ZP26)
- **EPA GHGRP** — California industrial facilities CO₂ emissions (heat demand derived)

## Running the data pipeline

```bash
pip install pandas geopandas gridstatus requests
python pipeline_v4.py
```

The pipeline refreshes queue data from Gridstatus, computes LMP zone scores, and embeds everything into the HTML as self-contained JSON constants.

## Tech stack

- [Leaflet.js](https://leafletjs.com/) — map rendering
- [Leaflet.markercluster](https://github.com/Leaflet/Leaflet.markercluster) — industrial heat clusters
- Pure HTML/CSS/JS — no build step, runs from file://
