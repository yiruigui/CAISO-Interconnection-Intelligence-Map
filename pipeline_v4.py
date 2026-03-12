"""
CAISO Transmission Intelligence Pipeline  v3
Processes CPUC IRP transmission constraint data into GeoJSON for frontend visualization.

Changes in v3:
  - Layer 02 upgraded from static Queue-to-Peak Ratio to live Queue Congestion metric
    Queue Congestion = Total Active Queue MW / Total Plan Capability (per constraint)
  - Fetches CAISO interconnection queue from Gridstatus.io API (filtered for Active status,
    Solar / Wind / Battery Storage fuel types)
  - Maps queue projects to constraints via Point of Interconnection (POI) name
  - New fields added per feature:
      queue_congestion_ratio  = active_queue_mw / plan_capability_mw
      active_queue_mw         = sum of MW for active Solar/Wind/Storage projects at POI
      active_queue_count       = number of such projects
      queue_data_vintage       = ISO timestamp of Gridstatus data pull

  - Fallback: if Gridstatus API is unavailable, pipeline prints a warning and sets
    queue_congestion_ratio = None so the frontend degrades gracefully to '--'

Layers:
  01 – Available Capacity MW (available_tpd_mw — the actual remaining headroom)
  02 – Queue Congestion Forecast (active_queue_mw / plan_capability_mw)
  03 – Constraint Severity (Allocated / Plan)
  04 – Industrial Heat Demand / CO₂ Displacement

Gridstatus API key is stored in gridstatus_api_key.txt in the same directory as this script.
"""

import pandas as pd
import json
import math
import re
import os
from pathlib import Path
from datetime import datetime, timezone

# ── File paths ───────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
ATTACHMENT_A = BASE_DIR / "attachment-a-transmission-capability-estimates-for-use-in-the-cpuc-irp-process-v2024.xlsx"
ATTACHMENT_B = BASE_DIR / "attachment-b1-v8-constraint-mapping-2024-ipe.xlsx"
OUTPUT_DIR   = BASE_DIR

# ── Gridstatus API key (stored separately for security) ──────────────────────
# The key is read from 'gridstatus_api_key.txt' in the same directory.
# If the file does not exist, the key can also be set via the environment variable
# GRIDSTATUS_API_KEY.  If neither is found, Layer 02 queue data will be skipped.
_KEY_FILE = BASE_DIR / "gridstatus_api_key.txt"

def _load_api_key() -> str | None:
    """Return the Gridstatus API key, or None if not found."""
    if _KEY_FILE.exists():
        return _KEY_FILE.read_text().strip()
    return os.environ.get("GRIDSTATUS_API_KEY")

GRIDSTATUS_API_KEY = _load_api_key()
# ────────────────────────────────────────────────────────────────────────────


# ═══════════════════════════════════════════════════════════════════════════
# SUBSTATION COORDINATES  (HIFLD + PG&E public data)
# Expanded to cover all PG&E NorCal, Greater Bay, Kern, and Fresno substations
# ═══════════════════════════════════════════════════════════════════════════
SUBSTATION_COORDS = {
    # ── SCE Northern ──────────────────────────────────────────────────────
    "Antelope Substation 500 kV":       {"lat": 34.7650, "lon": -118.4300, "utility": "SCE"},
    "Antelope Substation 230 kV":       {"lat": 34.7620, "lon": -118.4250, "utility": "SCE"},
    "Vincent Substation 500 kV":        {"lat": 34.4250, "lon": -117.9640, "utility": "SCE"},
    "Lugo Substation 500 kV":           {"lat": 34.3790, "lon": -117.5210, "utility": "SCE"},
    "Magunden Substation 230 kV":       {"lat": 37.0400, "lon": -119.0250, "utility": "SCE"},
    "Pardee Substation 230 kV":         {"lat": 34.3320, "lon": -118.5430, "utility": "SCE"},
    "Windhub Substation 500 kV":        {"lat": 34.9000, "lon": -118.3500, "utility": "SCE"},
    "Moorpark Substation 230 kV":       {"lat": 34.2780, "lon": -118.8830, "utility": "SCE"},
    # ── SCE Metro / Eastern ───────────────────────────────────────────────
    "Hinson Substation 230 kV":         {"lat": 33.8280, "lon": -118.1200, "utility": "SCE"},
    "Del Amo Substation 230 kV":        {"lat": 33.8420, "lon": -118.1390, "utility": "SCE"},
    "Devers Substation 500 kV":         {"lat": 33.9480, "lon": -116.9880, "utility": "SCE"},
    "Red Bluff Substation 500 kV":      {"lat": 33.7150, "lon": -116.3050, "utility": "SCE"},
    "Colorado River Substation 500 kV": {"lat": 34.4880, "lon": -114.3560, "utility": "SCE"},
    "Colorado River Substation 500kV":  {"lat": 34.4880, "lon": -114.3560, "utility": "SCE"},
    "Etiwanda Substation 230 kV":       {"lat": 34.1530, "lon": -117.4950, "utility": "SCE"},
    "Serrano Substation 500 kV":        {"lat": 33.9980, "lon": -117.5880, "utility": "SCE"},
    "Eagle Mountain Substation":        {"lat": 33.8340, "lon": -115.4380, "utility": "SCE"},
    # ── SDG&E ─────────────────────────────────────────────────────────────
    "Capistrano Substation 230 kV":     {"lat": 33.4870, "lon": -117.6650, "utility": "SDGE"},
    "San Onofre Substation 230 kV":     {"lat": 33.3720, "lon": -117.5530, "utility": "SDGE"},
    "Talega Substation 230 kV":         {"lat": 33.4730, "lon": -117.6100, "utility": "SDGE"},
    "Miguel Substation 500 kV":         {"lat": 32.8100, "lon": -116.8800, "utility": "SDGE"},
    "Sunrise Substation 230 kV":        {"lat": 32.9550, "lon": -116.7450, "utility": "SDGE"},
    "Otay Mesa Substation 230 kV":      {"lat": 32.5830, "lon": -117.0350, "utility": "SDGE"},
    "Silvergate Substation 230 kV":     {"lat": 32.7200, "lon": -117.2100, "utility": "SDGE"},
    "Encina Substation 230 kV":         {"lat": 33.1550, "lon": -117.3500, "utility": "SDGE"},
    "San Luis Rey Substation 230 kV":   {"lat": 33.2280, "lon": -117.3300, "utility": "SDGE"},
    "Trabuco Substation 138 kV":        {"lat": 33.6150, "lon": -117.5900, "utility": "SDGE"},
    # ── EOP / Nevada ──────────────────────────────────────────────────────
    "Eldorado Substation 500 kV":       {"lat": 35.7630, "lon": -114.8820, "utility": "NV"},
    "Sloan Canyon Substation 500 kV":   {"lat": 35.9330, "lon": -115.0530, "utility": "NV"},
    "GLW Substation 230 kV":            {"lat": 36.0850, "lon": -115.1450, "utility": "NV"},

    # ── PG&E Bay Area ──────────────────────────────────────────────────────
    "Tesla Substation 500 kV":          {"lat": 37.6380, "lon": -121.4350, "utility": "PGE"},
    "Collinsville Substation 500 kV":   {"lat": 38.0760, "lon": -121.8630, "utility": "PGE"},
    "Metcalf Substation 500 kV":        {"lat": 37.2750, "lon": -121.8230, "utility": "PGE"},
    "Diablo Substation 500 kV":         {"lat": 37.8870, "lon": -121.9370, "utility": "PGE"},
    "Mossland Substation 500 kV":       {"lat": 36.8000, "lon": -121.8020, "utility": "PGE"},
    "MossLanding Substation 500 kV":    {"lat": 36.8050, "lon": -121.7850, "utility": "PGE"},
    "Newark Substation 230 kV":         {"lat": 37.5220, "lon": -122.0310, "utility": "PGE"},
    "Dumbarton Substation 230 kV":      {"lat": 37.4950, "lon": -122.1100, "utility": "PGE"},
    "Eastshore Substation 230 kV":      {"lat": 37.8700, "lon": -122.3050, "utility": "PGE"},
    "Eastshore Substation 115 kV":      {"lat": 37.8700, "lon": -122.3050, "utility": "PGE"},
    "San Mateo Substation 230 kV":      {"lat": 37.5630, "lon": -122.3070, "utility": "PGE"},
    "Contra Costa Substation 230 kV":   {"lat": 38.0300, "lon": -122.1200, "utility": "PGE"},
    "Contra Costa Substation 115 kV":   {"lat": 38.0300, "lon": -122.1200, "utility": "PGE"},
    "Windmaster Substation 230 kV":     {"lat": 38.0550, "lon": -121.9500, "utility": "PGE"},
    "Delta Pumps Substation 230 kV":    {"lat": 38.0700, "lon": -121.8900, "utility": "PGE"},
    "Lakeville Substation 230 kV":      {"lat": 38.2100, "lon": -122.5200, "utility": "PGE"},
    "Ignacio Substation 230 kV":        {"lat": 38.0650, "lon": -122.5500, "utility": "PGE"},
    "Sobrante Substation 230 kV":       {"lat": 37.9550, "lon": -122.2800, "utility": "PGE"},
    "Moraga Substation 230 kV":         {"lat": 37.8350, "lon": -122.1300, "utility": "PGE"},
    "Tracy Pump Substation 230 kV":     {"lat": 37.7250, "lon": -121.4600, "utility": "PGE"},
    "Kasson Substation 115 kV":         {"lat": 37.3400, "lon": -121.9200, "utility": "PGE"},
    "Heinz Substation 115 kV":          {"lat": 37.5850, "lon": -122.2000, "utility": "PGE"},
    "Grant Substation 115 kV":          {"lat": 37.8780, "lon": -122.2700, "utility": "PGE"},
    "Salado Substation 115 kV":         {"lat": 37.3500, "lon": -121.9750, "utility": "PGE"},
    "Crow Creek Substation 60 kV":      {"lat": 37.3350, "lon": -121.9850, "utility": "PGE"},
    "Morganhill Substation 115 kV":     {"lat": 37.1300, "lon": -121.6500, "utility": "PGE"},
    "Birds Landing Substation 230 kV":  {"lat": 38.1100, "lon": -121.8900, "utility": "PGE"},
    "Bellota Substation 230 kV":        {"lat": 37.9600, "lon": -121.1900, "utility": "PGE"},
    "Weber Substation 230 kV":          {"lat": 37.9800, "lon": -121.2200, "utility": "PGE"},
    "Eight Mile Substation 230 kV":     {"lat": 38.0100, "lon": -121.3100, "utility": "PGE"},

    # ── PG&E North of Greater Bay ──────────────────────────────────────────
    "Woodland Substation 115 kV":       {"lat": 38.6780, "lon": -121.7730, "utility": "PGE"},
    "Davis Substation 115 kV":          {"lat": 38.5440, "lon": -121.7380, "utility": "PGE"},
    "Bell Substation 115 kV":           {"lat": 38.6600, "lon": -121.3800, "utility": "PGE"},
    "Placer Substation 115 kV":         {"lat": 38.8910, "lon": -121.0800, "utility": "PGE"},
    "Rocklin Substation 115 kV":        {"lat": 38.7910, "lon": -121.2360, "utility": "PGE"},
    "Pleasant Grove Substation 115 kV": {"lat": 38.7950, "lon": -121.5250, "utility": "PGE"},
    "Cortina Substation 115 kV":        {"lat": 39.1750, "lon": -122.3500, "utility": "PGE"},
    "Eagle Rock Substation 115 kV":     {"lat": 39.3300, "lon": -122.4600, "utility": "PGE"},
    "Carberry Substation 230 kV":       {"lat": 40.6500, "lon": -122.4800, "utility": "PGE"},
    "Round Mountain Substation 230 kV": {"lat": 40.7900, "lon": -122.0000, "utility": "PGE"},
    "Rio Oso Substation 230 kV":        {"lat": 38.9650, "lon": -121.5400, "utility": "PGE"},
    "Brighton Substation 230 kV":       {"lat": 38.5600, "lon": -121.4200, "utility": "PGE"},
    "Lockeford Substation 230 kV":      {"lat": 38.1600, "lon": -121.1500, "utility": "PGE"},

    # ── PG&E Kern ──────────────────────────────────────────────────────────
    "Gates Substation 500 kV":          {"lat": 36.8600, "lon": -120.1450, "utility": "PGE"},
    "Midway Substation 500 kV":         {"lat": 35.2800, "lon": -119.5600, "utility": "PGE"},
    "Midway Substation 230 kV":         {"lat": 35.2800, "lon": -119.5600, "utility": "PGE"},
    "Midway Substation 115 kV":         {"lat": 35.2800, "lon": -119.5600, "utility": "PGE"},
    "Kern Substation 230 kV":           {"lat": 35.3700, "lon": -119.0300, "utility": "PGE"},
    "Kern Substation 115 kV":           {"lat": 35.3700, "lon": -119.0300, "utility": "PGE"},
    "Smyrna Substation 115 kV":         {"lat": 35.4500, "lon": -119.2500, "utility": "PGE"},
    "Atwell Junction Substation 115 kV":{"lat": 35.5000, "lon": -119.0800, "utility": "PGE"},
    "Maricopa Substation 70 kV":        {"lat": 35.0580, "lon": -119.4030, "utility": "PGE"},
    "Copus Substation 70 kV":           {"lat": 35.1200, "lon": -119.2800, "utility": "PGE"},
    "Taft Substation 115 kV":           {"lat": 35.1420, "lon": -119.4480, "utility": "PGE"},
    "Tevis Substation 115 kV":          {"lat": 35.4000, "lon": -118.9800, "utility": "PGE"},
    "Stockdale Substation 115 kV":      {"lat": 35.3800, "lon": -119.1200, "utility": "PGE"},
    "Lamont Substation 115 kV":         {"lat": 35.2600, "lon": -118.9200, "utility": "PGE"},
    "Semitropic Substation 115 kV":     {"lat": 35.5400, "lon": -119.4200, "utility": "PGE"},
    "Buena Vista Junction Substation":  {"lat": 35.2200, "lon": -119.6500, "utility": "PGE"},
    "Cal Flat Substation 230 kV":       {"lat": 35.9600, "lon": -120.3800, "utility": "PGE"},
    "Arco Substation 230 kV":           {"lat": 35.6100, "lon": -119.8500, "utility": "PGE"},

    # ── PG&E Fresno ────────────────────────────────────────────────────────
    "Panoche Substation 230 kV":        {"lat": 36.5950, "lon": -120.8050, "utility": "PGE"},
    "Panoche Substation 115 kV":        {"lat": 36.5950, "lon": -120.8050, "utility": "PGE"},
    "Mendota Substation 115 kV":        {"lat": 36.7520, "lon": -120.3810, "utility": "PGE"},
    "Oro Loma Substation 115 kV":       {"lat": 36.7300, "lon": -120.2000, "utility": "PGE"},
    "El Nido Substation 115 kV":        {"lat": 37.1200, "lon": -120.5800, "utility": "PGE"},
    "Tranquility Substation 230 kV":    {"lat": 36.6250, "lon": -120.2550, "utility": "PGE"},
    "Helm Substation 230 kV":           {"lat": 36.5200, "lon": -119.9700, "utility": "PGE"},
    "Helm Substation 70 kV":            {"lat": 36.5200, "lon": -119.9700, "utility": "PGE"},
    "Dairyland Substation 115 kV":      {"lat": 36.6300, "lon": -119.6500, "utility": "PGE"},
    "Chowchilla Substation 115 kV":     {"lat": 37.1200, "lon": -120.2600, "utility": "PGE"},
    "Le Grand Substation 115 kV":       {"lat": 37.2300, "lon": -120.2500, "utility": "PGE"},
    "Schindler Substation 115 kV":      {"lat": 36.8000, "lon": -120.0000, "utility": "PGE"},
    "Borden Substation 230 kV":         {"lat": 36.9800, "lon": -120.1100, "utility": "PGE"},
    "Storey Substation 230 kV":         {"lat": 37.0700, "lon": -120.1300, "utility": "PGE"},
    "Merced Substation 115 kV":         {"lat": 37.3020, "lon": -120.4820, "utility": "PGE"},
    "Los Banos Substation 500 kV":      {"lat": 37.0570, "lon": -120.8500, "utility": "PGE"},
    "Mustang Substation 230 kV":        {"lat": 36.7100, "lon": -119.7800, "utility": "PGE"},
    "Henrietta Substation 230 kV":      {"lat": 36.6500, "lon": -119.6500, "utility": "PGE"},
    "Melones Substation 230 kV":        {"lat": 37.9300, "lon": -120.5000, "utility": "PGE"},
    "Cottle Substation 230 kV":         {"lat": 37.2400, "lon": -120.2300, "utility": "PGE"},
}

# ── Regional fallback coordinates when no substation match is found ────────
# Used as last resort; coordinates represent the geographic center of each area
REGIONAL_FALLBACKS = {
    # SCE
    "SCE Northern Interconnection Area":        {"lat": 34.830, "lon": -118.200},
    "SCE Metro Interconnection Area":           {"lat": 33.950, "lon": -118.200},
    "SCE North of Lugo":                        {"lat": 34.350, "lon": -117.850},
    "SCE Eastern Interconnection Area":         {"lat": 34.100, "lon": -116.500},
    "East of Pisgah":                           {"lat": 35.000, "lon": -115.500},
    # SDG&E
    "SDG&E Interconnection Area":               {"lat": 33.000, "lon": -117.100},
    # PG&E
    "PG&E North of Greater Bay":                {"lat": 38.850, "lon": -121.550},
    "PG&E Greater Bay":                         {"lat": 37.850, "lon": -122.100},
    "PG&E Kern":                                {"lat": 35.350, "lon": -119.350},
    "PG&E Fresno":                              {"lat": 36.900, "lon": -120.100},
    "PG&E South":                               {"lat": 37.300, "lon": -121.000},
    # Catch-all
    "Default":                                  {"lat": 36.500, "lon": -119.500},
}

# ── Explicit overrides for constraints that resist keyword extraction ───────
# Keyed on lowercase stripped constraint name
EXPLICIT_COORDS = {
    "sce metro area default constraint":  {"lat": 33.950, "lon": -118.240},
    "south of kramer area constraint":    {"lat": 34.730, "lon": -117.550},  # Kramer Junction
    "dcrt constraint":                    {"lat": 34.488, "lon": -114.356},  # Delaney-Colorado River Tx
    "chicarita 138 kv constraint":        {"lat": 33.050, "lon": -117.120},  # Escondido area
    "el cajon 69 kv constraint":          {"lat": 32.790, "lon": -116.960},
    "ocean ranch 69 kv constraint":       {"lat": 33.220, "lon": -117.380},  # Oceanside
}

# ── Keyword → substation lookup for automatic coordinate inference ─────────
# Maps keywords extracted from constraint names to substation keys.
# Order matters: more-specific matches should come first.
KEYWORD_TO_SUBSTATION = {
    # North Bay / Diablo Range
    "collinsville":    "Collinsville Substation 500 kV",
    "tesla":           "Tesla Substation 500 kV",
    "metcalf":         "Metcalf Substation 500 kV",
    "mosslanding":     "Mossland Substation 500 kV",
    "mossland":        "Mossland Substation 500 kV",
    "diablo":          "Diablo Substation 500 kV",
    # Greater Bay substations
    "dumbarton":       "Dumbarton Substation 230 kV",
    "newark":          "Newark Substation 230 kV",
    "eastshore":       "Eastshore Substation 230 kV",
    "san mateo":       "San Mateo Substation 230 kV",
    "lakeville":       "Lakeville Substation 230 kV",
    "ignacio":         "Ignacio Substation 230 kV",
    "sobrante":        "Sobrante Substation 230 kV",
    "moraga":          "Moraga Substation 230 kV",
    "windmaster":      "Windmaster Substation 230 kV",
    "contra costa":    "Contra Costa Substation 230 kV",
    "birds landing":   "Birds Landing Substation 230 kV",
    "bellota":         "Bellota Substation 230 kV",
    "weber":           "Weber Substation 230 kV",
    "tracy":           "Tracy Pump Substation 230 kV",
    "kasson":          "Kasson Substation 115 kV",
    "heinz":           "Heinz Substation 115 kV",
    "salado":          "Salado Substation 115 kV",
    "crow creek":      "Crow Creek Substation 60 kV",
    "morganhill":      "Morganhill Substation 115 kV",
    "morgan hill":     "Morganhill Substation 115 kV",
    "grant":           "Grant Substation 115 kV",
    "eight mile":      "Eight Mile Substation 230 kV",
    # North of Greater Bay
    "woodland":        "Woodland Substation 115 kV",
    "davis":           "Davis Substation 115 kV",
    "bell":            "Bell Substation 115 kV",
    "placer":          "Placer Substation 115 kV",
    "rocklin":         "Rocklin Substation 115 kV",
    "pleasant grove":  "Pleasant Grove Substation 115 kV",
    "cortina":         "Cortina Substation 115 kV",
    "eagle rock":      "Eagle Rock Substation 115 kV",
    "carberry":        "Carberry Substation 230 kV",
    "round mountain":  "Round Mountain Substation 230 kV",
    "rio oso":         "Rio Oso Substation 230 kV",
    "brighton":        "Brighton Substation 230 kV",
    "lockeford":       "Lockeford Substation 230 kV",
    # Kern
    "gates":           "Gates Substation 500 kV",
    "midway":          "Midway Substation 500 kV",
    "kern":            "Kern Substation 230 kV",
    "smyrna":          "Smyrna Substation 115 kV",
    "atwell":          "Atwell Junction Substation 115 kV",
    "maricopa":        "Maricopa Substation 70 kV",
    "copus":           "Copus Substation 70 kV",
    "taft":            "Taft Substation 115 kV",
    "tevis":           "Tevis Substation 115 kV",
    "stockdale":       "Stockdale Substation 115 kV",
    "lamont":          "Lamont Substation 115 kV",
    "semitropic":      "Semitropic Substation 115 kV",
    "buena vista":     "Buena Vista Junction Substation",
    "cal flat":        "Cal Flat Substation 230 kV",
    "arco":            "Arco Substation 230 kV",
    # Fresno
    "panoche":         "Panoche Substation 230 kV",
    "mendota":         "Mendota Substation 115 kV",
    "oro loma":        "Oro Loma Substation 115 kV",
    "el nido":         "El Nido Substation 115 kV",
    "tranquility":     "Tranquility Substation 230 kV",
    "helm":            "Helm Substation 230 kV",
    "dairyland":       "Dairyland Substation 115 kV",
    "chowchilla":      "Chowchilla Substation 115 kV",
    "le grand":        "Le Grand Substation 115 kV",
    "schindler":       "Schindler Substation 115 kV",
    "borden":          "Borden Substation 230 kV",
    "storey":          "Storey Substation 230 kV",
    "merced":          "Merced Substation 115 kV",
    "los banos":       "Los Banos Substation 500 kV",
    "mustang":         "Mustang Substation 230 kV",
    "henrietta":       "Henrietta Substation 230 kV",
    "melones":         "Melones Substation 230 kV",
    "cottle":          "Cottle Substation 230 kV",
    # SCE
    "antelope":        "Antelope Substation 500 kV",
    "vincent":         "Vincent Substation 500 kV",
    "lugo":            "Lugo Substation 500 kV",
    "magunden":        "Magunden Substation 230 kV",
    "pardee":          "Pardee Substation 230 kV",
    "windhub":         "Windhub Substation 500 kV",
    "moorpark":        "Moorpark Substation 230 kV",
    "hinson":          "Hinson Substation 230 kV",
    "del amo":         "Del Amo Substation 230 kV",
    "devers":          "Devers Substation 500 kV",
    "red bluff":       "Red Bluff Substation 500 kV",
    "colorado river":  "Colorado River Substation 500 kV",
    "etiwanda":        "Etiwanda Substation 230 kV",
    "serrano":         "Serrano Substation 500 kV",
    "eagle mountain":  "Eagle Mountain Substation",
    # SDG&E
    "capistrano":      "Capistrano Substation 230 kV",
    "san onofre":      "San Onofre Substation 230 kV",
    "talega":          "Talega Substation 230 kV",
    "miguel":          "Miguel Substation 500 kV",
    "sunrise":         "Sunrise Substation 230 kV",
    "otay":            "Otay Mesa Substation 230 kV",
    "silvergate":      "Silvergate Substation 230 kV",
    "encina":          "Encina Substation 230 kV",
    "san luis rey":    "San Luis Rey Substation 230 kV",
    "trabuco":         "Trabuco Substation 138 kV",
    # EOP / Nevada
    "eldorado":        "Eldorado Substation 500 kV",
    "sloan canyon":    "Sloan Canyon Substation 500 kV",
    "glw":             "GLW Substation 230 kV",
}

# ═══════════════════════════════════════════════════════════════════════════
# COORDINATE RESOLUTION  (3-tier: explicit map → keyword inference → regional fallback)
# ═══════════════════════════════════════════════════════════════════════════

def keywords_from_name(name: str) -> list[str]:
    """Return lowercase tokens from a constraint name, longest first."""
    name_lower = name.lower()
    # Try multi-word keywords first (longer = more specific)
    matches = []
    for kw in sorted(KEYWORD_TO_SUBSTATION.keys(), key=len, reverse=True):
        if kw in name_lower:
            matches.append(kw)
    return matches


def resolve_coordinates(constraint_name: str, area: str = ""):
    """
    4-tier resolution:
      0. Explicit override table (for constraints that resist keyword matching)
      1. Keyword extraction from constraint name → substation lookup
      2. All matched substations averaged to give a geographic centroid
      3. Regional fallback by area string

    Returns (lon, lat, method) where method is 'substation'|'regional'|None
    """
    name_lower = constraint_name.lower().strip()

    # ── Tier 0: explicit override ──────────────────────────────────────────
    explicit = EXPLICIT_COORDS.get(name_lower)
    if explicit:
        return round(explicit["lon"], 6), round(explicit["lat"], 6), "substation"


    # ── Tier 1 & 2: keyword → substation centroid ─────────────────────────
    matched_sub_names = []
    for kw in sorted(KEYWORD_TO_SUBSTATION.keys(), key=len, reverse=True):
        if kw in name_lower:
            sub_name = KEYWORD_TO_SUBSTATION[kw]
            if sub_name not in matched_sub_names:
                matched_sub_names.append(sub_name)

    # Collect coordinates for all matched substations
    lats, lons = [], []
    for sub_name in matched_sub_names:
        coord = SUBSTATION_COORDS.get(sub_name)
        if coord:
            lats.append(coord["lat"])
            lons.append(coord["lon"])

    if lats:
        lat = round(sum(lats) / len(lats), 6)
        lon = round(sum(lons) / len(lons), 6)
        return lon, lat, "substation"

    # ── Tier 3: regional fallback ──────────────────────────────────────────
    area_upper = area.upper()
    for region_key, coord in REGIONAL_FALLBACKS.items():
        if region_key.upper() in area_upper:
            return round(coord["lon"], 6), round(coord["lat"], 6), "regional"

    # Infer region from area string fragments
    if "SCE" in area_upper and "NORTHERN" in area_upper:
        c = REGIONAL_FALLBACKS["SCE Northern Interconnection Area"]
    elif "SCE" in area_upper and "METRO" in area_upper:
        c = REGIONAL_FALLBACKS["SCE Metro Interconnection Area"]
    elif "SCE" in area_upper and "EASTERN" in area_upper:
        c = REGIONAL_FALLBACKS["SCE Eastern Interconnection Area"]
    elif "SCE" in area_upper and "LUGO" in area_upper:
        c = REGIONAL_FALLBACKS["SCE North of Lugo"]
    elif "PISGAH" in area_upper or "EOP" in area_upper:
        c = REGIONAL_FALLBACKS["East of Pisgah"]
    elif "SDG" in area_upper:
        c = REGIONAL_FALLBACKS["SDG&E Interconnection Area"]
    elif "KERN" in area_upper:
        c = REGIONAL_FALLBACKS["PG&E Kern"]
    elif "FRESNO" in area_upper:
        c = REGIONAL_FALLBACKS["PG&E Fresno"]
    elif "GREATER BAY" in area_upper:
        c = REGIONAL_FALLBACKS["PG&E Greater Bay"]
    elif "NORTH OF GREATER BAY" in area_upper or "NORTH OF GREAT" in area_upper:
        c = REGIONAL_FALLBACKS["PG&E North of Greater Bay"]
    elif "PG&E" in area_upper or "PGE" in area_upper:
        c = REGIONAL_FALLBACKS["PG&E South"]
    else:
        return None, None, None

    return round(c["lon"], 6), round(c["lat"], 6), "regional"


# ═══════════════════════════════════════════════════════════════════════════
# GRIDSTATUS  —  Fetch live CAISO Interconnection Queue
# ═══════════════════════════════════════════════════════════════════════════
#
# Uses the gridstatus Python library (pip install gridstatus), NOT the REST API.
# The REST API uses different dataset slugs and is not needed here.
#
#   caiso = gridstatus.CAISO()
#   df = caiso.get_interconnection_queue()
#
# Standardised columns used:
#   "Point Of Interconnection"  — POI substation name
#   "Fuel Type"                 — e.g. "Solar", "Wind", "Battery Storage"
#   "Capacity (MW)"             — nameplate MW
#   "Status"                    — "Active", "Withdrawn", etc.
#
# Full queue DataFrame is saved to caiso_queue_raw.csv for inspection.

# Gridstatus CAISO queue "Fuel" column values (lowercased for matching):
#   "Solar", "Wind", "Battery Storage", "Photovoltaic" (rare), "Hybrid"
# We do substring matching so "battery storage" catches "battery storage".
QUEUE_FUEL_TYPES = {
    "solar", "photovoltaic",
    "wind",
    "battery storage", "battery", "storage", "bess", "ess",
    "hybrid",
}


def _normalize_poi(name: str) -> str:
    """Lowercase + strip punctuation for POI name matching."""
    return re.sub(r"[^a-z0-9 ]", " ", name.lower()).strip()


def _make_ssl_context():
    """Return an SSL context that works on macOS Python 3.13+ (uses certifi if available)."""
    import ssl
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    # Fallback: try system certs
    try:
        ctx = ssl.create_default_context()
        return ctx
    except Exception:
        pass
    # Last resort: unverified (logs a warning)
    print("   ⚠️  SSL cert verification disabled — run: pip install certifi")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 3 — Wholesale Arbitrage / LMP Price Analytics
#  Computes 3 metrics per CAISO pricing zone from 12 months of hourly DAM LMPs:
#    1. TB4 Spread Rank   — percentile of mean daily (top-4h avg - bottom-4h avg)
#    2. Volatility Rank   — percentile of stdev of all hourly prices
#    3. Neg Price Freq    — percentile of fraction of hours with price < 0
#  Equal-weight score = mean of three percentile ranks (0–100)
# ─────────────────────────────────────────────────────────────────────────────

# 6 nodes: 3 DLAPs + 3 Trading Hubs (one per CAISO zone, all fetchable via gridstatus library)
LMP_NODES: dict[str, str] = {
    # 3 DLAPs (one per CAISO zone) + 3 Trading Hubs — all fetchable via gridstatus library
    "DLAP_PGAE-APND":   "PG&E DLAP (NP15)",
    "DLAP_SCE-APND":    "SCE DLAP (SP15)",
    "DLAP_SDGE-APND":   "SDG&E DLAP (ZP26)",
    "TH_NP15_GEN-APND": "NP15 Hub",
    "TH_SP15_GEN-APND": "SP15 Hub",
    "TH_ZP26_GEN-APND": "ZP26 Hub",
}

# Map node → CAISO zone (for choropleth coloring)
NODE_ZONE: dict[str, str] = {
    "DLAP_PGAE-APND":   "NP15",
    "TH_NP15_GEN-APND": "NP15",
    "DLAP_SCE-APND":    "SP15",
    "TH_SP15_GEN-APND": "SP15",
    "DLAP_SDGE-APND":   "ZP26",
    "TH_ZP26_GEN-APND": "ZP26",
}


def _fetch_lmp_node(api_key: str, node_id: str,
                    start: str, end: str) -> list[dict]:
    """
    Fetch hourly DAM LMP for one node.

    Strategy (tried in order):
      1. gridstatus Python library — pulls from CAISO OASIS directly, supports
         DLAP and Trading Hub location types which are absent from the REST API
         dataset (caiso_lmp_day_ahead_hourly only has Node / AP Node rows).
      2. gridstatus REST API fallback — uses confirmed working params:
         start_time / end_time  (not start / end, which are silently ignored).

    Returns list of dicts with at minimum {interval_start_utc, lmp} keys.
    """
    rows = _fetch_lmp_via_library(node_id, start[:10], end[:10])
    if rows:
        return rows
    print("(library empty — trying REST API fallback)", end=" ", flush=True)
    return _fetch_lmp_via_rest(api_key, node_id, start, end)


def _fetch_lmp_via_library(node_id: str, start_date: str, end_date: str) -> list[dict]:
    """
    Primary: use gridstatus Python library (same one used for queue).
    start_date / end_date: "YYYY-MM-DD" strings.
    """
    try:
        import gridstatus
    except ImportError:
        return []

    try:
        caiso = gridstatus.CAISO()
        df = caiso.get_lmp(
            start=start_date,
            end=end_date,
            market="DAY_AHEAD_HOURLY",
            locations=[node_id],
            verbose=False,
        )
        if df is None or len(df) == 0:
            return []

        rows = []
        for _, row in df.iterrows():
            # Column names differ across gridstatus versions
            ts = (row.get("Interval Start") or row.get("Time") or
                  row.get("interval_start") or row.get("interval_start_utc") or "")
            price = (row.get("LMP") or row.get("lmp") or
                     row.get("LMP ($/MWh)") or row.get("price"))
            if ts and price is not None:
                rows.append({"interval_start_utc": str(ts), "lmp": float(price)})
        return rows

    except Exception as e:
        print(f"(gs-lib err: {type(e).__name__}) ", end="", flush=True)
        return []


def _fetch_lmp_via_rest(api_key: str, node_id: str,
                        start: str, end: str) -> list[dict]:
    """
    Fallback: gridstatus REST API with confirmed working params.
    Diagnostics showed:
      - start_time / end_time  (not start / end) actually scopes dates
      - location filter is silently ignored on this dataset, but date
        scoping alone still gives us useful data for zone-level metrics
    """
    import urllib.request, urllib.parse, json as _json, ssl, time
    try:
        import certifi
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ssl_ctx = ssl.create_default_context()

    base = "https://api.gridstatus.io/v1/datasets/caiso_lmp_day_ahead_hourly/query"
    all_rows: list[dict] = []
    offset = 0
    page_size = 1000

    while True:
        params = urllib.parse.urlencode({
            "start_time": start,   # confirmed working (not "start")
            "end_time":   end,     # confirmed working (not "end")
            "filters":    f'location="{node_id}"',
            "limit":      page_size,
            "offset":     offset,
        })
        req = urllib.request.Request(
            f"{base}?{params}",
            headers={"x-api-key": api_key, "Accept": "application/json"},
        )

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
                    data = _json.loads(resp.read())
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = 10 * (2 ** attempt)
                    print(f"\n   ⏳ 429 rate-limit — waiting {wait}s…", flush=True)
                    time.sleep(wait)
                    if attempt == 2:
                        return all_rows
                else:
                    print(f"\n   ⚠️  REST LMP error {node_id}: HTTP {e.code}")
                    return all_rows
            except Exception as e:
                print(f"\n   ⚠️  REST LMP error {node_id}: {e}")
                return all_rows

        rows = data.get("data") or data.get("results") or []
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
        if offset > 20000:
            break
        time.sleep(0.3)

    return all_rows


def _compute_arbitrage_metrics(rows: list[dict]) -> dict:
    """
    From hourly LMP rows compute the 3 raw arbitrage metrics.
    Returns dict with tb4_spread_mean, volatility_stdev, neg_price_freq,
    plus the raw hourly prices list for cross-node percentile ranking.
    """
    import statistics, collections

    # Extract (date_str, hour, price) — handle various field name spellings
    by_date: dict[str, list[float]] = collections.defaultdict(list)
    all_prices: list[float] = []

    for r in rows:
        # price field: "lmp", "lmp_total", "price", "LMP"
        price_raw = (r.get("lmp") or r.get("lmp_total") or
                     r.get("price") or r.get("LMP") or r.get("lmp_ex_congestion"))
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            continue

        # date key — gridstatus REST API v1 uses interval_start_utc / interval_end_utc
        ts_raw = (r.get("interval_start_utc") or r.get("interval_end_utc") or
                  r.get("interval_start")     or r.get("interval_end") or
                  r.get("timestamp") or "")
        date_key = str(ts_raw)[:10]  # "2024-03-11"
        if not date_key or date_key == "":
            continue

        by_date[date_key].append(price)
        all_prices.append(price)

    if len(all_prices) < 24:
        return {"tb4_spread_mean": None, "volatility_stdev": None,
                "neg_price_freq": None, "sample_hours": 0,
                "all_prices": all_prices}

    # ── Metric 1: TB4 Spread ──────────────────────────────────────────────────
    daily_spreads: list[float] = []
    for date_key, prices in by_date.items():
        if len(prices) < 8:
            continue
        sorted_p = sorted(prices)
        bottom4_avg = sum(sorted_p[:4]) / 4
        top4_avg    = sum(sorted_p[-4:]) / 4
        daily_spreads.append(top4_avg - bottom4_avg)

    tb4_mean = sum(daily_spreads) / len(daily_spreads) if daily_spreads else None

    # ── Metric 2: Volatility ─────────────────────────────────────────────────
    try:
        vol = statistics.stdev(all_prices) if len(all_prices) >= 2 else None
    except statistics.StatisticsError:
        vol = None

    # ── Metric 3: Negative Price Frequency ───────────────────────────────────
    neg_count = sum(1 for p in all_prices if p < 0)
    neg_freq  = neg_count / len(all_prices) if all_prices else None

    return {
        "tb4_spread_mean":  tb4_mean,
        "volatility_stdev": vol,
        "neg_price_freq":   neg_freq,
        "sample_hours":     len(all_prices),
        "all_prices":       all_prices,   # kept for cross-node percentile
    }


def fetch_lmp_arbitrage(api_key: str | None) -> dict:
    """
    Fetch 12-month rolling hourly DAM LMPs for all LMP_NODES, compute
    the 3 arbitrage metrics, rank them as percentiles across the node set,
    and return a zone_scores dict keyed by zone id (NP15, SP15, ZP26).

    Zone score = mean of the 3 equal-weight percentile ranks (0-100).
    Returns:
        {
          "NP15": { tb4_spread_mean, tb4_spread_rank, volatility_stdev, vol_rank,
                    neg_price_freq, neg_rank, score, nodes: [...], fetched_at },
          "SP15": { ... },
          "ZP26": { ... },
          "meta": { status, fetched_at, nodes_fetched }
        }
    """
    if not api_key:
        print("⚠️  No API key — skipping LMP fetch. Zone scores will be placeholder.")
        return _lmp_placeholder()

    import csv as _csv
    from datetime import datetime, timezone, timedelta
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=365)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── LMP cache: raw rows are saved per-node as CSV so re-runs skip re-fetch ─
    # Cache files: lmp_cache_<node_id>_<start_date>_<end_date>.csv
    # Invalidated automatically when the date window shifts (daily rolling year).
    CACHE_DIR = BASE_DIR / "lmp_cache"
    CACHE_DIR.mkdir(exist_ok=True)
    cache_tag = f"{start_str[:10]}_{end_str[:10]}"

    def _cache_path(node_id: str) -> "Path":
        safe_id = node_id.replace("/", "_").replace("\\", "_")
        return CACHE_DIR / f"lmp_{safe_id}_{cache_tag}.csv"

    def _load_cache(node_id: str) -> list[dict] | None:
        p = _cache_path(node_id)
        if not p.exists():
            return None
        try:
            with p.open(newline="") as f:
                rows = list(_csv.DictReader(f))
            if rows:
                return rows
        except Exception:
            pass
        return None

    def _save_cache(node_id: str, rows: list[dict]) -> None:
        if not rows:
            return
        p = _cache_path(node_id)
        try:
            fieldnames = list(rows[0].keys())
            with p.open("w", newline="") as f:
                w = _csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(rows)
        except Exception as e:
            print(f"(cache write failed: {e})", end=" ")

    print(f"   LMP date range: {start_str[:10]} → {end_str[:10]}")
    print(f"   Cache dir: {CACHE_DIR}")

    # ── Fetch all nodes ───────────────────────────────────────────────────────
    raw_metrics: dict[str, dict] = {}
    for i, (node_id, label) in enumerate(LMP_NODES.items()):
        cached = _load_cache(node_id)
        if cached:
            print(f"   LMP cache hit: {label} ({node_id}) → {len(cached)} rows", flush=True)
            rows = cached
        else:
            print(f"   Fetching LMP: {label} ({node_id})…", end=" ", flush=True)
            rows = _fetch_lmp_node(api_key, node_id, start_str, end_str)
            _save_cache(node_id, rows)
        metrics = _compute_arbitrage_metrics(rows)
        raw_metrics[node_id] = metrics
        tb4_disp = f"{metrics['tb4_spread_mean']:.1f}" if metrics['tb4_spread_mean'] is not None else '—'
        vol_disp = f"{metrics['volatility_stdev']:.1f}" if metrics['volatility_stdev'] is not None else '—'
        neg_disp = f"{metrics['neg_price_freq']:.3f}" if metrics['neg_price_freq'] is not None else '—'
        print(f"{metrics['sample_hours']} hrs  tb4={tb4_disp}  vol={vol_disp}  neg={neg_disp}")
        if i < len(LMP_NODES) - 1 and not cached:
            import time as _time; _time.sleep(2)  # 2s between nodes to avoid rate limits

    # ── Cross-node percentile ranking ─────────────────────────────────────────
    def percentile_rank(value: float | None, all_values: list[float | None]) -> float | None:
        """Rank of value among non-None values, 0–100."""
        valid = [v for v in all_values if v is not None]
        if value is None or len(valid) < 2:
            return None
        below = sum(1 for v in valid if v < value)
        return round(100.0 * below / (len(valid) - 1), 1)

    tb4_vals  = [raw_metrics[n]["tb4_spread_mean"]  for n in LMP_NODES]
    vol_vals  = [raw_metrics[n]["volatility_stdev"] for n in LMP_NODES]
    neg_vals  = [raw_metrics[n]["neg_price_freq"]   for n in LMP_NODES]

    node_scores: dict[str, dict] = {}
    for node_id in LMP_NODES:
        m = raw_metrics[node_id]
        tb4_rank = percentile_rank(m["tb4_spread_mean"],  tb4_vals)
        vol_rank = percentile_rank(m["volatility_stdev"], vol_vals)
        neg_rank = percentile_rank(m["neg_price_freq"],   neg_vals)

        ranks = [r for r in [tb4_rank, vol_rank, neg_rank] if r is not None]
        score = round(sum(ranks) / len(ranks), 1) if ranks else None

        node_scores[node_id] = {
            "node_id":          node_id,
            "label":            LMP_NODES[node_id],
            "node_type":        "DLAP" if "DLAP" in node_id else "Hub",
            "zone":             NODE_ZONE.get(node_id, "NP15"),
            "tb4_spread_mean":  round(m["tb4_spread_mean"],  2) if m["tb4_spread_mean"]  is not None else None,
            "tb4_spread_rank":  tb4_rank,
            "volatility_stdev": round(m["volatility_stdev"], 2) if m["volatility_stdev"] is not None else None,
            "vol_rank":         vol_rank,
            "neg_price_freq":   round(m["neg_price_freq"],   4) if m["neg_price_freq"]   is not None else None,
            "neg_rank":         neg_rank,
            "score":            score,
            "sample_hours":     m["sample_hours"],
        }

    # ── Aggregate nodes → zones (average scores within each zone) ────────────
    zone_accum: dict[str, list] = {"NP15": [], "SP15": [], "ZP26": []}
    for node_id, ns in node_scores.items():
        zone = ns["zone"]
        if zone in zone_accum and ns["score"] is not None:
            zone_accum[zone].append(ns)

    fetched_at = datetime.now(timezone.utc).isoformat()
    zone_scores: dict[str, dict] = {}

    for zone_id, nodes in zone_accum.items():
        if not nodes:
            zone_scores[zone_id] = _zone_placeholder(zone_id)
            continue
        avg = lambda key: round(sum(n[key] for n in nodes if n.get(key) is not None)
                                / max(1, sum(1 for n in nodes if n.get(key) is not None)), 2)
        zone_scores[zone_id] = {
            "zone_id":          zone_id,
            "tb4_spread_mean":  avg("tb4_spread_mean"),
            "tb4_spread_rank":  avg("tb4_spread_rank"),
            "volatility_stdev": avg("volatility_stdev"),
            "vol_rank":         avg("vol_rank"),
            "neg_price_freq":   avg("neg_price_freq"),
            "neg_rank":         avg("neg_rank"),
            "score":            avg("score"),
            "nodes":            [n["label"] for n in nodes],
            # ── Per-node breakdown (DLAP + Hub separately) ──────────
            "nodes_detail": [
                {
                    "node_id":          n["node_id"],
                    "label":            n["label"],
                    "node_type":        n["node_type"],
                    "tb4_spread_mean":  n["tb4_spread_mean"],
                    "tb4_spread_rank":  n["tb4_spread_rank"],
                    "volatility_stdev": n["volatility_stdev"],
                    "vol_rank":         n["vol_rank"],
                    "neg_price_freq":   n["neg_price_freq"],
                    "neg_rank":         n["neg_rank"],
                    "score":            n["score"],
                    "sample_hours":     n.get("sample_hours"),
                }
                for n in sorted(nodes, key=lambda x: 0 if x["node_type"] == "DLAP" else 1)
            ],
            "fetched_at":       fetched_at,
            "status":           "ok",
        }
        print(f"   ✅ Zone {zone_id}: score={zone_scores[zone_id]['score']}  "
              f"tb4={zone_scores[zone_id]['tb4_spread_mean']}  "
              f"vol={zone_scores[zone_id]['volatility_stdev']}  "
              f"neg={zone_scores[zone_id]['neg_price_freq']}")

    zone_scores["meta"] = {
        "status":       "ok",
        "fetched_at":   fetched_at,
        "nodes_fetched": sum(1 for m in raw_metrics.values() if m["sample_hours"] > 0),
    }
    return zone_scores


def _zone_placeholder(zone_id: str) -> dict:
    return {"zone_id": zone_id, "tb4_spread_mean": None, "tb4_spread_rank": None,
            "volatility_stdev": None, "vol_rank": None, "neg_price_freq": None,
            "neg_rank": None, "score": None, "nodes": [], "nodes_detail": [],
            "fetched_at": None, "status": "placeholder"}


def _lmp_placeholder() -> dict:
    return {
        "NP15": _zone_placeholder("NP15"),
        "SP15": _zone_placeholder("SP15"),
        "ZP26": _zone_placeholder("ZP26"),
        "meta": {"status": "placeholder", "fetched_at": None, "nodes_fetched": 0},
    }


def fetch_caiso_queue(api_key: str | None):
    """
    Fetch the CAISO interconnection queue via the gridstatus Python library.
    (pip install gridstatus)

    Returns (poi_mw, poi_count, meta):
      poi_mw    — dict: normalised POI name → total active queue MW
      poi_count — dict: normalised POI name → project count
      meta      — dict with status, fetched_at, total_projects

    Full raw DataFrame is saved to caiso_queue_raw.csv next to the outputs.
    """
    poi_mw:    dict[str, float] = {}
    poi_count: dict[str, int]   = {}
    meta = {"status": "unavailable", "fetched_at": None, "total_projects": 0}

    try:
        import gridstatus  # pip install gridstatus
    except ImportError:
        print("⚠️  gridstatus library not installed — run: pip install gridstatus")
        print("   Skipping live queue fetch.")
        return poi_mw, poi_count, meta

    try:
        print("   Calling gridstatus.CAISO().get_interconnection_queue() …")
        caiso = gridstatus.CAISO()
        df = caiso.get_interconnection_queue()
        df.columns = [c.strip() for c in df.columns]

        # ── Save full raw queue to CSV ────────────────────────────────────────
        raw_out = OUTPUT_DIR / "caiso_queue_raw.csv"
        df.to_csv(raw_out, index=False)
        print(f"   💾 Full queue saved → {raw_out}  ({len(df)} rows, {len(df.columns)} cols)")

        # ── Real gridstatus CAISO column names (confirmed from PublicQueueReport.xlsx):
        #   Status               → "Status"  values: "ACTIVE", "WITHDRAWN", "COMPLETED"
        #   POI                  → "Interconnection Location"  e.g. "Birds Landing 230 kV"
        #   Fuel (split)         → "Fuel-1", "Fuel-2", "Fuel-3"  e.g. "Wind Turbine", "Battery"
        #   MW per fuel (split)  → "MW-1", "MW-2", "MW-3"  (already floats)
        #   Total project MW     → "Capacity (MW)"  (float)

        # ── Filter: ACTIVE status (uppercase in this dataset) ────────────────
        active_df = df[df["Status"].astype(str).str.strip().str.upper() == "ACTIVE"]
        print(f"   Active rows: {len(active_df)} / {len(df)} total")

        matched = 0
        skipped_fuel = 0
        skipped_poi  = 0

        for _, row in active_df.iterrows():
            # POI — "Interconnection Location": "Birds Landing 230 kV"
            poi_raw = str(row.get("Interconnection Location", "") or "").strip()
            if not poi_raw or poi_raw.lower() in ("nan", "none", ""):
                skipped_poi += 1
                continue

            # Fuel — check Fuel-1/2/3; keep row if ANY slot matches our target fuels
            fuel_slots = [
                str(row.get("Fuel-1", "") or "").lower().strip(),
                str(row.get("Fuel-2", "") or "").lower().strip(),
                str(row.get("Fuel-3", "") or "").lower().strip(),
            ]
            matched_slots = [
                i for i, f in enumerate(fuel_slots)
                if f and f not in ("nan", "none")
                and any(ft in f for ft in QUEUE_FUEL_TYPES)
            ]
            if not matched_slots:
                skipped_fuel += 1
                continue

            # MW — sum only the slots that matched our fuel filter
            mw_cols = ["MW-1", "MW-2", "MW-3"]
            mw = sum(
                safe_float(row.get(mw_cols[i], 0)) or 0.0
                for i in matched_slots
            )
            # Fall back to total Capacity (MW) if all MW slots are zero/null
            if mw == 0.0:
                mw = safe_float(row.get("Capacity (MW)", 0)) or 0.0

            poi_key = _normalize_poi(poi_raw)
            poi_mw[poi_key]    = poi_mw.get(poi_key, 0.0) + mw
            poi_count[poi_key] = poi_count.get(poi_key, 0) + 1
            matched += 1

        fetched_at = datetime.now(timezone.utc).isoformat()
        meta = {
            "status":         "ok",
            "fetched_at":     fetched_at,
            "total_projects": matched,
        }
        print(f"   ↳ matched={matched}  skipped_fuel={skipped_fuel}  skipped_poi={skipped_poi}")
        print(f"✅ Gridstatus: {matched} active Solar/Wind/Storage projects "
              f"across {len(poi_mw)} POIs  [{fetched_at}]")

    except Exception as e:
        import traceback
        print(f"⚠️  Gridstatus fetch failed ({type(e).__name__}: {e}) — skipping queue data.")
        traceback.print_exc()

    return poi_mw, poi_count, meta


def match_queue_to_constraint(constraint_name: str, poi_mw: dict[str, float]) \
        -> tuple[float, int]:
    """
    Match a constraint name to POI entries in the queue dict.

    Strategy:
      1. Exact normalised match
      2. Constraint name words found in POI name (≥ 1 significant word match)
      3. POI name words found in constraint name

    Returns (total_matched_mw, project_count).
    """
    if not poi_mw:
        return 0.0, 0

    c_norm  = _normalize_poi(constraint_name)
    c_words = {w for w in c_norm.split() if len(w) > 3}

    total_mw = 0.0
    total_ct = 0
    seen: set[str] = set()

    for poi_key, mw in poi_mw.items():
        if poi_key in seen:
            continue
        poi_words = {w for w in poi_key.split() if len(w) > 3}

        # Exact match
        if c_norm == poi_key:
            total_mw += mw
            total_ct += poi_mw.get(poi_key, 0)
            seen.add(poi_key)
            continue

        # Word overlap — at least one significant word from constraint in POI
        if c_words and poi_words and c_words & poi_words:
            total_mw += mw
            seen.add(poi_key)

    return round(total_mw, 2), len(seen)


# ═══════════════════════════════════════════════════════════════════════════
# METRIC CALCULATIONS
# ═══════════════════════════════════════════════════════════════════════════

CO2_INTENSITY = {
    "Solar":       0.0,
    "Wind":        0.0,
    "Wind/Solar":  0.0,
    "Storage":     0.0,
    "Geothermal":  18.0,
    "Natural Gas": 116.9,
    "Coal":        205.7,
    "Nuclear":     0.0,
}
NG_HEAT_RATE_BTU_KWH = 6_800


def safe_float(val):
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def calc_queue_to_peak_ratio(available_mw, plan_mw):
    if available_mw is None or plan_mw is None or plan_mw == 0:
        return None
    return round(available_mw / plan_mw, 4)


def calc_queue_congestion(active_queue_mw, plan_mw):
    """
    Queue Congestion = Total Active Queue MW / Total Plan Capability.
    Higher value → more demand relative to capacity → more congested.
    Returns None when plan_mw is 0 or unavailable.
    """
    if active_queue_mw is None or plan_mw is None or plan_mw <= 0:
        return None
    return round(active_queue_mw / plan_mw, 4)


def calc_constraint_severity(allocated_mw, plan_mw):
    if allocated_mw is None or plan_mw is None or plan_mw == 0:
        return None
    return round(min(allocated_mw / plan_mw, 1.0), 4)


def calc_co2_displacement(plan_mw, resource_type):
    if plan_mw is None or plan_mw <= 0:
        return None
    cf_map = {"Solar": 0.25, "Wind": 0.35, "Wind/Solar": 0.30, "Mixed": 0.30,
              "Storage": 0.20, "Geothermal": 0.85, "Unknown": 0.28}
    cf = cf_map.get(resource_type, 0.28)
    annual_mwh   = plan_mw * 8760 * cf
    ng_mmbtu_mwh = NG_HEAT_RATE_BTU_KWH / 1_000_000 * 1000
    displaced    = annual_mwh * ng_mmbtu_mwh
    res_co2      = CO2_INTENSITY.get(resource_type.split("/")[0], 0)
    lbs          = displaced * (CO2_INTENSITY["Natural Gas"] - res_co2)
    return round(lbs / 2204.62, 0)


def severity_label(ratio):
    if ratio is None:
        return "Unknown"
    if ratio >= 0.90:
        return "Critical"
    if ratio >= 0.70:
        return "High"
    if ratio >= 0.40:
        return "Moderate"
    return "Low"


def get_color_by_severity(sev):
    return {"Critical": "#ef4444", "High": "#f97316", "Moderate": "#eab308",
            "Low": "#22c55e", "Unknown": "#6b7280"}.get(sev, "#6b7280")


# ═══════════════════════════════════════════════════════════════════════════
# PARSE ATTACHMENT A
# ═══════════════════════════════════════════════════════════════════════════

def parse_attachment_a():
    df = pd.read_excel(ATTACHMENT_A, sheet_name=0, header=None)
    constraints = []
    current_area = None

    for idx in range(3, len(df)):
        row  = df.iloc[idx]
        name = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

        if name and pd.isna(row.iloc[3]) and pd.isna(row.iloc[1]):
            current_area = name
            continue
        if not name or name.lower() in ("nan", ""):
            continue
        if any(x in name for x in ("Transmission Plan", "Allocated", "Available")):
            continue

        resource_type = str(row.iloc[11]).strip() if pd.notna(row.iloc[11]) else "Unknown"
        if resource_type in ("Unknown", "nan", ""):
            res = (str(row.iloc[1]) if pd.notna(row.iloc[1]) else "").lower()
            resource_type = ("Wind/Solar" if ("wind" in res or "tehachapi" in res)
                             else "Solar" if "solar" in res
                             else "Mixed")

        constraints.append({
            "constraint_name":    name,
            "area":               current_area or "Unknown",
            "affected_resources": str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else "",
            "binding_condition":  str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else "",
            # FCDS = Full Capacity Deliverability Status — the plan ceiling
            "plan_capability_mw": safe_float(row.iloc[3]),
            "adnu_increment_mw":  safe_float(row.iloc[4]),
            "adnu_description":   str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else "",
            "adnu_cost_m":        safe_float(row.iloc[6]),
            "eods_capability_mw": safe_float(row.iloc[7]),
            "resource_type":      resource_type,
        })

    return constraints


# ═══════════════════════════════════════════════════════════════════════════
# PARSE ATTACHMENT B  (queue matrix → available / allocated TPD)
# ═══════════════════════════════════════════════════════════════════════════

def parse_attachment_b(sheet_name):
    df = pd.read_excel(ATTACHMENT_B, sheet_name=sheet_name, header=None)

    header_row = None
    for i, row in df.iterrows():
        if any("POI Name" in str(c) for c in row):
            header_row = i
            break
    if header_row is None:
        return {}

    col_headers   = df.iloc[header_row]
    plan_row      = df.iloc[header_row + 1] if header_row + 1 < len(df) else None
    allocated_row = df.iloc[header_row + 2] if header_row + 2 < len(df) else None
    available_row = df.iloc[header_row + 3] if header_row + 3 < len(df) else None

    constraint_data = {}
    for col_idx in range(3, len(col_headers)):
        col_name = str(col_headers.iloc[col_idx]).strip()
        if not col_name or col_name.lower() == "nan":
            continue
        constraint_data[col_name] = {
            "plan_capability_mw": safe_float(plan_row.iloc[col_idx])      if plan_row      is not None else None,
            "allocated_tpd_mw":   safe_float(allocated_row.iloc[col_idx]) if allocated_row is not None else None,
            "available_tpd_mw":   safe_float(available_row.iloc[col_idx]) if available_row is not None else None,
        }

    poi_data_start = header_row + 4
    poi_rows = df.iloc[poi_data_start:]
    poi_counts = {k: 0 for k in constraint_data}
    for _, row in poi_rows.iterrows():
        for col_idx in range(3, len(col_headers)):
            col_name = str(col_headers.iloc[col_idx]).strip()
            if col_name in poi_counts:
                val = str(row.iloc[col_idx]).strip()
                if val in ("√", "1", "X", "x", "✓"):
                    poi_counts[col_name] += 1
    for k in constraint_data:
        constraint_data[k]["queued_poi_count"] = poi_counts.get(k, 0)

    return constraint_data


# ═══════════════════════════════════════════════════════════════════════════
# ATTACHMENT B MATCHING  (robust multi-strategy)
# ═══════════════════════════════════════════════════════════════════════════

def _normalize(s: str) -> str:
    """Lowercase, strip punctuation/spaces for fuzzy comparison."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def match_b_record(name: str, b_data: dict):
    """
    3-strategy match against Attachment B column headers:
      1. Exact normalized match
      2. Significant-word overlap (≥50% of words with len>4)
      3. Longest common substring score
    Returns the best matching record or None.
    """
    name_norm = _normalize(name)
    name_words = {w for w in re.split(r"\W+", name.lower()) if len(w) > 4}

    best_score = 0
    best_match = None

    for bkey, bval in b_data.items():
        bkey_norm  = _normalize(bkey)
        bkey_words = {w for w in re.split(r"\W+", bkey.lower()) if len(w) > 4}

        # Strategy 1: exact
        if name_norm == bkey_norm:
            return bval

        # Strategy 2: word overlap ratio
        if name_words and bkey_words:
            overlap = len(name_words & bkey_words)
            ratio   = overlap / max(len(name_words), len(bkey_words))
            if ratio > best_score:
                best_score = ratio
                best_match = bval

        # Strategy 3: substring — name normalized contains bkey normalized
        if len(bkey_norm) > 6 and bkey_norm in name_norm:
            if 0.9 > best_score:
                best_score = 0.9
                best_match = bval
        if len(name_norm) > 6 and name_norm in bkey_norm:
            if 0.85 > best_score:
                best_score = 0.85
                best_match = bval

    # Accept if overlap ≥ 40%
    return best_match if best_score >= 0.40 else None


# ═══════════════════════════════════════════════════════════════════════════
# BUILD GEOJSON FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def build_features(constraints_a, b_all, poi_mw=None, poi_count_map=None, queue_meta=None):
    features = []
    poi_mw         = poi_mw or {}
    poi_count_map  = poi_count_map or {}

    for c in constraints_a:
        name = c["constraint_name"]
        area = c["area"]

        # ── Merge Attachment B data ────────────────────────────────────────
        b_match = match_b_record(name, b_all)

        plan_mw      = c.get("plan_capability_mw")
        allocated_mw = b_match["allocated_tpd_mw"]  if b_match else None
        available_mw = b_match["available_tpd_mw"]  if b_match else None
        queued_poi   = b_match["queued_poi_count"]   if b_match else 0

        # ── Prefer Att B plan cap when Att A is missing ────────────────────
        if plan_mw is None and b_match:
            plan_mw = b_match.get("plan_capability_mw")

        # ── Layer 02: Queue Congestion from Gridstatus live data ───────────
        active_queue_mw, gs_match_count = match_queue_to_constraint(name, poi_mw)
        queue_congestion = calc_queue_congestion(active_queue_mw, plan_mw)

        # ── Legacy queue-to-peak ratio (kept for backward compat) ─────────
        qpr      = calc_queue_to_peak_ratio(available_mw, plan_mw)
        severity = calc_constraint_severity(allocated_mw, plan_mw)
        sev_lbl  = severity_label(severity)
        co2_disp = calc_co2_displacement(plan_mw, c["resource_type"])

        # ── Coordinate resolution (3-tier) ────────────────────────────────
        lon, lat, coord_method = resolve_coordinates(name, area)

        # ── Infer utility from area ────────────────────────────────────────
        if "SDG" in area:
            utility = "SDG&E"
        elif "PG&E" in area or "PGE" in area:
            utility = "PG&E"
        elif "SCE" in area:
            utility = "SCE"
        else:
            utility = "Unknown"

        # ── Anchor substations (keyword-inferred) ─────────────────────────
        anchor_subs = []
        for kw in sorted(KEYWORD_TO_SUBSTATION.keys(), key=len, reverse=True):
            if kw in name.lower():
                sub = KEYWORD_TO_SUBSTATION[kw]
                if sub not in anchor_subs:
                    anchor_subs.append(sub)

        feat = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat] if lon is not None else [None, None],
            },
            "properties": {
                # Identity
                "id":                  re.sub(r"[^a-z0-9]", "_", name.lower()),
                "constraint_name":     name,
                "area":                area,
                "utility":             utility,
                "affected_resources":  c["affected_resources"],
                "resource_type":       c["resource_type"],
                "binding_condition":   c["binding_condition"],

                # ── Layer 01: Available Capacity (primary sizing metric) ───
                # available_tpd_mw = remaining headroom (Att B Available row)
                # plan_capability_mw = FCDS ceiling (Att A col D)
                "available_tpd_mw":    available_mw,
                "plan_capability_mw":  plan_mw,
                "allocated_tpd_mw":    allocated_mw,
                "adnu_increment_mw":   c["adnu_increment_mw"],
                "adnu_description":    c["adnu_description"],
                "adnu_cost_m":         c["adnu_cost_m"],
                "eods_capability_mw":  c.get("eods_capability_mw"),

                # ── Layer 02: Queue Congestion (Gridstatus live) ───────────
                # queue_congestion_ratio = active_queue_mw / plan_capability_mw
                # Dot SIZE  = active_queue_mw (how much is trying to interconnect)
                # Dot COLOR = available_tpd_mw (how much capacity headroom remains)
                "queue_congestion_ratio": queue_congestion,
                "active_queue_mw":        active_queue_mw,
                "active_queue_count":     gs_match_count,
                "queue_data_vintage":     (queue_meta or {}).get("fetched_at"),

                # Legacy fields (kept for tooltip + Layer 03 reference)
                "queue_to_peak_ratio": qpr,
                "queued_poi_count":    queued_poi,

                # ── Layer 03: Severity ────────────────────────────────────
                "constraint_severity_index": severity,
                "severity_label":            sev_lbl,
                "color":                     get_color_by_severity(sev_lbl),

                # ── Layer 04: CO₂ / Heat Displacement ─────────────────────
                "co2_displacement_mtons_yr": co2_disp,
                "co2_per_mw": round(co2_disp / plan_mw, 1) if (co2_disp and plan_mw) else None,

                # Metadata
                "anchor_substations": anchor_subs,
                "coord_method":       coord_method,   # 'substation' | 'regional' | None
                "has_coords":         lon is not None,
            }
        }
        features.append(feat)

    return features


def build_substation_features():
    feats = []
    for name, coord in SUBSTATION_COORDS.items():
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [coord["lon"], coord["lat"]]},
            "properties": {
                "name":    name,
                "utility": coord["utility"],
                "type":    "substation",
                "kv":      int(re.search(r"(\d+)\s*kV", name).group(1))
                           if re.search(r"(\d+)\s*kV", name) else None,
            }
        })
    return feats


# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY STATS
# ═══════════════════════════════════════════════════════════════════════════

def compute_summary(features):
    total_plan      = sum(f["properties"]["plan_capability_mw"]        or 0 for f in features)
    total_available = sum(f["properties"]["available_tpd_mw"]          or 0 for f in features)
    total_co2       = sum(f["properties"]["co2_displacement_mtons_yr"] or 0 for f in features)
    total_adnu_cost = sum(f["properties"]["adnu_cost_m"]               or 0 for f in features)
    sev_counts      = {}
    for f in features:
        s = f["properties"]["severity_label"]
        sev_counts[s] = sev_counts.get(s, 0) + 1

    located     = sum(1 for f in features if f["properties"]["has_coords"])
    sub_located = sum(1 for f in features
                      if f["properties"].get("coord_method") == "substation")
    reg_located = sum(1 for f in features
                      if f["properties"].get("coord_method") == "regional")

    return {
        "total_constraints":           len(features),
        "total_plan_capability_mw":    round(total_plan),
        "total_available_tpd_mw":      round(total_available),
        "total_co2_displacement_mtons": round(total_co2),
        "total_adnu_cost_million":     round(total_adnu_cost, 1),
        "severity_distribution":       sev_counts,
        "constraints_with_coords":     located,
        "coords_from_substation":      sub_located,
        "coords_from_regional_fallback": reg_located,
        "data_vintage":                "CPUC IRP 2024 (revised 2024-08-28)",
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def run():
    print("📡 Parsing Attachment A…")
    constraints_a = parse_attachment_a()
    print(f"   → {len(constraints_a)} constraints parsed")

    print("📡 Parsing Attachment B — Southern Matrix…")
    b_south = parse_attachment_b("Southern_Substation_List_Matrix")
    print(f"   → {len(b_south)} constraint columns (south)")

    print("📡 Parsing Attachment B — Northern Matrix…")
    b_north = parse_attachment_b("Northern_Substation_List_Matrix")
    print(f"   → {len(b_north)} constraint columns (north)")

    b_all = {**b_south, **b_north}

    print("🌐 Fetching CAISO interconnection queue from Gridstatus…")
    poi_mw, poi_count_map, queue_meta = fetch_caiso_queue(GRIDSTATUS_API_KEY)

    print("💲 Fetching 12-month CAISO LMP data for arbitrage layer…")
    zone_scores = fetch_lmp_arbitrage(GRIDSTATUS_API_KEY)

    print("🔧 Building features & resolving coordinates…")
    features            = build_features(constraints_a, b_all, poi_mw, poi_count_map, queue_meta)
    substation_features = build_substation_features()
    summary             = compute_summary(features)

    # Add queue metadata to summary
    summary["queue_data_status"]   = queue_meta.get("status", "unavailable")
    summary["queue_fetched_at"]    = queue_meta.get("fetched_at")
    summary["queue_total_projects"] = queue_meta.get("total_projects", 0)

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            **summary,
            "layers": {
                "01": "Available Capacity — available_tpd_mw (dot size & score)",
                "02": "Queue Congestion Forecast — queue_congestion_ratio (active_queue_mw / plan_capability_mw)",
                "03": "Constraint Severity Index — constraint_severity_index",
                "04": "Industrial Heat / CO₂ Displacement — co2_displacement_mtons_yr",
            }
        },
        "features": features,
    }

    sub_geojson = {
        "type": "FeatureCollection",
        "metadata": {"description": "HIFLD + PG&E Electric Substation anchor points"},
        "features": substation_features,
    }

    out_combined = OUTPUT_DIR / "combined_output.geojson"
    out_s        = OUTPUT_DIR / "substations.geojson"
    out_xlsx     = OUTPUT_DIR / "intelligence_layers.xlsx"

    with open(out_combined, "w") as f:
        json.dump(geojson, f, indent=2, default=str)
    with open(out_s, "w") as f:
        json.dump(sub_geojson, f, indent=2, default=str)

    print(f"✅ Written: {out_combined}")
    print(f"✅ Written: {out_s}")

    # ── Build a flat list of dicts from features for pandas ──────────────────
    rows = []
    for feat in features:
        p      = feat["properties"]
        coords = feat.get("geometry", {}).get("coordinates", [None, None])
        rows.append({**p, "longitude": coords[0], "latitude": coords[1]})
    df_all = pd.DataFrame(rows)

    # ── Per-layer column definitions ─────────────────────────────────────────
    # Shared identity columns prepended to every sheet
    ID_COLS = ["constraint_name", "area", "utility", "latitude", "longitude", "coord_method"]

    LAYER_SHEETS = {
        # Layer 01 — Available TPD Capacity  (Attachment A + B)
        "L01_Available_Capacity": ID_COLS + [
            "plan_capability_mw",
            "allocated_tpd_mw",
            "available_tpd_mw",
            "available_pct",
            "adnu_increment_mw",
            "adnu_description",
            "adnu_cost_m",
            "eods_capability_mw",
            "affected_resources",
            "resource_type",
            "binding_condition",
        ],
        # Layer 02 — Queue Congestion  (Gridstatus live)
        "L02_Queue_Congestion": ID_COLS + [
            "active_queue_mw",
            "active_queue_count",
            "queue_congestion_ratio",
            "plan_capability_mw",       # denominator — useful to show
            "available_tpd_mw",         # remaining headroom context
            "queue_data_vintage",
        ],
        # Layer 03 slot intentionally removed from this loop —
        # L3 Arbitrage_Opportunity is zone-level (NP15/SP15/ZP26), not per-constraint,
        # so it is written separately below from zone_scores.
        # Layer 04 — CO₂ / Heat Displacement
        "L04_Industrial_Heat": ID_COLS + [
            "co2_displacement_mtons_yr",
        ],
    }

    def _autosize(ws):
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 50)

    try:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            # ── L01, L02, L04: constraint-level sheets ────────────────────────
            for sheet_name, cols in LAYER_SHEETS.items():
                present = [c for c in cols if c in df_all.columns]
                sheet_df = df_all[present].copy()
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                _autosize(writer.sheets[sheet_name])

            # ── L3 Arbitrage_Opportunity: zone-level LMP metrics ─────────────
            # zone_scores is keyed by NP15/SP15/ZP26 — one row per zone.
            # Column names match user's LAYER_SHEETS spec; field mapping below.
            l3_rows = []
            for zone_id in ("NP15", "SP15", "ZP26"):
                zs = zone_scores.get(zone_id, {})
                l3_rows.append({
                    "lmp_zone":        zone_id,
                    "zone_label":      {"NP15": "Northern CA (PG&E)",
                                        "SP15": "Southern CA (SCE/SDG&E)",
                                        "ZP26": "Central CA (Valley)"}[zone_id],
                    "nodes_used":      ", ".join(zs.get("nodes", [])),
                    "arbitrage_score": zs.get("score"),          # 0–100 equal-weight avg
                    "tb4_spread_rank": zs.get("tb4_spread_rank"),
                    "vol_rank":        zs.get("vol_rank"),
                    "neg_price_rank":  zs.get("neg_rank"),       # neg_rank → neg_price_rank
                    "tb4_spread_mean": zs.get("tb4_spread_mean"),
                    "volatility_stdev":zs.get("volatility_stdev"),
                    "neg_price_freq":  zs.get("neg_price_freq"),
                    "fetched_at":      zs.get("fetched_at"),
                    "status":          zs.get("status"),
                })
            df_l3 = pd.DataFrame(l3_rows)
            df_l3.to_excel(writer, sheet_name="L3 Arbitrage_Opportunity", index=False)
            _autosize(writer.sheets["L3 Arbitrage_Opportunity"])

        n_sheets = len(LAYER_SHEETS) + 1  # +1 for L3 Arbitrage_Opportunity
        print(f"✅ Written: {out_xlsx}  ({len(df_all)} constraint rows, {n_sheets} sheets)")
    except ImportError:
        print("⚠️  openpyxl not installed — run: pip install openpyxl")
        print("   Falling back to single CSV export.")
        out_csv = OUTPUT_DIR / "intelligence_layers.csv"
        df_all.to_csv(out_csv, index=False)
        print(f"✅ Written: {out_csv}")
    print(f"\n📊 Summary:")
    for k, v in summary.items():
        print(f"   {k}: {v}")

    # Diagnostic: show which constraints still lack substation-level coords
    missing = [f["properties"]["constraint_name"] for f in features
               if f["properties"].get("coord_method") != "substation"]
    if missing:
        print(f"\n⚠️  {len(missing)} constraints using regional/no coords:")
        for m in missing:
            method = next(f["properties"].get("coord_method") for f in features
                          if f["properties"]["constraint_name"] == m)
            print(f"   [{method or 'NONE'}] {m}")

    # Diagnostic: show queue congestion hits
    matched = [(f["properties"]["constraint_name"],
                f["properties"]["active_queue_mw"],
                f["properties"]["queue_congestion_ratio"])
               for f in features if f["properties"]["active_queue_mw"] > 0]
    if matched:
        print(f"\n📋 Queue Congestion matches ({len(matched)} constraints with live queue data):")
        for nm, mw, ratio in sorted(matched, key=lambda x: x[2] or 0, reverse=True)[:15]:
            print(f"   [{ratio:.2f}x] {nm}  ({mw:,.0f} MW active)")

    # ── Inject zone scores + zone GeoJSON into HTML ──────────────────────────
    # Simplified NP15/SP15/ZP26 polygon boundaries (county-level approximation)
    zone_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"zone_id": "NP15", "name": "NP15",
                               "label": "Northern CA (PG&E)"},
                "geometry": {"type": "Polygon", "coordinates": [[
                    [-124.5, 42.0], [-119.9, 42.0], [-119.9, 39.0],
                    [-121.5, 37.3], [-122.4, 37.3], [-122.8, 37.8],
                    [-123.8, 39.5], [-124.5, 40.5], [-124.5, 42.0],
                ]]}
            },
            {
                "type": "Feature",
                "properties": {"zone_id": "ZP26", "name": "ZP26",
                               "label": "Central CA (Valley)"},
                "geometry": {"type": "Polygon", "coordinates": [[
                    [-121.5, 37.3], [-119.9, 39.0], [-117.5, 37.5],
                    [-117.5, 35.5], [-119.0, 35.0], [-120.5, 35.0],
                    [-121.5, 36.0], [-121.5, 37.3],
                ]]}
            },
            {
                "type": "Feature",
                "properties": {"zone_id": "SP15", "name": "SP15",
                               "label": "Southern CA (SCE/SDG&E)"},
                "geometry": {"type": "Polygon", "coordinates": [[
                    [-121.5, 36.0], [-120.5, 35.0], [-119.0, 35.0],
                    [-117.5, 35.5], [-114.6, 35.0], [-114.6, 32.5],
                    [-117.1, 32.5], [-118.5, 33.7], [-120.5, 34.5],
                    [-121.5, 35.0], [-121.5, 36.0],
                ]]}
            },
        ]
    }

    # Attach zone score to each zone GeoJSON feature
    for feat in zone_geojson["features"]:
        zid = feat["properties"]["zone_id"]
        zs  = {k: v for k, v in (zone_scores.get(zid) or {}).items()
               if k != "nodes"}
        feat["properties"].update(zs)

    # Try to inject into the companion HTML file
    html_candidates = [
        OUTPUT_DIR / "caiso_tx_intelligence_v4.html",
        Path(__file__).parent / "caiso_tx_intelligence_v4.html",
    ]
    for html_path in html_candidates:
        if not html_path.exists():
            continue
        try:
            html = html_path.read_text(encoding="utf-8")
            zone_scores_clean = {k: v for k, v in zone_scores.items() if k != "meta"}
            new_zone_scores = f"const ZONE_SCORES = {json.dumps(zone_scores_clean, default=str)};"
            new_zone_geo    = f"const ZONE_GEOJSON = {json.dumps(zone_geojson, default=str)};"

            # Replace existing constants or inject before SUB_FEATURES if absent.
            # re.DOTALL allows .*? to span newlines in case JSON is pretty-printed.
            import re as _re
            _F = _re.DOTALL
            if "const ZONE_SCORES" in html:
                html = _re.sub(r"const ZONE_SCORES\s*=\s*\{.*?\};",
                               new_zone_scores, html, flags=_F)
            else:
                html = html.replace("const SUB_FEATURES",
                                    new_zone_scores + "\n" + new_zone_geo + "\nconst SUB_FEATURES")

            if "const ZONE_GEOJSON" in html:
                html = _re.sub(r"const ZONE_GEOJSON\s*=\s*\{.*?\};",
                               new_zone_geo, html, flags=_F)

            html_path.write_text(html, encoding="utf-8")
            print(f"✅ Zone scores + GeoJSON injected into {html_path.name}")
        except Exception as e:
            print(f"⚠️  Could not update HTML at {html_path}: {e}")

    return geojson, sub_geojson


if __name__ == "__main__":
    run()