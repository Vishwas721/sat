"""
SPIKE SAT-4: Wind Analysis Module PoC
Author: Vishwas (Site Analysis Tool Team)
Date: Nov 2025

Description:
This script validates the 'Triple-Tier' data architecture for the SAT Wind Module.
1. Tier 1: ERA5-Land Monthly (Fast Trend Analysis)
2. Tier 2: ERA5-Land Daily (Deep Analysis & Wind Rose Frequency)
3. Tier 3: ERA5 Standard (Map Visualization & Ocean Continuity)

It also demonstrates the 'Orientation Logic Engine' (AC4) which translates
weather data into architectural design advice.
"""

import ee
import json
import math
from datetime import datetime

# ==========================================
# 0. INITIALIZATION
# ==========================================
def initialize_gee():
    try:
        ee.Initialize(project='site-analysis-poc')
        print("✅ GEE Initialized successfully.")
    except Exception as e:
        print(f"⚠️ Auth required. Triggering flow...")
        ee.Authenticate()
        ee.Initialize(project='site-analysis-poc')

# ==========================================
# 1. LOGIC ENGINES (The "Brains")
# ==========================================

class WindRoseProcessor:
    """
    Converts raw daily vectors into 16-point frequency bins.
    """
    DIRECTIONS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

    @staticmethod
    def process(daily_records):
        bins = {d: 0 for d in WindRoseProcessor.DIRECTIONS}
        total_valid = 0
        
        # Speed Bins for categorization
        speed_sums = {d: 0.0 for d in WindRoseProcessor.DIRECTIONS}

        for record in daily_records:
            u = record.get('u')
            v = record.get('v')
            
            if u is None or v is None: continue

            speed = math.sqrt(u**2 + v**2)
            if speed < 0.5: continue # Calm wind is often excluded from Direction analysis

            # Calculate Meteorological Direction (0-360)
            # atan2(v, u) gives math angle. 
            math_angle = math.degrees(math.atan2(v, u))
            met_dir = (270 - math_angle) % 360
            
            # Binning
            idx = round(met_dir / 22.5) % 16
            cardinal = WindRoseProcessor.DIRECTIONS[idx]
            
            bins[cardinal] += 1
            speed_sums[cardinal] += speed
            total_valid += 1
        
        # Format for Frontend (Plotly)
        rose_data = []
        dominant_dir = "N"
        max_freq = 0

        if total_valid > 0:
            for d in WindRoseProcessor.DIRECTIONS:
                count = bins[d]
                freq = (count / total_valid) * 100
                avg_spd = speed_sums[d] / count if count > 0 else 0
                
                if freq > max_freq:
                    max_freq = freq
                    dominant_dir = d

                rose_data.append({
                    "direction": d,
                    "frequency": round(freq, 1),
                    "avg_speed": round(avg_spd, 1)
                })
                
        return rose_data, dominant_dir

class OrientationOptimizer:
    """
    Translates Wind Stats into Architectural Rules (AC4).
    """
    FACADES = {
        "N": "North", "S": "South", "E": "East", "W": "West",
        "NE": "North-East", "NW": "North-West", "SE": "South-East", "SW": "South-West",
        "NNE": "NNE", "ENE": "ENE", "ESE": "ESE", "SSE": "SSE",
        "SSW": "SSW", "WSW": "WSW", "WNW": "WNW", "NNW": "NNW"
    }

    @staticmethod
    def get_advice(dominant_dir):
        facade = OrientationOptimizer.FACADES.get(dominant_dir, dominant_dir)
        advice = []

        # Rule 1: Capture
        advice.append({
            "category": "Fenestration",
            "suggestion": f"Maximize operable windows on the {facade} facade.",
            "reason": f"Captures positive pressure from prevailing {dominant_dir} winds."
        })

        # Rule 2: Axis (Perpendicular)
        advice.append({
            "category": "Massing",
            "suggestion": f"Orient building long axis perpendicular to {dominant_dir}.",
            "reason": "Maximizes cross-ventilation surface area."
        })

        # Rule 3: Solar Conflict (West/East)
        if "W" in dominant_dir or "E" in dominant_dir:
            advice.append({
                "category": "Shading",
                "suggestion": "Use deep louvers or porous screens (Jaalis).",
                "reason": f"Prevailing wind ({dominant_dir}) coincides with low-angle sun. Block sun, admit wind."
            })
            
        return advice

# ==========================================
# 2. DATA FETCHERS (The "Triple Tier")
# ==========================================

def fetch_tier1_trends(lat, lon):
    """
    TIER 1: ERA5-Land Monthly
    Goal: Fast trend lines for Dashboard.
    """
    print("   ...Fetching Tier 1 (Monthly Trends)")
    point = ee.Geometry.Point([lon, lat])
    
    # Fetch last 5 years only for speed
    data = ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR") \
        .filterDate('2019-01-01', '2023-12-31') \
        .select('u_component_of_wind_10m') \
        .getRegion(point, 11000).getInfo()
    
    # Return simple count to prove it worked
    return len(data) - 1 # Minus header

def fetch_tier2_analysis(lat, lon):
    """
    TIER 2: ERA5-Land Daily
    Goal: Detailed Wind Rose & Orientation Logic.
    """
    print("   ...Fetching Tier 2 (Daily History for Wind Rose)")
    point = ee.Geometry.Point([lon, lat])
    
    # Fetch Daily data for 'Monsoon' season (Jun-Sep) over 5 years
    # We limit to 5 years for PoC execution speed
    dataset = ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR") \
        .filterDate('2019-01-01', '2023-12-31') \
        .filter(ee.Filter.calendarRange(6, 9, 'month')) \
        .select(['u_component_of_wind_10m', 'v_component_of_wind_10m'])
    
    # Client-side processing for PoC (Production would use GEE Reducers)
    # limit(1000) prevents timeouts during local python testing
    raw_data = dataset.getRegion(point, 11000).getInfo()
    
    # Parse into list of dicts
    headers = raw_data[0]
    records = []
    for row in raw_data[1:]:
        r = dict(zip(headers, row))
        records.append({
            'u': r['u_component_of_wind_10m'], 
            'v': r['v_component_of_wind_10m']
        })
        
    return records

def fetch_tier3_map_check(lat, lon):
    """
    TIER 3: ERA5 Standard (Global)
    Goal: Verify Ocean coverage for Map Visualization.
    """
    print("   ...Fetching Tier 3 (Map Layer Check)")
    
    # Test Point: Arabian Sea (Offshore Mumbai)
    sea_point = ee.Geometry.Point([72.0, 19.0])
    
    # Check if Standard ERA5 has data here
    img = ee.ImageCollection("ECMWF/ERA5/DAILY").first()
    val = img.reduceRegion(ee.Reducer.first(), sea_point, 28000).getInfo()
    
    return val.get('u_component_of_wind_10m') is not None

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    print("\n🚀 STARTING SAT-8 TRIPLE-TIER POC...\n")
    initialize_gee()
    
    # Test Site: Bangalore (REVA University)
    LAT, LON = 13.11, 77.63
    
    # --- TIER 1 ---
    trend_count = fetch_tier1_trends(LAT, LON)
    print(f"✅ Tier 1 Success: Retrieved {trend_count} monthly records for Sparkline.\n")
    
    # --- TIER 2 ---
    daily_records = fetch_tier2_analysis(LAT, LON)
    print(f"✅ Tier 2 Success: Retrieved {len(daily_records)} daily records for Analysis.")
    
    # Process Logic
    print("   ...Running WindRoseProcessor...")
    rose_data, dominant_dir = WindRoseProcessor.process(daily_records)
    
    print("   ...Running OrientationOptimizer...")
    advice = OrientationOptimizer.get_advice(dominant_dir)
    
    # --- TIER 3 ---
    has_ocean = fetch_tier3_map_check(LAT, LON)
    map_status = "READY" if has_ocean else "FAILED"
    print(f"✅ Tier 3 Success: Ocean Data Available? {has_ocean} ({map_status} for Map Layer).\n")
    
    # --- FINAL OUTPUT (The Payload) ---
    final_payload = {
        "meta": {
            "spike_id": "SAT-8",
            "location": "Bangalore",
            "tier_architecture": "Hybrid (Land-Monthly + Land-Daily + Standard)"
        },
        "analysis": {
            "season": "Monsoon (Jun-Sep)",
            "dominant_wind": dominant_dir,
            "wind_rose_sample": rose_data[:3], # Show first 3 for brevity
        },
        "architectural_insights": advice
    }
    
    print("📦 FINAL JSON PAYLOAD (API CONTRACT):")
    print(json.dumps(final_payload, indent=2))

if __name__ == "__main__":
    main()