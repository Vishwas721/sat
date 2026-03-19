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


# ==========================================
# 0. INITIALIZATION
# ==========================================

import math
import datetime
import ee
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict

# Initialize FastAPI app
app = FastAPI(title="SAT Wind Analysis Engine")

# --- Initialize Google Earth Engine ---
# Run `earthengine authenticate` in terminal before running
def initialize_gee():
    try:
        ee.Initialize(project='your-gee-project-id')  # Replace with your GEE project ID
        print("✅ GEE Initialized successfully.")
    except Exception:
        print(f"⚠️ Auth required. Triggering flow...")
        ee.Authenticate()
        ee.Initialize(project='site-analysis-poc')
        print("✅ GEE Initialized successfully after authentication.")


@app.on_event("startup")
async def startup_event():
    """Initializes GEE when the API server starts."""
    initialize_gee()

# ==========================================
# 1. CORE MATH & CLIMATOLOGY LOGIC (AC2 & AC3)
# ==========================================

def calculate_wind_metrics(u: float, v: float):
    """Calculates wind speed (m/s) and meteorological direction (degrees)."""
    speed = math.sqrt(u**2 + v**2)
    direction = (270 - (math.atan2(v, u) * (180 / math.pi))) % 360
    return round(speed, 2), round(direction, 2)

def get_compass_direction(degrees: float) -> str:
    """Maps degrees to the 16-point wind rose."""
    val = int((degrees / 22.5) + 0.5)
    compass_points = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                      "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return compass_points[(val % 16)]

def get_imd_season(month: int) -> str:
    """Maps months to standard Indian Meteorological Department seasons."""
    if month in [1, 2]: return "Winter"
    elif month in [3, 4, 5]: return "Pre-Monsoon"
    elif month in [6, 7, 8, 9]: return "Southwest Monsoon"
    elif month in [10, 11, 12]: return "Post-Monsoon"
    return "Unknown"

# ==========================================
# 2. ARCHITECTURAL RULES ENGINE (AC4 & AC5)
# ==========================================

def get_orientation_advice(prevailing_dir: str, season: str) -> Dict[str, str]:
    """Generates SME-validated architectural advice based on wind direction."""
    
    # Base heuristic: Orient long axis perpendicular to prevailing wind
    axis_map = {
        "N": "E-W", "S": "E-W", "E": "N-S", "W": "N-S",
        "NE": "NW-SE", "SW": "NW-SE", "NW": "NE-SW", "SE": "NE-SW",
        # Adding slight variations for 16-point compass
        "NNE": "WNW-ESE", "SSW": "WNW-ESE", "ENE": "NNW-SSE", "WSW": "NNW-SSE",
        "NNW": "ENE-WSW", "SSE": "ENE-WSW", "WNW": "NNE-SSW", "ESE": "NNE-SSW"
    }
    
    recommended_axis = axis_map.get(prevailing_dir, "Site-Specific")
    
    # Dynamic insight generation
    insight = f"The prevailing wind during {season} is from the {prevailing_dir}."
    strategy = f"Orient the building's long axis {recommended_axis} to maximize natural cross-ventilation."

    # Solar conflict edge cases (West Sun)
    if "W" in prevailing_dir:
        strategy += " However, because capturing West winds exposes the facade to harsh afternoon solar heat gain, utilize deep louvers, shaded verandas, or staggered fenestration on the windward side."
    
    # Monsoon rain edge cases
    if season == "Southwest Monsoon" and "SW" in prevailing_dir:
        strategy += " Ensure large openings on the SW facade have deep overhangs (chajjas) to prevent driving rain ingress during monsoon squalls."

    return {
        "recommended_axis": recommended_axis,
        "report_summary": f"{insight} {strategy}"
    }

# ==========================================
# 3. GEE DATA FETCHING (AC1)
# ==========================================

def fetch_gee_wind_data(lat: float, lon: float, years: int):
    """Fetches historical U and V vectors from ERA5-Land."""
    point = ee.Geometry.Point([lon, lat])
    end_date = datetime.date.today()
    start_date = end_date.replace(year=end_date.year - years)

    collection = (ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")
                  .filterBounds(point)
                  .filterDate(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                  .select(['u_component_of_wind_10m', 'v_component_of_wind_10m']))

    return collection.getRegion(point, scale=11132).getInfo()

# ==========================================
# 4. API MODELS & ROUTE (The JSON Contract)
# ==========================================

class WindFrequencyBin(BaseModel):
    direction: str
    frequency_percentage: float
    avg_speed_ms: float

class SeasonalWindData(BaseModel):
    season_name: str
    prevailing_direction: str
    wind_rose: List[WindFrequencyBin]
    architectural_advice: Dict[str, str]

class WindAnalysisResponse(BaseModel):
    site_coordinates: Dict[str, float]
    annual_prevailing_direction: str
    annual_architectural_advice: Dict[str, str]
    annual_wind_rose: List[WindFrequencyBin]
    seasonal_data: List[SeasonalWindData]

@app.get("/analysis/wind/climatology", response_model=WindAnalysisResponse)
async def get_wind_climatology(lat: float = Query(...), lon: float = Query(...), years: int = Query(10)):
    try:
        raw_data = fetch_gee_wind_data(lat, lon, years)
        if not raw_data or len(raw_data) <= 1:
            raise ValueError("No data returned from GEE.")

        headers = raw_data[0]
        time_idx, u_idx, v_idx = headers.index('time'), headers.index('u_component_of_wind_10m'), headers.index('v_component_of_wind_10m')

        compass_points = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        seasons = ["Annual", "Winter", "Pre-Monsoon", "Southwest Monsoon", "Post-Monsoon"]
        
        # Initialize data structures
        aggregated_data = {s: {dir: {"count": 0, "speed_sum": 0.0} for dir in compass_points} for s in seasons}
        total_days = {s: 0 for s in seasons}

        # Process the raw vectors
        for row in raw_data[1:]:
            if row[u_idx] is None or row[v_idx] is None: continue
            
            month = datetime.datetime.fromtimestamp(row[time_idx] / 1000.0).month
            speed, direction_deg = calculate_wind_metrics(row[u_idx], row[v_idx])
            compass_dir = get_compass_direction(direction_deg)
            season = get_imd_season(month)

            # Annual aggregation
            aggregated_data["Annual"][compass_dir]["count"] += 1
            aggregated_data["Annual"][compass_dir]["speed_sum"] += speed
            total_days["Annual"] += 1

            # Seasonal aggregation
            aggregated_data[season][compass_dir]["count"] += 1
            aggregated_data[season][compass_dir]["speed_sum"] += speed
            total_days[season] += 1

        # Build the final response objects
        def build_rose(season_key):
            rose = []
            days = total_days[season_key]
            if days == 0: return rose
            for dir_key, metrics in aggregated_data[season_key].items():
                if metrics["count"] > 0:
                    rose.append(WindFrequencyBin(
                        direction=dir_key,
                        frequency_percentage=round((metrics["count"] / days) * 100, 2),
                        avg_speed_ms=round(metrics["speed_sum"] / metrics["count"], 2)
                    ))
            return sorted(rose, key=lambda x: x.frequency_percentage, reverse=True)

        annual_rose = build_rose("Annual")
        annual_prevailing = annual_rose[0].direction if annual_rose else "Unknown"
        
        seasonal_response = []
        for s in ["Winter", "Pre-Monsoon", "Southwest Monsoon", "Post-Monsoon"]:
            s_rose = build_rose(s)
            if s_rose:
                s_prevailing = s_rose[0].direction
                seasonal_response.append(SeasonalWindData(
                    season_name=s,
                    prevailing_direction=s_prevailing,
                    wind_rose=s_rose,
                    architectural_advice=get_orientation_advice(s_prevailing, s)
                ))

        return WindAnalysisResponse(
            site_coordinates={"lat": lat, "lon": lon},
            annual_prevailing_direction=annual_prevailing,
            annual_architectural_advice=get_orientation_advice(annual_prevailing, "the year"),
            annual_wind_rose=annual_rose,
            seasonal_data=seasonal_response
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    print("Starting SAT Wind Analysis Engine...")
    print("API docs: http://127.0.0.1:8000/docs")
    try:
        import uvicorn
        uvicorn.run("sat_wind_poc:app", host="127.0.0.1", port=8000, reload=False)
    except ModuleNotFoundError:
        print("Missing dependency: uvicorn")
        print("Install it with: pip install uvicorn")