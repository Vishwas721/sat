import ee
import time

# Initialize
try:
    ee.Initialize(project='site-analysis-poc')
except:
    ee.Authenticate()
    ee.Initialize(project='site-analysis-poc')

def benchmark_dataset(name, dataset_id, time_window_years=10, is_daily=False):
    print(f"\n⏱️ Benchmarking: {name} ({dataset_id})...")
    
    lat, lon = 13.11, 77.63 # Bangalore
    point = ee.Geometry.Point([lon, lat])
    
    start_time = time.time()
    
    # 1. Define Request
    # We fetch a 10-year window to simulate a real user analysis
    start_date = '2014-01-01'
    end_date = '2024-01-01'
    
    collection = ee.ImageCollection(dataset_id).filterDate(start_date, end_date)
    
    # 2. Force Computation (The heavy lifting)
    # For Daily: We simulate fetching the Histogram (Wind Rose)
    # For Monthly: We simulate fetching the Trend Line
    
    if is_daily:
        # Complex Operation: Filter Monsoon + Calculate Mean
        # This simulates the "Wind Rose Processor" load
        monsoon_col = collection.filter(ee.Filter.calendarRange(6, 9, 'month'))
        result = monsoon_col.mean().reduceRegion(
            reducer=ee.Reducer.first(), geometry=point, scale=11000
        ).getInfo()
    else:
        # Simple Operation: Fetch list of values for Sparkline
        # (Reduced complexity for Monthly)
        result = collection.select('u_component_of_wind_10m').getRegion(point, 11000).getInfo()

    end_time = time.time()
    duration = round((end_time - start_time) * 1000, 0) # ms
    
    print(f"   ✅ Done. Latency: {duration} ms")
    return duration

# --- EXECUTE BENCHMARK ---
if __name__ == "__main__":
    print("🚀 STARTING LATENCY STRESS TEST...")
    
    # Tier 1: ERA5-Land Monthly (The "Instant" Layer)
    t1 = benchmark_dataset("Tier 1: Monthly Trends", "ECMWF/ERA5_LAND/MONTHLY_AGGR", is_daily=False)
    
    # Tier 2: ERA5-Land Daily (The "Analysis" Layer)
    t2 = benchmark_dataset("Tier 2: Daily Wind Rose", "ECMWF/ERA5_LAND/DAILY_AGGR", is_daily=True)
    
    # Tier 3: ERA5 Standard (The "Map" Layer)
    # Note: Map tiles are loaded by Leaflet client-side, but we test raw data fetch here
    t3 = benchmark_dataset("Tier 3: Standard Map Data", "ECMWF/ERA5/DAILY", is_daily=True)
    
    print("\n📊 FINAL REPORT:")
    print(f"Tier 1 (UI Speed): {t1} ms")
    print(f"Tier 2 (Analysis): {t2} ms")
    print(f"Tier 3 (Map Data): {t3} ms")
    
    if t1 < 1000:
        print("\n✅ PASSED: Tier 1 is suitable for Initial Page Load.")
    else:
        print("\n⚠️ WARNING: Tier 1 is too slow (>1s). Consider caching.")