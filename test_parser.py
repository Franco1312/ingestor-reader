"""Test script for BCRA Infomondia ETL flow with incremental updates."""

from src.application.etl_use_case import ETLUseCase
from src.infrastructure.config_loader import YamlConfigLoader
from src.infrastructure.lock_managers.lock_manager_factory import LockManagerFactory
from src.infrastructure.plugins.extractors.http_extractor import HttpExtractor
from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)
from src.infrastructure.plugins.parsers.bcra_infomondia_parser import BcraInfomondiaParser
from src.infrastructure.state_managers.state_manager_factory import StateManagerFactory


def test_etl_flow():
    """Test the complete ETL flow with incremental updates."""
    # Load configuration
    config_loader = YamlConfigLoader()
    config = config_loader.load_dataset_config("bcra_infomondia_series")
    
    print("=" * 60)
    print("ETL Flow Test - BCRA Infomondia")
    print("=" * 60)
    print(f"Dataset ID: {config['dataset_id']}")
    print(f"Provider: {config['provider']}")
    print()
    
    # Initialize components
    extractor = HttpExtractor(config["source"])
    parser = BcraInfomondiaParser()
    normalizer = BcraInfomondiaNormalizer()
    
    # State manager from config (or None if not configured)
    state_config = config.get("state")
    state_manager = StateManagerFactory.create(state_config)
    
    # Lock manager from config (or None if not configured)
    lock_config = config.get("lock")
    lock_manager = LockManagerFactory.create(lock_config)
    
    # Create ETL use case
    etl = ETLUseCase(
        extractor=extractor,
        parser=parser,
        normalizer=normalizer,
        state_manager=state_manager,
        lock_manager=lock_manager,
    )
    
    # Check series last dates before first run
    if state_manager:
        series_last_dates = state_manager.get_series_last_dates(config)
        print(f"Series last dates before first run: {len(series_last_dates)} series")
        print()
    
    # First run - full extraction
    print("First run (full extraction)...")
    print("-" * 60)
    data = etl.execute(config)
    print(f"Total data points: {len(data)}")
    
    # Show summary by series
    series_counts = {}
    series_max_dates = {}
    for data_point in data:
        series_code = data_point["internal_series_code"]
        series_counts[series_code] = series_counts.get(series_code, 0) + 1
        obs_time = data_point.get("obs_time")
        if obs_time:
            if series_code not in series_max_dates or obs_time > series_max_dates[series_code]:
                series_max_dates[series_code] = obs_time
    
    print("\nData points per series:")
    for series_code in sorted(series_counts.keys()):
        count = series_counts[series_code]
        max_date = series_max_dates.get(series_code)
        print(f"  {series_code}: {count} points (max date: {max_date})")
    
    # Check state
    print("\nState saved:")
    for series_code in sorted(series_max_dates.keys()):
        saved_date = state_manager.get_last_date(series_code)
        print(f"  {series_code}: {saved_date}")
    
    # Second run - incremental (should filter)
    print("\n" + "=" * 60)
    print("Second run (incremental - should filter old data)...")
    print("-" * 60)
    
    # Check series last dates before second run
    if state_manager:
        series_last_dates = state_manager.get_series_last_dates(config)
        print(f"Series last dates before second run: {len(series_last_dates)} series")
        print("Last dates per series:")
        for series_code in sorted(series_last_dates.keys()):
            print(f"  {series_code}: {series_last_dates[series_code]}")
        print()
    
    data2 = etl.execute(config)
    print(f"Total data points (after filtering): {len(data2)}")
    
    # Show breakdown by series for second run
    series_counts2 = {}
    for data_point in data2:
        series_code = data_point["internal_series_code"]
        series_counts2[series_code] = series_counts2.get(series_code, 0) + 1
    
    print("\nData points per series (second run):")
    for series_code in sorted(series_counts2.keys()):
        count1 = series_counts.get(series_code, 0)
        count2 = series_counts2.get(series_code, 0)
        diff = count1 - count2
        if diff > 0:
            print(f"  {series_code}: {count2} points (filtered {diff})")
        else:
            print(f"  {series_code}: {count2} points (no change)")
    
    if len(data2) < len(data):
        print(f"\n✓ Incremental update working! Filtered {len(data) - len(data2)} old data points")
    else:
        print("\n⚠ No filtering occurred - checking why...")
    
    print("\n" + "=" * 60)
    print("Test completed!")


if __name__ == "__main__":
    test_etl_flow()
