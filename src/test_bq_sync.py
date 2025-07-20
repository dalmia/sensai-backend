#!/usr/bin/env python3
"""
Test script for BigQuery sync functionality.
This script tests the sync process without running the full scheduler.
"""

import asyncio
import sys
import os

# Add the src directory to the path so we can import api modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.bq.cron import BigQuerySyncer, sync_to_bigquery
from api.settings import settings


async def test_bigquery_configuration():
    """Test BigQuery configuration."""
    print("Testing BigQuery configuration...")

    required_settings = [
        ("google_application_credentials", settings.google_application_credentials),
        ("bq_project_name", settings.bq_project_name),
        ("bq_dataset_name", settings.bq_dataset_name),
    ]

    for setting_name, setting_value in required_settings:
        if setting_value:
            print(f"✅ {setting_name}: {setting_value}")
        else:
            print(f"❌ {setting_name}: Not configured")
            return False

    print("✅ BigQuery configuration looks good!")
    return True


async def test_sync_tracking():
    """Test sync tracking table creation."""
    print("\nTesting sync tracking...")

    try:
        syncer = BigQuerySyncer()
        await syncer.initialize_sync_tracking()
        print("✅ Sync tracking initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to initialize sync tracking: {e}")
        return False


async def test_table_schemas():
    """Test table schema definitions."""
    print("\nTesting table schemas...")

    try:
        syncer = BigQuerySyncer()
        table_count = len(syncer.tables_to_sync)
        print(f"✅ Found {table_count} tables configured for sync:")

        for table_name in sorted(syncer.tables_to_sync.keys()):
            schema = syncer.tables_to_sync[table_name]
            field_count = len(schema)
            print(f"  - {table_name}: {field_count} fields")

        return True
    except Exception as e:
        print(f"❌ Failed to load table schemas: {e}")
        return False


async def test_single_table_sync(table_name: str = None):
    """Test syncing a single table."""
    if not table_name:
        # Default to a small table for testing
        table_name = "organizations"

    print(f"\nTesting sync for table: {table_name}")

    try:
        syncer = BigQuerySyncer()

        if table_name not in syncer.tables_to_sync:
            print(f"❌ Table {table_name} not found in sync configuration")
            return False

        # Initialize tracking
        await syncer.initialize_sync_tracking()

        # Get sync info
        sync_info = await syncer.get_last_sync_info(table_name)
        print(f"Last sync info: {sync_info}")

        # Get changed rows (without actually syncing to BQ in test)
        changed_rows = await syncer.get_changed_rows(
            table_name, sync_info["last_synced_row_id"]
        )
        print(f"Found {len(changed_rows)} rows to sync")

        if changed_rows:
            print("Sample row:", changed_rows[0] if changed_rows else "None")

        print(f"✅ Table {table_name} sync test completed")
        return True

    except Exception as e:
        print(f"❌ Failed to test sync for table {table_name}: {e}")
        import traceback

        traceback.print_exc()
        return False


async def run_full_sync_test():
    """Run a full sync test (only if BigQuery is configured)."""
    print("\nTesting full sync...")

    try:
        print("⚠️  This will attempt to sync to BigQuery if configured.")
        response = input("Continue? (y/N): ")
        if response.lower() != "y":
            print("Skipping full sync test")
            return True

        await sync_to_bigquery()
        print("✅ Full sync completed successfully")
        return True

    except Exception as e:
        print(f"❌ Full sync failed: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    """Main test function."""
    print("BigQuery Sync Test Suite")
    print("=" * 40)

    tests = [
        ("Configuration", test_bigquery_configuration),
        ("Sync Tracking", test_sync_tracking),
        ("Table Schemas", test_table_schemas),
        ("Single Table Sync", lambda: test_single_table_sync("organizations")),
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n{test_name}")
        print("-" * len(test_name))
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Ask about full sync test
    if all(result for _, result in results):
        print("\nAll basic tests passed!")
        await run_full_sync_test()

    # Summary
    print("\n" + "=" * 40)
    print("Test Results Summary:")
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")


if __name__ == "__main__":
    asyncio.run(main())
