"""
Main ETL Pipeline Orchestrator
Coordinates extraction, transformation, loading, and validation processes.
"""

import time
import extractor
import transformer
import loader
import validator


def run_etl_pipeline():
    """Execute the complete ETL pipeline"""
    pipeline_start = time.time()

    print("\n" + "=" * 50)
    print("NORTHWIND ETL PIPELINE - STARTING")
    print("=" * 50)

    # PHASE 1: EXTRACTION
    print("\n--- EXTRACTION PHASE ---")
    try:
        extractor.extract_from_sql_server()
        extractor.extract_from_access()
    except Exception as extract_error:
        print(f"EXTRACTION FAILED: {extract_error}")
        return

    # PHASE 2: TRANSFORMATION
    print("\n--- TRANSFORMATION PHASE ---")
    try:
        transformer.run_transformation()
    except Exception as transform_error:
        print(f"TRANSFORMATION FAILED: {transform_error}")
        return

    # PHASE 3: LOADING
    print("\n--- LOADING PHASE ---")
    try:
        loader.load_staging_to_warehouse()
    except Exception as load_error:
        print(f"LOADING FAILED: {load_error}")
        return

    # PHASE 4: VALIDATION
    print("\n--- VALIDATION PHASE ---")
    validator.execute_warehouse_validation()

    # Calculate total execution time
    pipeline_end = time.time()
    execution_time = pipeline_end - pipeline_start

    print("\n" + "=" * 50)
    print(f"ETL PIPELINE COMPLETED IN {execution_time:.2f} SECONDS")
    print("=" * 50)


if __name__ == "__main__":
    run_etl_pipeline()