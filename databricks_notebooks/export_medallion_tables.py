# ==============================================================================
# PROJECT: INDIA RETAIL ANALYSIS 
# COMPONENT: SECURE UNITY CATALOG VOLUME EXPORT UTILITY
# ==============================================================================
import os

def run_table_export_pipeline():
    print("[INFO] Initiating independent Medallion Table Export Pipeline to CSV...")
    
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except Exception as init_err:
        print(f"[FATAL] Unable to reference active cluster Spark context: {str(init_err)}")
        return

    # Create the landing Volume path if it doesn't already exist
    # Replace 'semantic_catalog_enrichment' and 'public_assets' if your schema differs
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS semantic_catalog_enrichment.public_assets")
        spark.sql("CREATE VOLUME IF NOT EXISTS semantic_catalog_enrichment.public_assets.csv_exports")
        print("[INFO] Unity Catalog Volume checked/initialized successfully.")
    except Exception as vol_err:
        print(f"[WARN] Volume pre-check skipped or custom namespace required: {str(vol_err)}")

    # Target path pointing securely inside Unity Catalog Volumes instead of DBFS root
    volume_path = "/Volumes/semantic_catalog_enrichment/public_assets/csv_exports"

    target_tables = {
        "Bronze (Raw Ingestion Baseline)": {
            "catalog_path": "semantic_catalog_enrichment.`01_bronze`.tbl_cleaned_retail_transactions",
            "output_dir": f"{volume_path}/data_ingestion_csv"
        },
        "Silver (spaCy NLP Tokenization)": {
            "catalog_path": "semantic_catalog_enrichment.`02_silver`.tbl_semantic_catalog_tokens",
            "output_dir": f"{volume_path}/silver_layer_csv"
        },
        "Gold (Apriori Market Basket Rules)": {
            "catalog_path": "semantic_catalog_enrichment.`03_gold`.tbl_market_basket_rules",
            "output_dir": f"{volume_path}/gold_layer_csv"
        }
    }

    for stage_name, paths in target_tables.items():
        try:
            print(f"\n[PROCESSING] Extracting dataset from layer: {stage_name}...")
            
            # Read the dataset table from the catalog
            spark_df = spark.table(paths["catalog_path"])
            
            # Write out to the secure Volume location via PySpark
            spark_df.coalesce(1).write \
                .format("csv") \
                .option("header", "true") \
                .mode("overwrite") \
                .save(paths["output_dir"])
                
            print(f"[SUCCESS] Saved data grid to Unity Catalog Volume path: {paths['output_dir']}")

        except Exception as stage_error:
            print(f"[WARN] Ingestion pipeline bypassed stage '{stage_name}' due to error: {str(stage_error)}")

    print("\n[ALL EXPORTS COMPLETE] Independent table packaging execution loop finished.")

if __name__ == "__main__":
    run_table_export_pipeline()
