# Cell 1: Data Ingestion and Structural Cleaning Engine
from pyspark.sql.functions import col, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("[INFO] Initializing Indian Retail Catalog Cleaning Pipeline...")

# 1. Enforce a strict schema structure to handle mixed data types safely
retail_schema = StructType([
    StructField("Order_ID", StringType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Ship_Date", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Customer_Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub_Category", StringType(), True),
    StructField("Product_Name", StringType(), True), # Unstructured text column for NLP
    StructField("List_Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Payment_Mode", StringType(), True),
    StructField("Returned_Status", StringType(), True)
])

# 2. Map the validated absolute Unity Catalog file path string (Corrected filename)
volume_path = "/Volumes/semantic_catalog_enrichment/01_bronze/raw_ingestion_layer/india_retail_sales_dataset.csv"

try:
    # Read the messy CSV data using our strict engineering schema bounds
    raw_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(retail_schema) \
        .load(volume_path)
        
    initial_count = raw_df.count()
    print(f"[SUCCESS] Raw dataset loaded from Volume. Total rows: {initial_count}")
    
    # 3. Data Cleaning: De-duplicate records based on key unique transaction indicators
    deduped_df = raw_df.dropDuplicates(["Order_ID", "Product_Name"])
    cleaned_count = deduped_df.count()
    print(f"[CLEANING] Redundant entries dropped. Removed {initial_count - cleaned_count} duplicate records.")
    
    # 4. Handle Operational Noise: Fill missing categorical attributes with standardized fallbacks
    final_cleaned_df = deduped_df.na.fill({"Returned_Status": "No", "Payment_Mode": "Unknown"})
    
    # 5. Drop low-signal/completely empty structural columns if any exist in the schema
    low_signal_cols = [c for c in ["unnamed", "operational_notes", "noise_field"] if c in final_cleaned_df.columns]
    if low_signal_cols:
        final_cleaned_df = final_cleaned_df.drop(*low_signal_cols)
        print(f"[CLEANING] Successfully dropped low-signal fields: {low_signal_cols}")
    
    # 6. Save data as an optimized, permanent Bronze Delta Table in Unity Catalog
    final_cleaned_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("semantic_catalog_enrichment.`01_bronze`.tbl_cleaned_retail_transactions")
    
    print("[SUCCESS] Bronze Delta Table securely committed to catalog metadata store.")
    
    # Preview our clean engineering baseline grid
    display(spark.table("semantic_catalog_enrichment.`01_bronze`.tbl_cleaned_retail_transactions").limit(5))

except Exception as e:
    print(f"[ERROR] Ingestion phase failed: {str(e)}")
