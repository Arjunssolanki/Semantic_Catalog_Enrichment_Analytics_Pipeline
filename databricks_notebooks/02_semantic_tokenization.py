# ==============================================================================
# SECTION 1: SYSTEM SETUP & DRIVER INITIALIZATION
# ==============================================================================
import os
import sys

print("[INFO] Setting up local session properties...")
try:
    import subprocess
    # Install dependency natively into the driver runtime context
    subprocess.check_call([sys.executable, "-m", "pip", "install", "spacy==3.7.5", "--quiet"])
    subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm", "--quiet"])
    print("[SUCCESS] Local session dependencies aligned successfully.")
except Exception as config_err:
    print(f"[WARN] Local environment pipeline fallback active: {str(config_err)}")

# ==============================================================================
# SECTION 2: VECTORIZED NLP BATCH PROCESSING ENGINE
# ==============================================================================
import pandas as pd
import spacy
import mlflow
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StructType, StructField, StringType

print("[INFO] Initializing Vectorized Semantic NLP Tokenizer Engine...")

# Define structural schema mapping layout for the batch outputs
token_schema = StructType([
    StructField("Brand", StringType(), False),
    StructField("Product_Type", StringType(), False),
    StructField("Attributes", StringType(), False)
])

# Create a high-performance Vectorized Pandas UDF to process data in batches
@pandas_udf(token_schema)
def tokenize_batch_udf(product_series: pd.Series) -> pd.DataFrame:
    # Load the dictionary model safely once inside the batch worker context
    try:
        nlp_worker = spacy.load("en_core_web_sm")
    except:
        nlp_worker = spacy.blank("en")
        
    brands, item_types, attributes = [], [], []
    
    # Process the entire batch in memory as a fast vectorized array loop
    for product_name in product_series:
        if not product_name:
            brands.append("UNKNOWN_BRAND")
            item_types.append("Unknown_Item")
            attributes.append("Standard_Spec")
            continue
            
        doc = nlp_worker(str(product_name).strip())
        tokens = [token.text for token in doc if not token.is_punct]
        
        # Rule-Based Tokenization Strategy
        brand = tokens[0] if len(tokens) > 0 else "Generic"
        item_type = tokens[1] if len(tokens) > 1 else "General_merchandise"
        specs = "_".join(tokens[2:]) if len(tokens) > 2 else "Standard"
        
        brands.append(brand.upper())
        item_types.append(item_type.capitalize())
        attributes.append(specs.replace(" ", "_"))
        
    return pd.DataFrame({
        "Brand": brands,
        "Product_Type": item_types,
        "Attributes": attributes
    })

try:
    # 1. Read entries from your raw Bronze Delta Table 
    bronze_df = spark.table("semantic_catalog_enrichment.`01_bronze`.tbl_cleaned_retail_transactions")
    
    # 2. Execute vector processing across data frames in high-speed batches
    enriched_df = bronze_df.withColumn("Tokens", tokenize_batch_udf(col("Product_Name"))) \
                           .withColumn("Extracted_Brand", col("Tokens.Brand")) \
                           .withColumn("Extracted_Product_Type", col("Tokens.Product_Type")) \
                           .withColumn("Extracted_Attributes", col("Tokens.Attributes")) \
                           .drop("Tokens")
    
    # 3. Commit records to your permanent Silver Delta Table namespace
    enriched_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("semantic_catalog_enrichment.`02_silver`.tbl_semantic_catalog_tokens")
        
    print("[SUCCESS] Silver Delta Table securely written with clean product token data.")
    
    # Preview layout
    display(spark.table("semantic_catalog_enrichment.`02_silver`.tbl_semantic_catalog_tokens")
            .select("Product_Name", "Extracted_Brand", "Extracted_Product_Type", "Extracted_Attributes")
            .limit(5))

except Exception as e:
    print(f"[ERROR] Semantic tokenization phase failed: {str(e)}")

# ==============================================================================
# SECTION 3: MLOPS EXPERIMENT TRACKING & LINEAGE LOGGING LAYER
# ==============================================================================
print("[INFO] Initializing MLflow Experiment Logging...")

try:
    os.environ["MLFLOW_DISABLE_ENV_MANAGER_LOGGING"] = "true"
    mlflow.set_experiment("/Shared/Semantic_Catalog_Enrichment_Audit")

    with mlflow.start_run(run_name="nlp_tokenization_execution") as run:
        print(f"[INFO] Active MLflow Run ID initialized: {run.info.run_id}")
        
        mlflow.log_param("nlp_library", "spaCy_Vectorized")
        mlflow.log_param("nlp_model_name", "en_core_web_sm")
        mlflow.log_param("extraction_logic", "pandas_batch_vector_split")
        
        silver_table_df = spark.table("semantic_catalog_enrichment.`02_silver`.tbl_semantic_catalog_tokens")
        total_records_processed = silver_table_df.count()
        
        general_merch_count = silver_table_df.filter(col("Extracted_Product_Type") == "General_merchandise").count()
        fallback_percentage = (general_merch_count / total_records_processed) * 100 if total_records_processed > 0 else 0
        
        mlflow.log_metric("total_records_tokenized", total_records_processed)
        mlflow.log_metric("general_merchandise_fallbacks", general_merch_count)
        mlflow.log_metric("fallback_rate_percentage", round(fallback_percentage, 2))
        
        mlflow.set_tag("pipeline_stage", "Silver_Enrichment")
        mlflow.set_tag("dataset_focus", "India_Retail_Analysis")
        
        print(f"[SUCCESS] Pipeline lineage committed to MLflow dashboard.")
        print(f" -> Metrics Captured: Total Rows Tokenized = {total_records_processed}")
        print(f" -> Architecture Alert: Found {general_merch_count} single-word fallback items ({round(fallback_percentage, 2)}%).")

except Exception as mlflow_error:
    print(f"[ERROR] MLOps Logging failed: {str(mlflow_error)}")
