# ==============================================================================
# SECTION 1: ENVIRONMENT SETUP & DEPENDENCY INJECTION
# ==============================================================================
import os
import sys

print("[INFO] Auditing background cluster packages for Apriori Engine...")
try:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mlxtend==0.23.1", "--quiet"])
    print("[SUCCESS] MLxtend engine dependencies successfully aligned.")
except Exception as config_err:
    print(f"[WARN] In-line configuration bypassed: {str(config_err)}")

# ==============================================================================
# SECTION 2: TRANSFORMATION LAYER - RESHAPE DATA TO SUB-CATEGORY BASKETS
# ==============================================================================
import pandas as pd
import mlflow
from mlxtend.frequent_patterns import apriori, association_rules
from pyspark.sql.functions import col, collect_list, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("[INFO] Reshaping Silver layers into Sub-Category Market Basket matrices...")

try:
    silver_df = spark.table("semantic_catalog_enrichment.`02_silver`.tbl_semantic_catalog_tokens")
    
    # CRUCIAL RE-ENGINEERING: Group by Order_ID and concatenate Sub_Category values 
    # to catch real co-purchasing patterns among distinct item types
    basket_df = silver_df.groupBy("Order_ID") \
                         .agg(concat_ws(" ", collect_list("Sub_Category")).alias("Product_Basket"))
    
    local_basket_pd = basket_df.toPandas()
    print(f"[SUCCESS] Reshaped {len(local_basket_pd)} unique basket order streams for analysis.")
    
    matrix_df = local_basket_pd['Product_Basket'].str.get_dummies(sep=' ')
    basket_matrix = matrix_df.astype(bool)
    print(f"[INFO] One-hot matrix generated. Evaluated columns: {list(basket_matrix.columns)}")

except Exception as data_err:
    print(f"[ERROR] Transaction reshaping matrix layout configuration failed: {str(data_err)}")

# ==============================================================================
# SECTION 3: DEPLOY APRIORI ASSOCIATION CORE & LOG TO MLFLOW
# ==============================================================================
print("[INFO] Initializing MLflow Pattern Discovery Logging lifecycle...")

try:
    os.environ["MLFLOW_DISABLE_ENV_MANAGER_LOGGING"] = "true"
    mlflow.set_experiment("/Shared/Semantic_Catalog_Enrichment_Audit")

    with mlflow.start_run(run_name="apriori_market_basket_execution") as run:
        print(f"[INFO] Active MLflow Analytics Run ID: {run.info.run_id}")
        
        # Standard configuration thresholds for diverse retail market baskets
        MIN_SUPPORT_THRESHOLD = 0.005
        MIN_CONFIDENCE_THRESHOLD = 0.05
        
        mlflow.log_param("analytics_algorithm", "Apriori_Association_Rules")
        mlflow.log_param("min_support", MIN_SUPPORT_THRESHOLD)
        mlflow.log_param("min_confidence", MIN_CONFIDENCE_THRESHOLD)
        
        print("[MODELING] Mining frequent item combinations patterns using Apriori...")
        frequent_itemsets = apriori(basket_matrix, min_support=MIN_SUPPORT_THRESHOLD, use_colnames=True)
        
        rules_df = pd.DataFrame()
        
        if not frequent_itemsets.empty:
            rules_df = association_rules(frequent_itemsets, metric="confidence", min_threshold=MIN_CONFIDENCE_THRESHOLD)
            
        if not rules_df.empty:
            rules_df['antecedents'] = rules_df['antecedents'].apply(lambda x: ', '.join(list(x)))
            rules_df['consequents'] = rules_df['consequents'].apply(lambda x: ', '.join(list(x)))
            
            # Replace spaces with underscores for clean Delta storage compliance
            rules_df.columns = [c.replace(' ', '_') for c in rules_df.columns]
            
            gold_spark_df = spark.createDataFrame(rules_df)
            
            gold_spark_df.write.format("delta") \
                         .mode("overwrite") \
                         .saveAsTable("semantic_catalog_enrichment.`03_gold`.tbl_market_basket_rules")
            print("[SUCCESS] Gold Layer Table successfully populated with pattern association rules.")
            total_rules_mined = len(rules_df)
            max_lift_discovered = float(rules_df['lift'].max())
        else:
            print("[WARN] No association patterns crossed the threshold baseline. Initializing empty fallback table structure...")
            
            empty_schema = StructType([
                StructField("antecedents", StringType(), True),
                StructField("consequents", StringType(), True),
                StructField("antecedent_support", DoubleType(), True),
                StructField("consequent_support", DoubleType(), True),
                StructField("support", DoubleType(), True),
                StructField("confidence", DoubleType(), True),
                StructField("lift", DoubleType(), True),
                StructField("leverage", DoubleType(), True),
                StructField("conviction", DoubleType(), True),
                StructField("zhangs_metric", DoubleType(), True)
            ])
            gold_spark_df = spark.createDataFrame([], empty_schema)
            gold_spark_df.write.format("delta").mode("overwrite").saveAsTable("semantic_catalog_enrichment.`03_gold`.tbl_market_basket_rules")
            
            total_rules_mined = 0
            max_lift_discovered = 0.0

        # Log tracked metrics out to MLflow
        mlflow.log_metric("total_rules_discovered", total_rules_mined)
        mlflow.log_metric("max_lift_metric", round(max_lift_discovered, 4))
        
        mlflow.set_tag("pipeline_stage", "Gold_Consumption")
        mlflow.set_tag("dataset_focus", "India_Retail_Analysis")
        
        print(f"[SUCCESS] Pattern analytics lineage safely committed to MLflow dashboard.")
        print(f" -> Insights Discovered: Mined {total_rules_mined} unique product buying relationships.")
        
        if total_rules_mined > 0:
            display(spark.table("semantic_catalog_enrichment.`03_gold`.tbl_market_basket_rules")
                    .select("antecedents", "consequents", "support", "confidence", "lift")
                    .limit(5))

except Exception as model_err:
    print(f"[ERROR] Gold Analytics Model execution block failed: {str(model_err)}")
