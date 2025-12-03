# glue_delta_catalog_etl.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import time
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.utils import AnalysisException
import boto3
from botocore.exceptions import ClientError


# ---------- Job arguments ----------
args = getResolvedOptions(
    sys.argv,
    [
        'source-path',     # e.g. s3://my-bucket/input/sales_enhanced.csv
        'destination-path',    # e.g. s3://my-bucket/output/delta/sales
        'database-name',  # e.g. sales_db
        'table-name',
        'job-name'
    ]
)
print("Received args:", args)

input_path = args['source_path']
output_path = args['destination_path']
database_name = args['database_name']
table_name = args['table_name']
job_name = args['job_name']
input_path = "s3://raw-bucket-s3-source/*/*.csv"
print(input_path)
# print("DEBUG: output_path ->", repr(output_path))


# ---------- Glue / Spark Context ----------
spark = (SparkSession.builder
    .appName("Glue-Delta-ETL")
    .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse",
                output_path)
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.write.metadata.evolution.enabled" ,"true")
    # .config("spark.sql.catalog.glue_catalog.write.schema.evolution.enabled", "true")
    .config("spark.sql.catalog.glue_catalog.write.spark.accept-any-schema", "true")
    .config("spark.sql.catalog.glue_catalog.allow-column-addition", "true")
    .config("spark.sql.catalog.glue_catalog.allow-column-drop", "true")
    .config("spark.sql.iceberg.check-ordering", "false")
    .config("spark.sql.catalog.glue_catalog.cache-enabled", "false")
    .config("spark.sql.iceberg.schema.auto-evolution", "true")
    .config("spark.sql.caseSensitive", "false")
    .config("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate())
  
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(job_name, args)

s3 = boto3.client('s3')
redshift = boto3.client("redshift-data")
qualified_table = f"glue_catalog.{database_name}.{table_name}"

def spark_to_redshift_type(dtype):
    mapping = {
                "string": "VARCHAR(255)",
                "integer": "INT",
                "long": "BIGINT",
                "double": "DOUBLE PRECISION",
                "float": "FLOAT4",
                "boolean": "BOOLEAN",
                "timestamp": "TIMESTAMP",
                "date": "DATE",
                "decimal": "DECIMAL(18,2)"
            }
            # default fallback
    return mapping.get(dtype, "VARCHAR(255)")

def copy_to_redshift(folder_date,schema):    
    redshift_table = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sales') as table_exists"""
    try:
        print("inside redshift try")
        resp = redshift.execute_statement(
        WorkgroupName = "analytics-wg",
        Database = "analyticsdb",
        Sql = redshift_table)
        statement_id = resp["Id"]
        while(True):
            status1 = redshift.describe_statement(Id=statement_id)["Status"]
            print("query to check redshift table",status1)
            if status1 in("FINISHED", "FAILED", "ABORTED"):
                break
            time.sleep(2)
        query_status = True
        if status1 == "FAILED" or status1 =="ABORTED":
            err = redshift.describe_statement(Id=statement_id).get("Error")
            print(f"Query Execution Failed → {err}")
        
    except Exception as e:
        print(f"Table does not exist in redshift")
        query_status = False

    if not query_status:
        print("inside if statement")
        columns = []
        for field in schema.fields:
            col_name = field.name
            col_type = spark_to_redshift_type(field.dataType.simpleString())
            columns.append(f"{col_name} {col_type}")
    
        create_sql = f"Create table if not exists analyticsdb.public.sales({', '.join(columns)});"
        res1 = redshift.execute_statement(
            WorkgroupName = "analytics-wg",
            Database = "analyticsdb",
            Sql = create_sql 
        )
        stmt_id = res1["Id"]
        while(True):
            status1 = redshift.describe_statement(Id=stmt_id)["Status"]
            print(status1)
            if status1 in("FINISHED", "FAILED", "ABORTED"):
                break
            time.sleep(2)
        if status1 == "FAILED" or status1 =="ABORTED":
            err = redshift.describe_statement(Id=stmt_id).get("Error")
            print(f"Query Execution Failed → {err}")
        print("table created successfully in redshift")
    else:
        print("table already exist in redshift")

    marker_bucket = "gold-bucket-curated"
    marker_prefix = "markers/sales_loaded" 
    
    marker_key = f"{marker_prefix}/{folder_date}.marker"
    
    try:
        response = s3.head_object(Bucket=marker_bucket, Key=marker_key)
        print(f"Redshift Load Already Done for: {folder_date}")
        redshift_marker_exist = True
    except ClientError:
        print(f"No marker for {folder_date}")
        redshift_marker_exist = False
        
    if not redshift_marker_exist:
        copy_sql = f"""
            copy analyticsdb.public.sales(order_id, customer_id, product, category, 
                quantity, price, total, order_date, sale_date)
            from 's3://gold-bucket-curated/sales/sale_date={folder_date}/'
            iam_role 'arn:aws:iam::654654322747:role/redshift_serverless_role'
            format as parquet;   
        """
        try:
            res2 = redshift.execute_statement(
                WorkgroupName="analytics-wg",
                Database="analyticsdb",
                Sql=copy_sql
            )
            stmt_id1 = res2["Id"]
            print(f"Query submitted with ID: {stmt_id1}")
            max_wait_time = 300  # 5 minutes maximum
            wait_time = 0
            while True:
                status2 = redshift.describe_statement(Id=stmt_id1)["Status"]
                print(status2)
                if status2 in ("FINISHED","FAILED","ABORTED"):
                    break
                time.sleep(5)
                wait_time += 5
                
                if wait_time > max_wait_time:
                        print("Timeout: Query taking too long to complete")
                        break
            if status2 == "FINISHED":
                print("COPY command completed successfully!")
                
            elif status2 == "FAILED" or status2 == "ABORTED":
                err = redshift.describe_statement(Id=stmt_id1).get("Error")
                if err:
                        print(f"Query Failed → Error: {err}")
                else:
                        print(f"Query Failed but no error details.")
                        print(f"Full response: {response}")
            elif status2 == "SUBMITTED":
                    print("Query still in SUBMITTED state. It might be queued or taking longer than expected.")
                    
        except Exception as e:
            print(f" Exception occurred: {str(e)}")
            
        print(f"Redshift COPY Completed for sale_date = {folder_date}")
        
        s3.put_object(Bucket = marker_bucket, Key = marker_key, Body = b'')
        print(f"marker created for s3://{marker_bucket}/{marker_key}")

def copy_to_curated_bucket(folder_date):
    df_iceberg = (spark.read.format("iceberg").load(qualified_table).filter(F.col("sale_date")==folder_date).select(
        "order_id",
        "customer_id", 
        "product",
        "category",
        "quantity",
        "price",
        "total",
        "order_date",
        "sale_date"))
    schema = df_iceberg.schema
    folder_date_str = folder_date.strftime('%d-%m-%Y')
    curated_bucket_path = f"s3://gold-bucket-curated/sales/sale_date={folder_date_str}/"
    (df_iceberg
    .write.mode("overwrite")
    .format("parquet")
    .save(curated_bucket_path)
    )
    print("Curated data written to GOLD Zone")
    copy_to_redshift(folder_date_str,schema)

def create_iceberg_table(df,table_exists,folder_date):
    print("table_exists",table_exists)
    if table_exists:
        print("Table exists → APPENDING")
        target_df = spark.read.table(qualified_table)
        for col in df.columns:
            if col not in target_df.columns:
                spark.sql(f"""Alter table {qualified_table} add column {col} string""")
        for col in target_df.columns:
            if col not in df.columns:
                df = df.withColumn(col,F.lit(None))
        cols = target_df.columns
        df.createOrReplaceTempView("incoming")
        print("incoming table")
        set_expr = ", ".join([f"t.{c} = s.{c}" for c in cols if c!='order_id'])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"s.{c}" for c in cols])
        spark.sql("DESCRIBE incoming").show(100,False)
        try:
            merge_sql = f"""
            MERGE INTO glue_catalog.processed_database.processed_table t
            USING incoming s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET {set_expr}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) values ({insert_vals})
            """
            print("Trying simplified MERGE...")
            print(merge_sql)
            spark.sql(merge_sql)
            print("MERGE successful!")
        except Exception as e:
            print(f"MERGE failed with error: {str(e)}")
            print(f"Error type: {type(e).__name__}")
    else:
        print("table does not exist")
        (df.writeTo(qualified_table)
                  .tableProperty("format-version", "2") 
                #   .tableProperty("write.spark.accept-any-schema", "true")
                  .tableProperty("write.metadata.metrics.default", "full")
                  .tableProperty("write.upsert.enabled", "true")
                  .tableProperty("write.merge.mode", "merge-on-read")
                  .create()
        )
    copy_to_curated_bucket(folder_date)
        
def data_transformation(path,table_exists,folder_date):
    print("inside data transformation")
    df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(path)
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
    df = df.withColumn("file_path", F.input_file_name())
    df = df.withColumn("sale_date", F.lit(folder_date))
    df = df.withColumn('price', F.col('price').cast('double')) if 'price' in df.columns else df.withColumn('price', F.lit(0.0))
    df = df.withColumn('quantity', F.col('quantity').cast('double')) if 'quantity' in df.columns else df.withColumn('quantity', F.lit(1.0))
    df = df.withColumn('discount_percent', F.col('discount_percent').cast('double')) if 'discount_percent' in df.columns else df.withColumn('discount_percent', F.lit(1))  
    df = df.withColumn('discount_amount', F.col('discount_amount').cast('double')) if 'discount_amount' in df.columns else df.withColumn('discount_amount', F.lit(1))  
    df = df.withColumn('total',F.col('total').cast('double'))
    df = df.withColumn('discount_percent', F.when(F.col('discount_percent') < 0, 0.0)
                                   .when(F.col('discount_percent') > 1, 1.0)
                                   .otherwise(F.col('discount_percent')))
    df = df.withColumn('final_price', F.col('total') * (1 - F.col('discount_percent'))).withColumn('order_date',F.col("order_date").cast('string'))
    df = df.withColumn('ship_date', F.col('ship_date').cast('string'))
    df = df.withColumn('year', F.year('sale_date')) \
           .withColumn('month', F.month('sale_date')) \
           .withColumn('day', F.dayofmonth('sale_date'))
    create_iceberg_table(df,table_exists,folder_date)
    
new_dates = []
def dates_in_s3_folder():
    res = s3.list_objects_v2(Bucket='raw-bucket-s3-source', Delimiter='/')
    for prefix in res.get('CommonPrefixes', []):
        folder_date = prefix['Prefix'].rstrip('/')
        new_dates.append(folder_date)
    return new_dates
    
try :
    print("inside first try")
    existing_df = spark.sql(f"select max(sale_date) as max_date from {qualified_table}")
    max_date_result = existing_df.collect()[0]['max_date']
    max_date_result = max_date_result.strftime('%d-%m-%Y')
    print("max_date",max_date_result)
    table_exists = True
    date_value = dates_in_s3_folder()
    print("date_value",date_value)
    for i in date_value:
        if i > max_date_result:
            print("comparison",i)
            date_obj = datetime.strptime(i, '%d-%m-%Y').date()
            data_transformation(f"s3://raw-bucket-s3-source/{i}/*.csv",table_exists,date_obj)
        elif i == max_date_result:
            print("inside else of first try")
            max_date_obj = datetime.strptime(max_date_result, '%d-%m-%Y').date()
            copy_to_curated_bucket(max_date_obj)
            
except:
    table_exists = False
    
if not table_exists:    
    date_value = dates_in_s3_folder()
    for i in date_value:
        date_obj = datetime.strptime(i, '%d-%m-%Y').date()
        data_transformation(input_path,table_exists,date_obj)

# spark.sql(f"SHOW TBLPROPERTIES {qualified_table}").show(truncate=False)

# spark.table("glue_catalog.processed_database.processed_table").printSchema()

job.commit()