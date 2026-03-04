# Databricks notebook source
# ===== CONFIG (edit these BEFORE running) =====
CATALOG = "your_catalog"         # example: demo_catalog
SCHEMA = "your_schema"           # example: idp_demo
DOCS_LOCAL_PATH = "data/sample_docs"   # relative path in repo
DOCS_TARGET_DBFS = "dbfs:/FileStore/idp-demo/docs/"  # where to copy in workspace
# ==============================================

# COMMAND ----------

# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC select *
# MAGIC from read_files('/Volumes/idp_demo/idp_bronze/pdfs')

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC create or replace table idp_demo.idp_bronze.parsed_data as
# MAGIC select path,
# MAGIC ai_parse_document(content) as parsed_content
# MAGIC from read_files('/Volumes/idp_demo/idp_bronze/pdfs')

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC select * from idp_demo.idp_bronze.parsed_data

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC create schema if not exists idp_demo.idp_silver

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC create or replace table  idp_demo.idp_silver.better_data as 
# MAGIC SELECT 
# MAGIC   path,
# MAGIC   concat_ws('\n',
# MAGIC     transform(
# MAGIC       try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
# MAGIC       e -> coalesce(try_cast(e:content AS STRING), '')
# MAGIC     )
# MAGIC   ) AS doc_text
# MAGIC FROM negin_demo.idp.parsed_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from idp_demo.idp_silver.better_data

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create or replace table idp_demo.idp_silver.classified_data as
# MAGIC select *,
# MAGIC ai_classify(doc_text, array('construction_invoice','apartment_rental','purchase_order')) as document_classification
# MAGIC  from negin_demo.idp.better_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from idp_demo.idp_silver.classified_data

# COMMAND ----------

# MAGIC %sql select * from idp_demo.idp_silver.classified_data
# MAGIC where document_classification = 'purchase_order'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC select *, ai_extract(doc_text, array('po_number','po_date','buyer','vendor','item_code','description','unit_price','quantity','total')) as extracted from idp_demo.idp_silver.classified_data
# MAGIC where document_classification ='purchase_order'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,
# MAGIC ai_extract(
# MAGIC   doc_text,
# MAGIC   array(
# MAGIC     'purchase order number',
# MAGIC     'purchase order date',
# MAGIC     'buyer company name',
# MAGIC     'vendor company name',
# MAGIC     'item codes from the line items',
# MAGIC     'product descriptions from the line items',
# MAGIC     'unit price for each item',
# MAGIC     'quantity for each item',
# MAGIC     'total amount of the purchase order'
# MAGIC   )
# MAGIC ) as extracted
# MAGIC from idp_demo.idp_silver.classified_data
# MAGIC where document_classification = 'purchase_order'

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %sql
# MAGIC create or replace table idp_demo.idp_silver.purchase_orders as
# MAGIC select *,
# MAGIC ai_extract(
# MAGIC   doc_text,
# MAGIC   array(
# MAGIC     'purchase_order_number',
# MAGIC     'purchase_order_date',
# MAGIC     'buyer_company_name',
# MAGIC     'vendor_company_name',
# MAGIC     'item_codes_from_the_line_items',
# MAGIC     'product_descriptions_from_the_line_items',
# MAGIC     'unit_price_for_each_item',
# MAGIC     'quantity_for_each_item',
# MAGIC     'total_amount_of_the_purchase_order'
# MAGIC   )
# MAGIC ) as extracted
# MAGIC from idp_demo.idp_silver.classified_data
# MAGIC where document_classification = 'purchase_order'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from idp_demo.idp_silver.purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists idp_demo.idp_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table idp_demo.idp_gold.purchase_orders as 
# MAGIC select 
# MAGIC path , 
# MAGIC extracted.purchase_order_number as po_number,
# MAGIC extracted.purchase_order_date as po_date,
# MAGIC extracted.buyer_company_name as buyer,
# MAGIC extracted.vendor_company_name as vendor,
# MAGIC extracted.item_codes_from_the_line_items as item_code,
# MAGIC extracted.product_descriptions_from_the_line_items as description,
# MAGIC extracted.unit_price_for_each_item as unit_price,
# MAGIC extracted.quantity_for_each_item as quantity,
# MAGIC extracted.total_amount_of_the_purchase_order as total
# MAGIC from idp_demo.idp_silver.purchase_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from idp_demo.idp_gold.purchase_orders