# databricks idp demo

a small demo showing how pdf documents can be parsed and turned into structured tables using databricks ai functions and a simple medallion architecture.

the goal is to take unstructured documents (like purchase orders or invoices), parse them, classify them, and extract useful fields that can be used for analytics.

---

## goal

parse pdf documents using databricks idp functions and organize the results in bronze, silver, and gold layers.

the pipeline does three main things:

* parse raw pdf documents
* classify the document type
* extract structured information from the documents

---

## architecture

the pipeline follows a simple medallion structure.

```
pdf documents
      ↓
ai_parse_document()
      ↓
bronze layer
(parsed pdf content)

      ↓
ai_classify()
      ↓
silver layer
(classified documents)

      ↓
ai_extract()
      ↓
gold layer
(structured tables ready for analysis)
```

---

## catalog structure

catalog used in the demo:

```
idp_demo
```

### bronze layer

schema: `idp_demo.idp_bronze`

contains raw document data.

* pdf files stored in
  `/Volumes/idp_demo/idp_bronze/pdfs`

tables:

* `parsed_data` – parsed document content created using `ai_parse_document()`

---

### silver layer

schema: `idp_demo.idp_silver`

contains intermediate processed data.

tables:

* `better_data`
* `classified_data`
* `purchase_orders`

this layer is responsible for document classification and preparing data for extraction.

---

### gold layer

schema: `idp_demo.idp_gold`

contains clean structured data ready for analytics.

tables:

* `purchase_orders`

this table stores the final extracted purchase order fields.

---

## ai functions used

the demo uses built-in databricks ai functions.

* `ai_parse_document()`
  parses the pdf document and returns structured document elements.

* `ai_classify()`
  classifies the document type (purchase order, invoice, etc).

* `ai_extract()`
  extracts structured fields such as vendor, buyer, item descriptions, quantities, and totals.

---

## sample documents

the demo includes several types of example documents such as:

* purchase orders
* invoices
* apartment rental ledgers

these documents are used to demonstrate parsing and extraction.

---
## example output

you can see a few screenshots from the demo pipeline in the output_sample folder


## how to run

1. upload sample pdf files to the bronze volume:

```
/Volumes/idp_demo/idp_bronze/pdfs
```

2. open the notebook:

```
IDP_final_example
```

3. run the notebook cells in order.

4. query the gold table to see the extracted data.

example:

```sql
select *
from idp_demo.idp_gold.purchase_orders
```

---

## repository structure

```
idp-demo
│
├── notebooks
│   └── IDP_final_example.py
│
├── data
│   └── sample_docs
│
└── README.md
```

---

## git usage

the project can be used directly from databricks repos.

steps:

1. open **repos** in databricks.
2. connect your git provider.
3. clone this repository.
4. open the notebook and run the pipeline.

---

this project is intended as a simple example of how databricks idp features can be used to process documents and convert them into structured datasets.
