
# 📊 SQL Data Warehouse Project

## Overview

This project implements a **Modern Data Warehouse** using a **multi-layer architecture (Bronze, Silver, Gold)** in SQL Server to simulate a retail business scenario. The goal is to demonstrate how raw operational data can be transformed into valuable business insights using dimensional modeling and ETL best practices.

---

## 🏗️ Architecture

### Bronze Layer
Raw CSVs ingested into staging tables (not included in this repo but assumed as source files).

### Silver Layer
Cleansed, conformed tables derived from CRM and ERP systems:
- `crm_cust_info`
- `crm_prd_info`
- `crm_sales_details`
- `erp_loc_a101`
- `erp_cust_az12`
- `erp_px_cat_g1v2`

### Gold Layer (Star Schema)
- `dim_customers`
- `dim_products`
- `fact_sales`

---

## 💡 Key Features

- **ETL Logic:** Cleanse and transform CRM and ERP data into a usable structure.
- **Surrogate Keys:** Generated using `ROW_NUMBER()` to ensure data warehouse integrity.
- **Data Enrichment:** Combined CRM and ERP sources for fuller customer profiles.
- **Fact Table Creation:** Created detailed sales transactions for analysis.
- **Star Schema:** Optimized for fast querying and business intelligence consumption.
- **Data Governance:** Standardized data formats, handled nulls, ensured consistency.

---

## 📈 Suggested Data Visualizations

You can use **Tableau**, **Power BI**, or **Python (Seaborn/Matplotlib)** to create the following dashboards:

### Sales Performance Dashboard
- Line chart: Total sales over time
- Bar chart: Top 10 products by revenue
- Map: Sales by country
- Box plot: Average order value by customer demographic

### Customer Insights
- Pie chart: Gender and marital status breakdown
- Histogram: Age distribution
- Aggregation: Customer lifetime value
- Line chart: Customer acquisition trend

### Product Analytics
- Bar chart: Product category & subcategory performance
- Trend line: Product lifecycle sales
- Scatter plot: Cost vs price margins

### Operational KPIs
- Bar chart: Order-to-ship time
- KPI card: Late shipments count
- Heatmap: Revenue by geographic region

---

## 🧰 Tools & Technologies

- **SQL Server** – Schema design, querying, ETL logic
- **T-SQL** – Data transformations and scripting
- **Tableau / Power BI** – (for dashboard development)
- **Draw.io** – Data flow diagrams
- **Notion** – Task planning and documentation

---

## 🧼 Future Improvements

- Automate ETL pipelines using SSIS or Apache Airflow
- Add a data quality framework with validation rules
- Build REST APIs to expose gold layer metrics
- Schedule data refresh with triggers or orchestration tools

---



## 🤝 Contributing

This portfolio project is designed to showcase data engineering and analytics skills. Open to collaborations and suggestions. Feel free to reach out!

---
