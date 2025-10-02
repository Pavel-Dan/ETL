# Uniclothe Data Pipeline

## The Goal of this project

Uniclothe is a fast-growing, digital-native fashion brand known for its minimalist and affordable collections. While the company initially scaled through its e-commerce platform, it has recently opened its first flagship physical store to experiment with an omnichannel retail strategy.
To sustain this growth, Uniclothe aims to leverage data to better understand customer behavior and continuously adapt its marketing and commercial strategy.
Management wants to rely on a modern data pipeline to unify online and in-store sales and provide a clear, reliable, and near real-time view of business performance.

Uniclothe’s sales data is fragmented across two main channels:
- E-commerce transactions from the online platform.
- In-store transactions from the flagship physical store.

This fragmentation creates several challenges:
- Lack of a unified dashboard to compare online vs offline performance.
- Absence of near real-time analytics for decision-making.
- Difficulty identifying top-performing products and key customer segments.

The objective of this use case is to build a modern data pipeline on Azure and Snowflake that consolidates e-commerce and physical store sales, providing reliable insights into product performance, sales trends, and customer behavior. The solution will deliver a unified, near real-time view of the business through Power BI dashboards, enabling data-driven decisions for marketing, sales, and inventory optimization.

Expected Impact:
- Smarter targeting of products and channels for promotion.
- Improved marketing campaigns tailored to online and in-store audiences.
- Better inventory planning and demand forecasting.
- Faster insights with near real-time analytics.


## Composition of this repository

### [sql_queries]("./sql_queries")

In this folder, we kept each important sql query

### [src]("./src")

This foler contain our python scripts and libs

### [environment.yml]("./environment.yml")

The environment file to execute python scripts troughth `conda`

## Parts of this project

### Batch
