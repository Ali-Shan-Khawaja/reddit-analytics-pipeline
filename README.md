# Reddit Analytics Pipeline

This project builds an end-to-end data pipeline to extract Reddit comment data, clean and transform it using PySpark, and load it into both Amazon Redshift and Snowflake for analysis and dashboarding.

## 🔧 Tech Stack
- PySpark
- AWS S3
- Amazon Redshift
- Snowflake
- Apache Superset / Streamlit

## 📊 Goals
- Transform raw nested Reddit JSON into a structured star schema
- Perform sentiment analysis and feature engineering
- Load cleaned data into Redshift and Snowflake
- Create dashboards to analyze subreddit trends, user engagement, and sentiment