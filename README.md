# Reddit Analytics ETL Pipeline

An end-to-end PySpark ETL pipeline that processes Reddit comment data from CSV files into structured Parquet data warehouse tables. It supports downstream analytics and dashboards using clean, de-duplicated, and optimized datasets.

---

## 🔧 Features

- Incremental dimension updates to avoid duplicate keys.
- Automatic surrogate key assignment for dimension tables.
- Fact table joins to dimensions via clean foreign keys.
- Timestamp handling: UNIX to UTC timestamp conversion.
- Filters out malformed timestamps (e.g. null or 1969-12-31).
- Logs processed dates for monitoring.
- Duplicate prevention via primary key logic.

---

## 🗂️ Data Source

This project uses public Reddit comment data from May 2015, available on Kaggle:

🔗 Reddit Comments - May 2015 Dataset (https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015)

- Over 160 million Reddit comments
- Provided as a single sqlite file
- Source: Pushshift Reddit dataset via Kaggle
---

## 📂 Project Structure

```
.
├── data/
│   ├── partitions/                 # Input CSV files
│   └── DW_cache/                   # Output Parquet files (dimension + fact)
│   └── Raw/                        # Containg Raw SQLite Databse file.
├── notebooks/
│   └── fetch_reddit_data.ipynb     # Fetching data from sqlite databse file into csvs
│   └── ingest_and_transform.ipynb  # Transform the Reddit comments raw csvs into dimension and fact table parquet files
│   └── load_dim_date.ipynb         # Load parquet date dimension file into snowflake
│   └── load_dim_users.ipynb        # Load parquet user dimension files into snowflake
│   └── load_dim_subreddit.ipynb    # Load parquet subreddit dimension files into snowflake
│   └── load_fact_comments.ipynb    # Load parquet fact comment files into snowflake
├── scripts/               # Contain similar files to load data into AWS redshift
├── .gitignore
└── README.md

```

---

## 🚀 Setup & Execution

### Prerequisites
- Python 3.8+
- PySpark


## 📅 Tables Created

### Dimension Tables

- **dim_user**: Unique authors with surrogate `user_id`
- **dim_subreddit**: Unique subreddits with `subreddit_key`
- **dim_date**: Distinct dates from `created_datetime`

### Fact Table

- **fact_comment**:
  - `comment_id`
  - Foreign keys to `user_id`, `subreddit_key`, `date_key`
  - Metadata fields: score, ups, downs, body, etc.
  - Duplicates removed via `dropDuplicates(["comment_id"])`

---

## 📊 Logs & Debugging

During processing, each file:
- Prints distinct `created_datetime` dates being written.
- Skips malformed or empty files.
- Logs if parquet folders already exist.

Example:
```
🚚 Processing file: reddit_data_2015-05-01.csv
🗓️ Distinct created dates being written:
|2015-05-01|
```

---

## 📄 Enhancements

- Add schema validation or automated tests.
- Replace overwrite with incremental write logic for fact.
- Add support for large datasets (partitioned writes).
- Add Spark job scheduling and parameter parsing.

---

## 🤝 Contributing

Open to contributions for enhancements, performance improvements, or integration with BI tools.

---

Built with ❤️ by [Ali-Shan-Khawaja](https://github.com/Ali-Shan-Khawaja)

