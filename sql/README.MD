# SQL Directory

This folder contains SQL scripts used in the project. These scripts are responsible for querying, transforming, and optimizing data stored in relational databases or Hive tables. They play a crucial role in data analysis, preprocessing, and preparation for visualization.

## Scripts Included:

### Stage 1 - PostgreSQL: Creating Models and Importing Data
- `create_tables.sql`: Defines the schema and creates the necessary tables in PostgreSQL.
- `import_data.sql`: Automates the process of importing raw data into PostgreSQL tables.
- `test_database.sql`: Validates the database setup and ensures data integrity.

### Stage 2 - Hive: Creating and Optimizing Tables
- `db.hql`: Creates Hive tables for storing and querying data.
- `optimization.hql`: Optimizes Hive tables with partitioning and bucketing for efficient querying and storage.

### Stage 2 & 4 - Chart Creation and Evaluation
- `chart1.hql` to `chart6.hql`: Generate data tables for visualizations and charts.
- `check_count.hql`: Verifies data counts and consistency across tables.
- `evaluation_results.hql`: Produces evaluation metrics table of models comparison (ML).
- `lr_predictions.hql`: Generates predictions tabel using logistic regression models output.
- `rf_predictions.hql`: Generates predictions table using random forest models output.
- `q1.hql`: Testing query.