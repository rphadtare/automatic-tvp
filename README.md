# Safe Home Exploration
Repository for safe home exploration project

## Assumptions
- Assuming Dune query is getting refresh daily between 00:00 - 00:15 AM

# docker commands to spin up airflow and spark cluster
  - For spinning up container services locally - `docker-compose up -d`
  - to tear down services- `docker compose down --rmi all`
  - Login to particular container - ` docker exec -it spark-master bash`

# How to run jobs using commands on spark cluster for testing purpose
  - python3 spark_scripts/bronze.py 2024-11-23
  - spark-submit --master spark://spark-master:7077 spark_scripts/silver.py 2024-11-23
  - spark-submit --master spark://spark-master:7077 spark_scripts/gold.py 2024-11-23
  - spark-submit --master spark://spark-master:7077 spark_scripts/topk_analysis.py 2024-11-23
s
  - [readme.txt](etl-workflow%2Freadme.txt)