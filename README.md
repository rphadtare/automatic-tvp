# Safe Home Exploration
Repository for safe home exploration project

## Assumptions on Dune Query
- Assuming Dune query is getting refresh daily between 00:00 - 00:15 AM




Run job
docker exec -it spark-master bash
python3 spark_scripts/bronze.py 2024-11-23
spark-submit --master spark://spark-master:7077 spark_scripts/silver.py 2024-11-23
spark-submit --master spark://spark-master:7077 spark_scripts/gold.py 2024-11-23
spark-submit --master spark://spark-master:7077 spark_scripts/topk_analysis.py 2024-11-23