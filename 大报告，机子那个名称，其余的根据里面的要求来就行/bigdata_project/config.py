"""
项目配置文件
Hadoop/Spark大数据开发训练 - 用户行为日志分析系统
"""

# ==================== MySQL数据库配置 ====================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "bigdata_analysis",
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# ==================== HDFS配置 ====================
HDFS_CONFIG = {
    "namenode": "localhost",
    "port": 9000,
    "base_path": "/user/hqh/log_analysis",
    "cleaned_path": "/user/hqh/log_analysis/cleaned_logs.parquet"
}

# ==================== Spark配置 ====================
SPARK_CONFIG = {
    "app_name": "UserBehaviorLogAnalysis",
    "driver_memory": "2g",
    "executor_memory": "2g",
    "executor_cores": "2",
    "num_executors": "2",
    "partitions": "8"
}

# ==================== 数据路径配置 ====================
PATHS = {
    "input_logs": "user_behavior_logs.json",
    "output_parquet": "hdfs://localhost:9000/user/hqh/log_analysis/cleaned_logs.parquet",
    "mysql_jar": "C:/Program Files/MySQL/MySQL Server 8.0/lib/mysql-connector-java-8.0.27.jar"
}

# ==================== 日志配置 ====================
LOGGING_CONFIG = {
    "level": "WARN",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}
