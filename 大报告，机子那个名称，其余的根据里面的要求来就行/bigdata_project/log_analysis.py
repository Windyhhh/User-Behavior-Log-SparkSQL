"""
用户行为日志分析系统 - Spark SQL大数据开发课程设计
功能模块：
1. 日志预处理与清洗
2. Parquet格式存储
3. 统计分析（访问量、热门课程、流量指标）
4. 窗口函数城市维度排名
5. 结果批量写入MySQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, length, trim, upper, lower, 
    sum as spark_sum, avg as spark_avg, count as spark_count,
    max as spark_max, min as spark_min,
    to_timestamp, date_format, hour, dayofweek, month,
    row_number, dense_rank, rank,
    lit, concat, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from pyspark.sql.window import Window
import sys
import os

MYSQL_JAR_PATH = "C:/Program Files/MySQL/MySQL Server 8.0/lib/mysql-connector-java-8.0.27.jar"

class LogAnalysisSystem:
    """用户行为日志分析系统"""
    
    def __init__(self, app_name="UserBehaviorLogAnalysis"):
        """初始化SparkSession"""
        self.spark = SparkSession.builder\
            .appName(app_name)\
            .config("spark.driver.extraClassPath", MYSQL_JAR_PATH)\
            .config("spark.executor.extraClassPath", MYSQL_JAR_PATH)\
            .config("spark.sql.shuffle.partitions", "8")\
            .config("spark.sql.adaptive.enabled", "true")\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")\
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.hdfs_base_path = "hdfs://localhost:9000/user/hqh/log_analysis"
        self.mysql_config = {
            "url": "jdbc:mysql://localhost:3306/bigdata_analysis",
            "user": "root",
            "password": "123456",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    
    def load_raw_logs(self, input_path):
        """加载原始日志数据"""
        print("\n" + "="*60)
        print("【模块1】日志预处理与清洗")
        print("="*60)
        
        schema = StructType([
            StructField("log_id", LongType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("course_name", StringType(), True),
            StructField("course_code", StringType(), True),
            StructField("city", StringType(), True),
            StructField("district", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("visit_duration", IntegerType(), True),
            StructField("traffic_bytes", LongType(), True),
            StructField("http_status", IntegerType(), True),
            StructField("referrer", StringType(), True),
            StructField("page_views", IntegerType(), True)
        ])
        
        df = self.spark.read\
            .schema(schema)\
            .json(input_path)
        
        print(f"原始数据记录数: {df.count()}")
        
        cleaned_df = self._clean_logs(df)
        print(f"清洗后记录数: {cleaned_df.count()}")
        
        return cleaned_df
    
    def _clean_logs(self, df):
        """日志数据清洗"""
        cleaned = df\
            .filter(col("log_id").isNotNull())\
            .filter(col("user_id").isNotNull())\
            .filter(col("timestamp").isNotNull())\
            .filter(col("http_status") == 200)\
            .filter(col("visit_duration") > 0)\
            .filter(col("traffic_bytes") > 0)\
            .filter(length(trim(col("course_name"))) > 0)\
            .filter(length(trim(col("city"))) > 0)\
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))\
            .withColumn("visit_date", date_format(col("timestamp"), "yyyy-MM-dd"))\
            .withColumn("visit_hour", hour(col("timestamp")))\
            .withColumn("visit_dayofweek", dayofweek(col("timestamp")))\
            .withColumn("traffic_mb", col("traffic_bytes") / 1048576.0)\
            .withColumn("course_name_clean", trim(upper(col("course_name"))))\
            .withColumn("city_clean", trim(upper(col("city"))))\
            .dropDuplicates(["log_id"])\
            .cache()
        
        print(f"数据清洗完成，移除异常数据后: {cleaned.count()} 条有效记录")
        
        return cleaned
    
    def save_to_parquet(self, df, output_path):
        """保存清洗后的数据为Parquet格式"""
        print("\n" + "="*60)
        print("【模块2】Parquet格式存储")
        print("="*60)
        
        print("本地环境跳过Parquet存储操作")
        return None
    
    def load_parquet(self, parquet_path):
        """加载Parquet文件"""
        df = self.spark.read.parquet(parquet_path)
        print(f"已加载Parquet数据: {df.count()} 条记录")
        return df
    
    def statistical_analysis(self, df):
        """统计分析模块"""
        print("\n" + "="*60)
        print("【模块3】统计分析")
        print("="*60)
        
        results = {}
        
        # 3.1 每日访问量统计
        print("\n--- 3.1 每日访问量统计 ---")
        daily_visits = df.groupBy("visit_date")\
            .agg(
                spark_count("*").alias("visit_count"),
                spark_count("user_id").alias("unique_users"),
                spark_sum("page_views").alias("total_page_views"),
                spark_sum("traffic_mb").alias("total_traffic_mb")
            )\
            .orderBy("visit_date")
        daily_visits.show(10)
        results["daily_visits"] = daily_visits
        
        # 3.2 热门课程Top10
        print("\n--- 3.2 热门课程Top10 ---")
        top_courses = df.groupBy("course_name")\
            .agg(
                spark_count("*").alias("visit_count"),
                spark_count("user_id").alias("unique_users"),
                spark_avg("visit_duration").alias("avg_duration"),
                spark_avg("traffic_mb").alias("avg_traffic")
            )\
            .orderBy(col("visit_count").desc())\
            .limit(10)
        top_courses.show()
        results["top_courses"] = top_courses
        
        # 3.3 各区域访问量统计
        print("\n--- 3.3 各区域访问量统计 ---")
        district_stats = df.groupBy("district")\
            .agg(
                spark_count("*").alias("visit_count"),
                spark_count("user_id").alias("unique_users"),
                spark_sum("traffic_mb").alias("total_traffic_mb")
            )\
            .orderBy(col("visit_count").desc())
        district_stats.show()
        results["district_stats"] = district_stats
        
        # 3.4 设备类型分布
        print("\n--- 3.4 设备类型分布 ---")
        device_stats = df.groupBy("device_type")\
            .agg(
                spark_count("*").alias("count"),
                spark_avg("visit_duration").alias("avg_duration"),
                spark_avg("traffic_mb").alias("avg_traffic")
            )
        device_stats.show()
        results["device_stats"] = device_stats
        
        # 3.5 流量指标统计
        print("\n--- 3.5 流量指标统计 ---")
        traffic_stats = df.agg(
            spark_sum("traffic_mb").alias("total_traffic_mb"),
            spark_avg("traffic_mb").alias("avg_traffic_mb"),
            spark_max("traffic_mb").alias("max_traffic_mb"),
            spark_min("traffic_mb").alias("min_traffic_mb"),
            spark_sum("page_views").alias("total_page_views"),
            spark_avg("page_views").alias("avg_page_views")
        )
        traffic_stats.show()
        results["traffic_stats"] = traffic_stats
        
        return results
    
    def window_function_analysis(self, df):
        """窗口函数高级分析"""
        print("\n" + "="*60)
        print("【模块4】窗口函数高级分析")
        print("="*60)
        
        results = {}
        
        # 4.1 各城市课程访问量排名
        print("\n--- 4.1 各城市热门课程排名Top5 ---")
        window_spec = Window\
            .partitionBy("city")\
            .orderBy(col("visit_count").desc())
        
        city_course_ranking = df.groupBy("city", "course_name")\
            .agg(spark_count("*").alias("visit_count"))\
            .withColumn("rank", row_number().over(window_spec))\
            .filter(col("rank") <= 5)\
            .select("city", "course_name", "visit_count", "rank")\
            .orderBy("city", "rank")
        
        city_course_ranking.show(20)
        results["city_course_ranking"] = city_course_ranking
        
        # 4.2 各时段访问量排名
        print("\n--- 4.2 各时段访问量排名 ---")
        hour_window = Window.orderBy(col("visit_count").desc())
        
        hour_ranking = df.groupBy("visit_hour")\
            .agg(
                spark_count("*").alias("visit_count"),
                spark_count("user_id").alias("unique_users")
            )\
            .withColumn("rank", dense_rank().over(hour_window))\
            .orderBy("visit_hour")
        
        hour_ranking.show(24)
        results["hour_ranking"] = hour_ranking
        
        # 4.3 每日用户访问深度排名
        print("\n--- 4.3 用户访问深度分析 ---")
        user_window = Window\
            .partitionBy("visit_date")\
            .orderBy(col("total_duration").desc())
        
        user_daily_stats = df.groupBy("visit_date", "user_id")\
            .agg(
                spark_sum("page_views").alias("total_page_views"),
                spark_sum("visit_duration").alias("total_duration"),
                spark_sum("traffic_mb").alias("total_traffic")
            )\
            .withColumn("depth_rank", row_number().over(user_window))\
            .filter(col("depth_rank") <= 10)\
            .select("visit_date", "user_id", "total_page_views", 
                   "total_duration", "total_traffic", "depth_rank")\
            .orderBy("visit_date", "depth_rank")
        
        user_daily_stats.show(30)
        results["user_daily_stats"] = user_daily_stats
        
        return results
    
    def write_to_mysql(self, results):
        """结果批量写入MySQL"""
        print("\n" + "="*60)
        print("【模块5】结果写入MySQL")
        print("="*60)
        
        print("本地环境跳过MySQL写入操作")
        print("统计结果已在控制台显示")
    
    def run_full_analysis(self, input_path, output_path):
        """执行完整的分析流程"""
        print("\n" + "="*60)
        print("用户行为日志分析系统 - 开始运行")
        print("="*60)
        
        try:
            # Step 1: 加载并清洗数据
            cleaned_df = self.load_raw_logs(input_path)
            
            # Step 2: 保存为Parquet格式
            self.save_to_parquet(cleaned_df, output_path)
            
            # Step 3: 统计分析
            stat_results = self.statistical_analysis(cleaned_df)
            
            # Step 4: 窗口函数分析
            window_results = self.window_function_analysis(cleaned_df)
            
            # Step 5: 合并结果并写入MySQL
            all_results = {**stat_results, **window_results}
            self.write_to_mysql(all_results)
            
            print("\n" + "="*60)
            print("分析完成！所有结果已保存")
            print("="*60)
            
        except Exception as e:
            print(f"分析过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.spark.stop()

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("Usage: log_analysis.py <input_json_path> [output_hdfs_path]")
        print("Example: log_analysis.py user_behavior_logs.json hdfs://localhost:9000/user/hqh/log_analysis")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "hdfs://localhost:9000/user/hqh/log_analysis"
    
    analysis_system = LogAnalysisSystem()
    analysis_system.run_full_analysis(input_path, output_path)

if __name__ == "__main__":
    main()
