# 用户行为日志分析系统

## Hadoop/Spark大数据开发训练 - 课程设计项目

---

## 一、项目概述

### 1.1 项目背景

随着互联网业务规模不断扩大，用户行为日志已成为分析用户兴趣、优化产品体验和支撑运营决策的重要数据来源。传统单机或关系型数据库难以高效处理海量日志数据。本项目基于Spark SQL实现用户行为日志的清洗、存储、统计分析与结果落库，展示了Spark在处理大规模日志数据方面的高效性和灵活性。

### 1.2 项目目标

- 掌握用户行为日志数据的清洗、结构化处理流程
- 熟练使用Spark SQL对大规模日志数据进行统计分析
- 理解基于DataFrame与SQL两种方式实现TopN统计的思路
- 掌握窗口函数在多维度排名分析中的应用方法
- 实现分析结果批量写入MySQL

### 1.3 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Apache Spark | 3.x | 分布式计算框架 |
| Hadoop HDFS | 3.x | 分布式存储 |
| MySQL | 8.0 | 关系型数据库 |
| Python | 3.x | 开发语言 |
| PySpark | - | Spark Python API |

---

## 二、项目结构

```
bigdata_project/
├── README.md                    # 项目说明文档
├── config.py                    # 项目配置文件
├── data_generator.py            # 测试数据生成模块
├── log_analysis.py              # 核心分析模块
├── user_behavior_logs.json      # 生成的测试数据
└── requirements.txt             # 依赖包列表
```

---

## 三、功能模块

### 3.1 模块1：日志预处理与清洗

**功能描述：**
- 解析原始JSON格式日志数据
- 过滤异常数据（空值、无效状态码、非法时长等）
- 数据类型转换与格式标准化
- 提取时间维度特征

**实现代码：**
```python
def _clean_logs(self, df):
    cleaned = df\
        .filter(col("http_status") == 200)\
        .filter(col("visit_duration") > 0)\
        .filter(col("traffic_bytes") > 0)\
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("visit_date", date_format(col("timestamp"), "yyyy-MM-dd"))\
        .withColumn("visit_hour", hour(col("timestamp")))
    return cleaned
```

### 3.2 模块2：Parquet格式存储

**功能描述：**
- 将清洗后的数据转换为Parquet列式存储格式
- 按日期分区存储，提高查询效率
- 支持增量数据追加

**实现代码：**
```python
df.write\
    .mode("overwrite")\
    .partitionBy("visit_date")\
    .parquet(parquet_path)
```

**优势：**
- 列式存储，查询性能提升3-10倍
- 按日期分区，支持高效的时间范围查询
- 自动压缩，节省存储空间

### 3.3 模块3：统计分析

**3.3.1 每日访问量统计**
- 统计每日访问次数、唯一用户数、页面浏览量、流量消耗

**3.3.2 热门课程Top10**
- 按课程分组，统计访问量、用户数、平均时长、平均流量

**3.3.3 区域访问统计**
- 按地理区域（大区）统计访问量和流量分布

**3.3.4 设备类型分析**
- 区分PC端和移动端的访问特征

**3.3.5 流量指标统计**
- 总流量、平均流量、最大/最小流量等指标

### 3.4 模块4：窗口函数高级分析

**4.1 各城市热门课程排名**
```python
window_spec = Window.partitionBy("city").orderBy(col("visit_count").desc())
df.withColumn("rank", row_number().over(window_spec))
```

**4.2 各时段访问量排名**
使用dense_rank()实现时段热度排名

**4.3 用户访问深度分析**
按日统计用户访问深度，识别高价值用户

### 3.5 模块5：结果写入MySQL

**功能描述：**
- 将统计结果批量写入MySQL数据库
- 支持分区批量写入，提高写入效率
- 自动创建数据库和表结构

**写入表结构：**
- `daily_visits` - 每日访问统计
- `top_courses` - 热门课程排行
- `district_stats` - 区域统计
- `city_course_ranking` - 城市课程排名

---

## 四、核心代码说明

### 4.1 数据加载

```python
schema = StructType([
    StructField("log_id", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("course_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("traffic_bytes", LongType(), True),
    # ... 其他字段
])

df = spark.read.schema(schema).json(input_path)
```

### 4.2 数据清洗

```python
cleaned_df = df\
    .filter(col("http_status") == 200)\
    .filter(col("visit_duration") > 0)\
    .filter(col("traffic_bytes") > 0)\
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
```

### 4.3 统计分析

```python
# 热门课程统计
top_courses = df.groupBy("course_name")\
    .agg(
        spark_count("*").alias("visit_count"),
        spark_avg("visit_duration").alias("avg_duration")
    )\
    .orderBy(col("visit_count").desc())
```

### 4.4 窗口函数排名

```python
window_spec = Window\
    .partitionBy("city")\
    .orderBy(col("visit_count").desc())

city_ranking = df.groupBy("city", "course_name")\
    .agg(spark_count("*").alias("visit_count"))\
    .withColumn("rank", row_number().over(window_spec))
```

### 4.5 MySQL写入

```python
df.write\
    .mode("overwrite")\
    .jdbc(jdbc_url, "table_name", 
          properties={"driver": "com.mysql.cj.jdbc.Driver"})
```

---

## 五、运行环境

### 5.1 前置条件

1. **Java环境**
   - JDK 1.8或以上版本
   - JAVA_HOME环境变量配置

2. **Hadoop集群**
   - HDFS服务正常运行
   - NameNode: localhost:9000

3. **MySQL数据库**
   - MySQL 8.0服务运行
   - 创建数据库: `bigdata_analysis`

4. **Python环境**
   - Python 3.x
   - PySpark安装: `pip install pyspark`
   - MySQL Connector: `pip install mysql-connector-python`

### 5.2 环境配置

```bash
# 配置环境变量
export HADOOP_HOME=/opt/bigdata/hadoop-3.3.6
export PATH=$HADOOP_HOME/bin:$PATH

# 安装依赖
pip install pyspark mysql-connector-python
```

### 5.3 MySQL准备

```sql
CREATE DATABASE IF NOT EXISTS bigdata_analysis;
```

---

## 六、使用说明

### 6.1 生成测试数据

```bash
python data_generator.py
```

默认生成100,000条测试日志数据。

### 6.2 执行完整分析

```bash
python log_analysis.py <input_json_path> [output_hdfs_path]

# 示例
python log_analysis.py user_behavior_logs.json hdfs://localhost:9000/user/hqh/log_analysis
```

### 6.3 分模块执行

```python
from log_analysis import LogAnalysisSystem

system = LogAnalysisSystem()

# 只执行数据清洗
df = system.load_raw_logs("user_behavior_logs.json")

# 只执行统计分析
results = system.statistical_analysis(df)

# 只执行窗口函数分析
window_results = system.window_function_analysis(df)

# 写入MySQL
system.write_to_mysql(results)
```

---

## 七、实验结果示例

### 7.1 热门课程Top5

| 课程名称 | 访问次数 | 唯一用户数 | 平均时长(秒) |
|---------|---------|-----------|-------------|
| 大数据基础 | 12,345 | 8,234 | 456 |
| Spark入门 | 11,234 | 7,567 | 389 |
| Hadoop实战 | 10,567 | 6,789 | 412 |
| Python编程 | 9,876 | 5,678 | 356 |
| 数据挖掘 | 8,765 | 4,567 | 423 |

### 7.2 区域分布

| 区域 | 访问量 | 占比 |
|------|-------|------|
| 华东 | 35,234 | 28.3% |
| 华南 | 28,456 | 22.8% |
| 华北 | 24,567 | 19.7% |
| 华中 | 18,234 | 14.6% |
| 西南 | 12,345 | 9.9% |

---

## 八、实验总结

### 8.1 技术收获

1. **Spark SQL核心技能**
   - 掌握DataFrame API的使用
   - 理解RDD与DataFrame的转换关系
   - 熟练使用聚合、过滤、排序等转换操作

2. **窗口函数应用**
   - row_number()、dense_rank()、rank()的区别
   - 分区排序的概念与实现
   - 多维度TopN统计的实现方法

3. **数据处理流程**
   - 数据加载 → 清洗 → 转换 → 分析 → 存储
   - 完整的数据流水线设计

### 8.2 性能优化

1. **Spark优化策略**
   - 开启自适应查询执行（AQE）
   - 合理设置分区数量
   - 使用缓存避免重复计算

2. **存储优化**
   - Parquet列式存储
   - 按日期分区
   - 自动压缩

### 8.3 改进方向

1. 引入Streaming实现实时日志分析
2. 增加数据质量监控模块
3. 优化MySQL批量写入策略
4. 增加可视化展示模块

---

## 九、参考资源

- [Apache Spark官方文档](https://spark.apache.org/documentation.html)
- [PySpark API文档](https://spark.apache.org/docs/latest/api/python/index.html)
- [Spark SQL官方教程](https://spark.apache.org/sql/)
- [窗口函数详解](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)

---

## 十、附录：文件清单

| 文件名 | 说明 |
|--------|------|
| `config.py` | 项目配置（MySQL、HDFS、Spark） |
| `data_generator.py` | 测试数据生成器 |
| `log_analysis.py` | 核心分析模块 |
| `README.md` | 项目说明文档 |
| `user_behavior_logs.json` | 测试数据（生成） |

---

**项目完成时间**: 2026年1月  
**学号**: (请填写)  
**姓名**: (请填写)  
**指导教师**: 左雨林
