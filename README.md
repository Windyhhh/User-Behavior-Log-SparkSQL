# 📊 User Behavior Log Spark SQL | 用户行为日志分析系统

> **Big data processing solution for user behavior log analysis using Apache Spark SQL. Includes log analysis pipeline, data generator, two Spark experiments (student stats, TopN anchor gold), and complete project documentation.**
>
> 基于 Apache Spark SQL 的用户行为日志大数据处理方案。包含日志分析流水线、数据生成器、两个 Spark 实验（学生成绩统计、TopN 主播金币）和完整项目文档。

---

## 🌟 Features | 核心特性

- **Apache Spark SQL** — Large-scale log data processing
- **User Behavior Analysis** — Click, view, purchase behavior patterns
- **Data Generator** — Synthetic user behavior log generator
- **Two Experiments** — Student grade statistics + TopN gold anchor
- **Scala + Python** — Both Spark Scala and PySpark implementations
- **RDD + DataFrame** — Both RDD and DataFrame API examples
- **Complete Documentation** — Project report, experiment guides, blog

---

## 📁 Project Structure | 项目结构

```
User-Behavior-Log-SparkSQL/
├── 大报告/bigdata_project/          # Main project
│   ├── config.py                     # Configuration
│   ├── data_generator.py             # Synthetic data generator
│   ├── log_analysis.py               # Core log analysis (Spark SQL)
│   ├── user_behavior_logs.json       # Sample log data
│   ├── requirements.txt
│   └── README.md
├── 实验一/                            # Experiment 1: Student grade stats
│   ├── data1.txt                      # Student data
│   ├── experiment1.py                 # PySpark DataFrame version
│   ├── experiment1_rdd.py             # PySpark RDD version
│   └── 要求.txt
├── 实验二/                            # Experiment 2: TopN gold anchor
│   ├── gift_record.log                # Gift sending records
│   ├── video_info.log                 # Video information
│   ├── TopGoldAnchor.scala            # Spark Scala implementation
│   ├── top_gold_anchor.py             # PySpark implementation
│   └── 要求.txt
├── format_namenode.sh                 # HDFS namenode format script
├── 博客.md                             # Technical blog
├── 大作业.docx                         # Assignment document
├── 实验一：学生成绩统计分析.docx
├── 实验二：TopN主播金币收入统计.docx
└── README.md
```

---

## 🚀 Quick Start | 快速开始

```bash
# Main project
cd 大报告/bigdata_project
pip install -r requirements.txt
python log_analysis.py

# Experiment 1: Student grade statistics
cd 实验一
spark-submit experiment1.py
# or RDD version:
spark-submit experiment1_rdd.py

# Experiment 2: TopN gold anchor
cd 实验二
spark-submit top_gold_anchor.py
# or Scala version:
spark-submit --class TopGoldAnchor TopGoldAnchor.jar
```

---

## 🔬 Core Analysis | 核心分析

### User Behavior Log Analysis | 用户行为日志分析

The main project analyzes user behavior logs including:
- **Page views** — Most viewed pages, visit duration
- **Click patterns** — Click-through rates, button usage
- **Purchase behavior** — Conversion funnels, purchase frequency
- **User segmentation** — Active vs inactive users
- **Time patterns** — Peak usage hours, daily/weekly trends

### Experiment 1: Student Grade Statistics | 实验一：学生成绩统计

- Average score per student
- Subject-wise average
- Top N students
- Grade distribution (A/B/C/D/F)
- Pass/fail statistics

### Experiment 2: TopN Gold Anchor | 实验二：TopN 主播金币收入

- Join gift records with video info
- Aggregate gold income per anchor
- Top N anchors by gold income
- Time-windowed analysis (daily/weekly)
- Both Scala and Python implementations

---

## 📊 Spark APIs Used | 使用的 Spark API

| API | Usage |
|-----|-------|
| **Spark SQL** | DataFrame queries, SQL syntax |
| **RDD** | Low-level transformations and actions |
| **Join** | Inner/left joins between datasets |
| **Aggregation** | groupBy, agg, sum, avg, count |
| **Window Functions** | rank, row_number for TopN |
| **UDF** | User-defined functions |

---

## 📚 References | 参考文献

1. **Apache Spark.** (2024). *Spark SQL Programming Guide.*
2. **Zaharia, M., et al.** (2016). *Apache Spark: a unified engine for big data processing.* Communications of the ACM.
3. **Karau, H., et al.** (2015). *Learning Spark: Lightning-Fast Big Data Analysis.* O'Reilly.

---

## 📄 License | 许可证

MIT License.

---

<div align="center">

**Built with 📊 for big data processing**

[GitHub](https://github.com/Windyhhh/User-Behavior-Log-SparkSQL)

</div>
