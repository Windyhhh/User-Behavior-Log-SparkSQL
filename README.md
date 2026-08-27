<div align="center">

# 📋 User-Behavior-Log-SparkSQL

### Apache Spark SQL user-behavior log analysis.

A log pipeline with data generator, student statistics and Top-N broadcaster experiments.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-SQL-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

</div>

---

**User-Behavior-Log-SparkSQL** analyzes user-behavior logs with **Apache Spark SQL** — a log pipeline with a data generator, student statistics and **Top-N broadcaster** experiments.

> [!NOTE]
> 中文项目：Apache Spark SQL 用户行为日志分析——日志流水线、数据生成器、学生统计 + TopN 主播实验。

---

## Quickstart

```bash
git clone https://github.com/Windyhhh/User-Behavior-Log-SparkSQL.git
cd User-Behavior-Log-SparkSQL

# data generation + log analysis
python bigdata_project/data_generator.py
python bigdata_project/log_analysis.py
```

---

## Features

- **Spark SQL pipeline** — distributed log analysis.
- **Data generator** — synthetic user-behavior logs.
- **Top-N experiments** — broadcaster ranking.

---

## Project Structure

```
User-Behavior-Log-SparkSQL/
├── bigdata_project/
│   ├── data_generator.py
│   ├── log_analysis.py
│   ├── config.py
│   └── requirements.txt
├── format_namenode.sh
└── 实验*/              # per-experiment scripts
```

---

## License

MIT — free to use, modify and distribute.
