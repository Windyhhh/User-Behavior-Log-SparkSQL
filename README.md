<div align="center">

# 📋 User-Behavior-Log-SparkSQL

### Spark SQL user-behavior log analytics.

Clean, store, analyze and persist user logs with multi-dimensional stats and Top-N ranking — 10× faster.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-SQL-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

</div>

---

**User-Behavior-Log-SparkSQL** analyzes user-behavior logs with **Spark SQL** — cleaning, storing, analyzing and persisting logs with **multi-dimensional statistics** and **Top-N ranking**. It processes **1M log records in ~30s**, over 10× faster than single-machine approaches, with >99.9% cleaning accuracy.

> [!NOTE]
> 中文项目：Spark SQL 用户行为日志分析——清洗/存储/统计/落库，多维度分析 + TopN 排名，100 万条日志 30 秒，效率提升 10 倍。

---

## Features

- **Spark SQL pipeline** — distributed log cleaning, storage, analysis.
- **Multi-dimensional stats** — deep user-behavior insights.
- **Top-N ranking** — broadcaster / entity rankings.
- **Fast** — 1M records < 30s (10× speedup).
- **Accurate** — >99.9% cleaning accuracy.

---

## Quickstart

```bash
git clone https://github.com/Windyhhh/User-Behavior-Log-SparkSQL.git
cd User-Behavior-Log-SparkSQL

# generate data + run analysis
python bigdata_project/data_generator.py
python bigdata_project/log_analysis.py
```

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
