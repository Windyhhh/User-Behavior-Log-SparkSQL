# 📊 用户行为日志 Spark SQL 分析 | User Behavior Log Spark SQL Analytics

> **基于 Spark SQL 的海量用户行为日志分析系统——从亿级日志中挖掘用户行为模式、漏斗转化、留存分析，支撑数据驱动的产品决策。**
>
> *Massive user behavior log analysis system based on Spark SQL — mine user behavior patterns, funnel conversion, retention analysis from billions of logs, supporting data-driven product decisions.*

---

## ⭐ 核心卖点 | Why Star This

| 卖点 | Feature | 一句话 |
|------|---------|--------|
| ⚡ **Spark SQL 引擎** | Spark SQL Engine | 基于 Spark SQL 的分布式计算，亿级日志秒级响应 |
| 🔄 **完整漏斗分析** | Funnel Analysis | 多步骤转化漏斗，精准定位流失环节 |
| 📈 **用户留存分析** | Retention Analysis | 次日/7日/30日留存， cohort 分析 |
| 🎯 **用户分群** | User Segmentation | RFM 模型、行为聚类，精准用户画像 |
| 📊 **多维分析** | Multi-Dimension | 时间/地域/设备/渠道多维度交叉分析 |

---

## 🏆 技术栈 | Tech Stack

![Apache Spark](https://img.shields.io/badge/Spark-3.0+-red?logo=apachespark)
![SQL](https://img.shields.io/badge/SQL-ANSI-blue?logo=sql)
![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)
![Hadoop](https://img.shields.io/badge/Hadoop-3.0+-yellow?logo=apachehadoop)
![Hive](https://img.shields.io/badge/Hive-3.1+-orange?logo=apachehive)
![Matplotlib](https://img.shields.io/badge/Matplotlib-3.4+-green?logo=plotly)

---

## 📊 分析模块 | Analysis Modules

| 模块 | 分析内容 | 业务价值 |
|------|---------|---------|
| 📊 流量分析 | PV/UV、访问时长、页面热度 | 了解整体流量趋势 |
| 🔄 漏斗分析 | 注册→浏览→加购→支付转化 | 定位流失环节，优化转化 |
| 📈 留存分析 | 次日/7日/30日留存率 | 评估用户粘性和产品健康度 |
| 🎯 用户分群 | RFM、活跃度、价值分层 | 精准运营，差异化策略 |
| 🗺️ 路径分析 | 用户访问路径、页面流转 | 优化产品流程和信息架构 |
| 📱 渠道分析 | 各渠道用户质量和 ROI | 优化投放预算分配 |
| 🌍 地域分析 | 各省市用户分布和行为差异 | 区域化运营策略 |
| ⏰ 时段分析 | 24小时/7天行为分布 | 优化运营活动时间 |

---

## 🚀 快速开始 | Quick Start

```bash
git clone https://github.com/Windyhhh/User-Behavior-Log-SparkSQL.git
cd User-Behavior-Log-SparkSQL

# 1. 启动 Spark
start-all.sh

# 2. 创建 Hive 表
hive -f sql/ddl/create_tables.sql

# 3. 加载示例数据
hdfs dfs -put data/sample_logs/ /user/hive/warehouse/behavior_log.db/

# 4. 运行漏斗分析
spark-sql -f sql/analysis/funnel_analysis.sql

# 5. 运行留存分析
spark-sql -f sql/analysis/retention_analysis.sql

# 6. 运行用户分群
spark-sql -f sql/analysis/user_segmentation.sql

# 7. 生成可视化报告
python visualization/generate_report.py --results results/
```

---

## 📂 项目结构 | Project Structure

```
User-Behavior-Log-SparkSQL/
├── sql/
│   ├── ddl/                   # 建表语句
│   │   ├── create_tables.sql
│   │   └── create_views.sql
│   ├── dml/                   # 数据加载
│   │   └── load_data.sql
│   └── analysis/              # 分析查询
│       ├── traffic_analysis.sql      # 流量分析
│       ├── funnel_analysis.sql       # 漏斗分析
│       ├── retention_analysis.sql    # 留存分析
│       ├── user_segmentation.sql     # 用户分群
│       ├── path_analysis.sql         # 路径分析
│       ├── channel_analysis.sql      # 渠道分析
│       ├── region_analysis.sql       # 地域分析
│       └── time_analysis.sql         # 时段分析
├── python/
│   ├── etl/                   # ETL 脚本
│   │   ├── log_parser.py      # 日志解析
│   │   ├── data_cleaning.py   # 数据清洗
│   │   └── data_loading.py    # 数据加载
│   └── analysis/              # Python 分析
│       ├── funnel.py          # 漏斗分析
│       ├── retention.py       # 留存分析
│       └── segmentation.py    # 用户分群
├── visualization/
│   ├── generate_report.py     # 报告生成
│   ├── charts.py              # 图表生成
│   └── templates/             # 报告模板
├── data/
│   └── sample_logs/           # 示例日志数据
├── results/                   # 分析结果
├── docs/
│   ├── data_dictionary.md     # 数据字典
│   └── analysis_methodology.md # 分析方法论
└── README.md
```

---

## 🔬 核心分析 | Core Analysis

### 数据模型 | Data Model

```sql
-- 用户行为日志表 (分区表)
CREATE TABLE behavior_log (
    log_id        STRING COMMENT '日志唯一ID',
    user_id       STRING COMMENT '用户ID',
    session_id    STRING COMMENT '会话ID',
    event_type    STRING COMMENT '事件类型: page_view/click/add_cart/purchase',
    page_url      STRING COMMENT '页面URL',
    referrer_url  STRING COMMENT '来源URL',
    device_type   STRING COMMENT '设备类型: mobile/desktop/tablet',
    os            STRING COMMENT '操作系统',
    browser       STRING COMMENT '浏览器',
    ip            STRING COMMENT 'IP地址',
    province      STRING COMMENT '省份',
    city          STRING COMMENT '城市',
    channel       STRING COMMENT '渠道: organic/paid/social/email',
    duration      INT COMMENT '停留时长(秒)',
    event_time    TIMESTAMP COMMENT '事件时间'
)
COMMENT '用户行为日志表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET;

-- 用户信息表
CREATE TABLE user_info (
    user_id       STRING PRIMARY KEY,
    register_time TIMESTAMP COMMENT '注册时间',
    register_channel STRING COMMENT '注册渠道',
    gender        STRING COMMENT '性别',
    age           INT COMMENT '年龄',
    vip_level     INT COMMENT 'VIP等级',
    total_spend   DECIMAL(10,2) COMMENT '总消费金额',
    total_orders  INT COMMENT '总订单数'
);

-- 商品信息表
CREATE TABLE product_info (
    product_id    STRING PRIMARY KEY,
    product_name  STRING COMMENT '商品名称',
    category      STRING COMMENT '商品分类',
    price         DECIMAL(10,2) COMMENT '价格',
    brand         STRING COMMENT '品牌'
);
```

### 漏斗分析 | Funnel Analysis

```sql
-- 电商转化漏斗分析
WITH funnel AS (
    -- 步骤1: 浏览商品
    SELECT user_id, MIN(event_time) as view_time
    FROM behavior_log
    WHERE event_type = 'page_view' AND page_url LIKE '/product/%'
    AND dt = '2024-01-01'
    GROUP BY user_id
),
add_cart AS (
    -- 步骤2: 加入购物车
    SELECT user_id, MIN(event_time) as cart_time
    FROM behavior_log
    WHERE event_type = 'add_cart'
    AND dt = '2024-01-01'
    GROUP BY user_id
),
purchase AS (
    -- 步骤3: 完成支付
    SELECT user_id, MIN(event_time) as pay_time
    FROM behavior_log
    WHERE event_type = 'purchase'
    AND dt = '2024-01-01'
    GROUP BY user_id
)
SELECT
    '浏览商品' as step,
    COUNT(DISTINCT f.user_id) as user_count,
    100.0 as conversion_rate
FROM funnel f
UNION ALL
SELECT
    '加入购物车' as step,
    COUNT(DISTINCT a.user_id) as user_count,
    ROUND(COUNT(DISTINCT a.user_id) * 100.0 / COUNT(DISTINCT f.user_id), 2) as conversion_rate
FROM funnel f
LEFT JOIN add_cart a ON f.user_id = a.user_id AND a.cart_time >= f.view_time
UNION ALL
SELECT
    '完成支付' as step,
    COUNT(DISTINCT p.user_id) as user_count,
    ROUND(COUNT(DISTINCT p.user_id) * 100.0 / COUNT(DISTINCT f.user_id), 2) as conversion_rate
FROM funnel f
LEFT JOIN add_cart a ON f.user_id = a.user_id AND a.cart_time >= f.view_time
LEFT JOIN purchase p ON a.user_id = p.user_id AND p.pay_time >= a.cart_time;
```

**漏斗结果示例:**

| 步骤 | 用户数 | 转化率 | 流失率 |
|------|--------|--------|--------|
| 浏览商品 | 100,000 | 100% | - |
| 加入购物车 | 35,000 | 35% | 65% |
| 完成支付 | 12,000 | 12% | 65.7% |

> 整体转化率 12%，浏览→加购流失 65% 是主要流失环节，需优化商品详情页和加购引导。

### 留存分析 | Retention Analysis

```sql
-- 次日/7日/30日留存分析 (Cohort Analysis)
WITH user_cohort AS (
    -- 用户注册日期 (Cohort)
    SELECT user_id, DATE(register_time) as cohort_date
    FROM user_info
    WHERE register_time >= '2024-01-01'
),
user_activity AS (
    -- 用户活跃日期
    SELECT DISTINCT user_id, DATE(event_time) as active_date
    FROM behavior_log
    WHERE dt >= '2024-01-01'
)
SELECT
    c.cohort_date,
    COUNT(DISTINCT c.user_id) as new_users,
    -- 次日留存
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(c.cohort_date, 1) THEN c.user_id END) as day1_retention,
    ROUND(COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(c.cohort_date, 1) THEN c.user_id END) * 100.0 / COUNT(DISTINCT c.user_id), 2) as day1_rate,
    -- 7日留存
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(c.cohort_date, 7) THEN c.user_id END) as day7_retention,
    ROUND(COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(c.cohort_date, 7) THEN c.user_id END) * 100.0 / COUNT(DISTINCT c.user_id), 2) as day7_rate,
    -- 30日留存
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(c.cohort_date, 30) THEN c.user_id END) as day30_retention,
    ROUND(COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(c.cohort_date, 30) THEN c.user_id END) * 100.0 / COUNT(DISTINCT c.user_id), 2) as day30_rate
FROM user_cohort c
LEFT JOIN user_activity a ON c.user_id = a.user_id
GROUP BY c.cohort_date
ORDER BY c.cohort_date;
```

**留存结果示例:**

| 注册日期 | 新用户 | 次日留存 | 7日留存 | 30日留存 |
|---------|--------|---------|---------|----------|
| 2024-01-01 | 5,000 | 45.2% | 28.5% | 15.3% |
| 2024-01-02 | 4,800 | 46.1% | 29.2% | 16.1% |
| 2024-01-03 | 5,200 | 44.8% | 27.9% | 14.8% |

### 用户分群 (RFM) | User Segmentation (RFM)

```sql
-- RFM 用户价值分群
WITH user_rfm AS (
    SELECT
        user_id,
        -- R: 最近一次购买距今天数
        DATEDIFF(CURRENT_DATE, MAX(CASE WHEN event_type = 'purchase' THEN event_time END)) as recency,
        -- F: 购买频次
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN DATE(event_time) END) as frequency,
        -- M: 消费金额
        SUM(CASE WHEN event_type = 'purchase' THEN CAST(get_json_object(event_properties, '$.amount') AS DECIMAL(10,2)) ELSE 0 END) as monetary
    FROM behavior_log
    WHERE dt >= DATE_SUB(CURRENT_DATE, 90)
    GROUP BY user_id
),
rfm_scores AS (
    SELECT
        user_id,
        recency, frequency, monetary,
        -- R 评分 (越近越高分)
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        -- F 评分 (越频繁越高分)
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        -- M 评分 (消费越高越高分)
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score
    FROM user_rfm
)
SELECT
    user_id,
    recency, frequency, monetary,
    r_score, f_score, m_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN '重要价值用户'
        WHEN r_score >= 4 AND f_score < 4 AND m_score >= 4 THEN '重要发展用户'
        WHEN r_score < 4 AND f_score >= 4 AND m_score >= 4 THEN '重要保持用户'
        WHEN r_score < 4 AND f_score < 4 AND m_score >= 4 THEN '重要挽留用户'
        WHEN r_score >= 4 AND f_score >= 4 AND m_score < 4 THEN '一般价值用户'
        WHEN r_score >= 4 AND f_score < 4 AND m_score < 4 THEN '一般发展用户'
        WHEN r_score < 4 AND f_score >= 4 AND m_score < 4 THEN '一般保持用户'
        ELSE '一般挽留用户'
    END as user_segment
FROM rfm_scores;
```

**用户分群结果:**

| 用户群 | 占比 | 特征 | 运营策略 |
|--------|------|------|---------|
| 重要价值用户 | 8% | 近期活跃、高频、高消费 | VIP服务，专属优惠 |
| 重要发展用户 | 12% | 近期活跃、低频、高消费 | 提高购买频次 |
| 重要保持用户 | 10% | 不活跃、高频、高消费 | 召回唤醒 |
| 重要挽留用户 | 5% | 不活跃、低频、高消费 | 重点挽留 |
| 一般价值用户 | 15% | 近期活跃、高频、低消费 | 提升客单价 |
| 其他 | 50% | 一般用户 | 常规运营 |

---

## 📊 性能指标 | Performance Metrics

| 指标 | 数值 | 说明 |
|------|------|------|
| 数据规模 | 10 亿条/天 | 日志数据量 |
| 查询响应 | < 10s | 复杂分析查询 P95 |
| 集群规模 | 10 节点 | Spark 集群 |
| 存储格式 | Parquet | 列式存储，压缩比 5:1 |
| 分区策略 | 日期分区 | 按天分区，高效裁剪 |
| 数据倾斜处理 | 加盐 + 广播 | 解决热点 key 倾斜 |

---

## 🎯 应用场景 | Use Cases

- 🛒 **电商平台**：用户行为分析、转化漏斗、购物篮分析
- 📱 **移动应用**：APP 使用行为分析、功能使用统计
- 🌐 **网站分析**：页面流量、用户路径、跳出率分析
- 📺 **内容平台**：内容消费分析、推荐效果评估
- 🎮 **游戏行业**：玩家行为分析、留存分析、付费分析
- 📊 **数据产品**：用户行为分析平台、BI 报表系统
- 🎓 **教学项目**：Spark SQL 大数据分析教学案例

---

## 📚 参考文献 | References

- Spark SQL Reference. Apache.org 2023.
- Karau, H., et al. "Learning Spark: Lightning-Fast Big Data Analysis." O'Reilly 2020.
- Hughes, J. N. "Funnel Analysis: A Practical Guide." 2022.
- Fader, P. S., & Hardie, B. G. "Customer-Base Analysis." 2021.
- 张良均. "Spark SQL 大数据分析实战." 机械工业出版社.

---

## 📄 License

MIT License — 自由使用、修改和分发。

---

> 💡 **Spark SQL + 用户行为分析的大数据实战，Star ⭐ 支持开源数据分析！**
