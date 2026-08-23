import random
import json
from datetime import datetime, timedelta

CITIES = {
    "北京": {"code": "BJ", "district": "华北"},
    "上海": {"code": "SH", "district": "华东"},
    "广州": {"code": "GZ", "district": "华南"},
    "深圳": {"code": "SZ", "district": "华南"},
    "杭州": {"code": "HZ", "district": "华东"},
    "成都": {"code": "CD", "district": "西南"},
    "武汉": {"code": "WH", "district": "华中"},
    "南京": {"code": "NJ", "district": "华东"},
    "西安": {"code": "XA", "district": "西北"},
    "重庆": {"code": "CQ", "district": "西南"}
}

COURSES = [
    ("大数据基础", "BD001"),
    ("Hadoop实战", "HD001"),
    ("Spark入门", "SP001"),
    ("机器学习", "ML001"),
    ("深度学习", "DL001"),
    ("Python编程", "PY001"),
    ("数据挖掘", "DM001"),
    ("数据可视化", "DV001"),
    ("实时计算", "RT001"),
    ("云计算概论", "CC001")
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 Chrome/91.0.4472.120",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)"
]

def generate_log_entry(log_id, start_time, days=7):
    city_name = random.choice(list(CITIES.keys()))
    city_info = CITIES[city_name]
    course_name, course_code = random.choice(COURSES)
    
    timestamp = start_time + timedelta(
        seconds=random.randint(0, 86400 * days - 1)
    )
    
    user_id = f"U{random.randint(10001, 50000)}"
    session_id = f"S{random.randint(100001, 500000)}"
    
    is_mobile = random.random() < 0.35
    
    return {
        "log_id": log_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id,
        "session_id": session_id,
        "course_name": course_name,
        "course_code": course_code,
        "city": city_name,
        "district": city_info["district"],
        "ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "user_agent": random.choice(USER_AGENTS) + (" Mobile" if is_mobile else ""),
        "device_type": "mobile" if is_mobile else "desktop",
        "visit_duration": random.randint(10, 3600),
        "traffic_bytes": random.randint(1024, 10485760),
        "http_status": random.choices([200, 301, 404, 500], weights=[85, 5, 8, 2])[0],
        "referrer": random.choice(["direct", "search", "social", "email", "course_list"]) if random.random() > 0.2 else "direct",
        "page_views": random.randint(1, 20)
    }

def generate_sample_logs(num_logs=100000, output_file="user_behavior_logs.json"):
    start_time = datetime(2025, 12, 1)
    
    print(f"正在生成 {num_logs} 条用户行为日志...")
    
    logs = []
    for i in range(num_logs):
        log_entry = generate_log_entry(i + 1, start_time)
        logs.append(log_entry)
        
        if (i + 1) % 10000 == 0:
            print(f"  已生成 {i + 1} 条日志...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for log in logs:
            f.write(json.dumps(log, ensure_ascii=False) + '\n')
    
    print(f"日志文件已生成: {output_file}")
    print(f"总记录数: {len(logs)}")
    
    return output_file

if __name__ == "__main__":
    generate_sample_logs(100000, "user_behavior_logs.json")
