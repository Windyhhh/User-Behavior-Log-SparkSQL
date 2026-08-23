from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.rdd import RDD

if __name__ == "__main__":
    # 创建SparkSession和SparkContext
    spark = SparkSession.builder\
        .appName("Experiment1_RDD")\
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # 从本地文件读取数据创建RDD
    # 数据格式：name,course,score
    lines_rdd = sc.textFile("file:///c:/Users/32517/Desktop/250/实验一，要求和里面要求差不多，机子的名字用名字缩写hqh@node102/data1.txt")
    
    # 解析数据为 (name, course, score) 元组
    def parse_line(line):
        parts = line.split(",")
        return (parts[0], parts[1], int(parts[2]))
    
    data_rdd = lines_rdd.map(parse_line)
    
    # 1. 该系总共有多少学生
    students_rdd = data_rdd.map(lambda x: x[0]).distinct()
    student_count = students_rdd.count()
    print(f"1. 该系总共有 {student_count} 名学生")
    
    # 2. 该系共开设了多少门课程
    courses_rdd = data_rdd.map(lambda x: x[1]).distinct()
    course_count = courses_rdd.count()
    print(f"2. 该系共开设了 {course_count} 门课程")
    
    # 3. Tom 同学的总成绩平均分是多少
    tom_scores = data_rdd.filter(lambda x: x[0] == "Tom")
    tom_total = tom_scores.map(lambda x: x[2]).sum()
    tom_count = tom_scores.count()
    tom_avg = tom_total / tom_count if tom_count > 0 else 0
    print(f"3. Tom 同学的总成绩平均分是 {tom_avg}")
    
    # 4. 求每名同学选修的课程门数
    print("4. 每名同学选修的课程门数：")
    course_count_per_student = data_rdd\
        .map(lambda x: (x[0], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .sortByKey()
    
    for name, count in course_count_per_student.collect():
        print(f"   {name}: {count}门")
    
    # 5. 该系 DataBase 课程共有多少人选修
    db_students = data_rdd\
        .filter(lambda x: x[1] == "DataBase")\
        .map(lambda x: x[0])\
        .distinct()
    db_student_count = db_students.count()
    print(f"5. 该系 DataBase 课程共有 {db_student_count} 人选修")
    
    # 6. 各门课程的平均分是多少
    print("6. 各门课程的平均分：")
    course_scores = data_rdd.map(lambda x: (x[1], (x[2], 1)))
    course_totals = course_scores.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    course_avgs = course_totals.mapValues(lambda x: x[0] / x[1]).sortByKey()
    
    for course, avg_score in course_avgs.collect():
        print(f"   {course}: {avg_score:.2f}分")
    
    # 7. 使用RDD API计算共有多少人选了 DataBase 这门课
    db_students_count = data_rdd\
        .filter(lambda x: x[1] == "DataBase")\
        .map(lambda x: x[0])\
        .distinct()\
        .count()
    
    print(f"7. 使用RDD API计算共有 {db_students_count} 人选了 DataBase 这门课")
    
    spark.stop()
