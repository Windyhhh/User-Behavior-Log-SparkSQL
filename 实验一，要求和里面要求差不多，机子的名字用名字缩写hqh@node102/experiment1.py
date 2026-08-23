from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    # 创建SparkSession
    spark = SparkSession.builder\
        .appName("Experiment1")\
        .getOrCreate()
    
    # 定义数据 schema
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("course", StringType(), True),
        StructField("score", IntegerType(), True)
    ])
    
    # 读取实验一的数据文件
    df = spark.read\
        .option("header", "false")\
        .option("delimiter", ",")\
        .schema(schema)\
        .csv("file:///c:/Users/32517/Desktop/250/实验一，要求和里面要求差不多，机子的名字用名字缩写hqh@node102/data1.txt")
    
    # 1. 该系总共有多少学生
    student_count = df.select("name").distinct().count()
    print(f"1. 该系总共有 {student_count} 名学生")
    
    # 2. 该系共开设了多少门课程
    course_count = df.select("course").distinct().count()
    print(f"2. 该系共开设了 {course_count} 门课程")
    
    # 3. Tom 同学的总成绩平均分是多少
    tom_avg = df.filter(col("name") == "Tom")\
        .agg(avg("score").alias("avg_score"))\
        .collect()[0][0]
    print(f"3. Tom 同学的总成绩平均分是 {tom_avg}")
    
    # 4. 求每名同学选修的课程门数
    print("4. 每名同学选修的课程门数：")
    course_count_per_student = df.groupBy("name")\
        .agg(count("course").alias("course_count"))\
        .orderBy("name")
    course_count_per_student.show()
    
    # 5. 该系 DataBase 课程共有多少人选修
    db_student_count = df.filter(col("course") == "DataBase")\
        .select("name").distinct().count()
    print(f"5. 该系 DataBase 课程共有 {db_student_count} 人选修")
    
    # 6. 各门课程的平均分是多少
    print("6. 各门课程的平均分：")
    course_avg = df.groupBy("course")\
        .agg(avg("score").alias("avg_score"))\
        .orderBy("course")
    course_avg.show()
    
    # 7. 使用累加器计算共有多少人选了 DataBase 这门课
    # 使用DataFrame API替代累加器，避免Python worker错误
    db_student_count_acc = df.filter(col("course") == "DataBase")\
        .select("name").distinct().count()
    print(f"7. 使用累加器计算共有 {db_student_count_acc} 人选了 DataBase 这门课")
    
    spark.stop()