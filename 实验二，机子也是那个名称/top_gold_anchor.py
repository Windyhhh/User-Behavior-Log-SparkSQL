from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    # 创建SparkSession
    spark = SparkSession.builder\
        .appName("TopGoldAnchor")\
        .getOrCreate()
    
    # 读取礼物记录数据
    gift_df = spark.read.json("file:///c:/Users/32517/Desktop/250/实验二，机子也是那个名称/gift_record.log")
    
    # 读取视频信息数据
    video_df = spark.read.json("file:///c:/Users/32517/Desktop/250/实验二，机子也是那个名称/video_info.log")
    
    # 关联礼物记录和视频信息，获取每个礼物对应的区域
    joined_df = gift_df.join(video_df, "vid", "inner")\
        .select(
            col("area"),\
            col("vid"),\
            col("gold").cast("int").alias("gold")\
        )
    
    # 按照区域和vid分组，计算每个主播的金币收入总和
    gold_sum_df = joined_df.groupBy("area", "vid")\
        .agg(sum("gold").alias("total_gold"))
    
    # 使用窗口函数对每个区域内的主播按照金币收入排序，取Top3
    window_spec = Window.partitionBy("area").orderBy(col("total_gold").desc())
    
    topN_df = gold_sum_df.withColumn("rank", row_number().over(window_spec))\
        .filter(col("rank") <= 3)\
        .select("area", "vid", "total_gold", "rank")
    
    # 显示结果
    topN_df.show()
    
    spark.stop()