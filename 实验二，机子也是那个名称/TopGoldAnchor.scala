import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TopGoldAnchor {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: TopGoldAnchor <gift_input_path> <video_input_path> <output_path>")
      System.exit(1)
    }
    
    val giftInputPath = args(0)
    val videoInputPath = args(1)
    val outputPath = args(2)
    
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("TopGoldAnchor")
      .getOrCreate()
    
    // 读取礼物记录数据
    val giftDF = spark.read.json(giftInputPath)
    
    // 读取视频信息数据
    val videoDF = spark.read.json(videoInputPath)
    
    // 关联礼物记录和视频信息，获取每个礼物对应的区域
    val joinedDF = giftDF.join(videoDF, Seq("vid"), "inner")
      .select(
        col("area"),
        col("vid"),
        col("gold").cast("int").as("gold")
      )
    
    // 按照区域和vid分组，计算每个主播的金币收入总和
    val goldSumDF = joinedDF.groupBy("area", "vid")
      .agg(sum("gold").as("total_gold"))
    
    // 使用窗口函数对每个区域内的主播按照金币收入排序，取Top3
    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window.partitionBy("area").orderBy(col("total_gold").desc)
    
    val topNDF = goldSumDF.withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") <= 3)
      .select("area", "vid", "total_gold", "rank")
    
    // 将结果保存到HDFS
    topNDF.write.mode("overwrite").json(outputPath)
    
    // 显示结果
    topNDF.show()
    
    spark.stop()
  }
}