import org.apache.spark.sql.SparkSession
/**
  * Created by brendan on 4/18/17.
  */
object timequery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
            .builder()
            .appName("timequery")
            .master("local")
            .getOrCreate()

    // load in trends file
    val df = spark.read.json("trends.txt")
    df.createOrReplaceTempView("file")

    // get the to time topic spent trending by getting max time - min time and the topic
    val query = spark.sql("SELECT  max(timestamp)-min(timestamp) as mm, tnd.name FROM file LATERAL VIEW explode(details.trends[0])" +
      " AS tnd GROUP BY tnd.name").toDF()
    query.printSchema()
    query.createOrReplaceTempView("minmax")
    // Then average all of the times spent trending and output
    val ave = spark.sql("SELECT avg(mm) as avgHTtrendingTime FROM minmax").show()

  }
}
