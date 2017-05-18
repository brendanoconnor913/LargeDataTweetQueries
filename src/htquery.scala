import org.apache.spark.sql.SparkSession

/**
  * Created by brendan on 4/26/17.
  */
object htquery {
  def main(args: Array[String]): Unit = {
    // initalize environemnt details
    val spark = SparkSession
      .builder()
      .appName("timequery")
      .master("local")
      .getOrCreate()

    // read in the json tweet data and print the schema
    val df = spark.read.json("data/tweets_1485117000000/")
    df.createOrReplaceTempView("file")
    df.printSchema()

    // get average number of followers by number of other user mentions in tweet
    val numfollowers = df.select("id", "user.followersCount")
    val mentions = spark.sql("SELECT id, size(userMentionEntities.id) as size FROM file")
    val follAndMentions = numfollowers.join(mentions, "id")
    follAndMentions.createOrReplaceTempView("mnf")
    val mentionfinal =
      spark.sql("SELECT size as num_mentions, AVG(followersCount) as avg_num_followers FROM mnf GROUP BY size ORDER BY size")
    mentionfinal.show()

    // get average number of followers by number of ht's used
    val hashtags = spark.sql("SELECT id, size(hashtagEntities) as size FROM file")
    val follAndTags = numfollowers.join(hashtags, "id")
    follAndTags.createOrReplaceTempView("fnt")
    val tagfinal =
      spark.sql("SELECT  size as num_hts, AVG(followersCount) as avg_num_follower FROM fnt GROUP BY size ORDER BY size")
    tagfinal.show()

    // get average number of followers by number of urls used
    val urls = spark.sql("SELECT id, size(mediaEntities.url) as url FROM file")
    val follAndUrl = numfollowers.join(urls, "id")
    follAndUrl.createOrReplaceTempView("fnu")
    val urlfinal =
      spark.sql("SELECT url, AVG(followersCount) as avg_num_follower FROM fnu GROUP BY url ORDER BY url")
    urlfinal.show()
  }
}
