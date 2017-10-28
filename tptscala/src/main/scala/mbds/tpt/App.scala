package mbds.tpt

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object App {

    def main(args: Array[String]) {

      val databaseName = "tpt_power_sante"
      var collectionName = "parcours"
      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("TPT")
        .config("spark.mongodb.input.uri", "mongodb://localhost/"+databaseName+"."+collectionName)
        .config("spark.mongodb.output.uri", "mongodb://localhost/"+databaseName+"."+collectionName)
        .getOrCreate()

      val sc = SparkContext.getOrCreate();
      sc.setLogLevel("WARN")

      // get the most viewed item for hot-items recommandations
      val dataframeParcours = MongoSpark.load(spark)
      dataframeParcours.createOrReplaceTempView("parcours")
      dataframeParcours.printSchema()
      val top_10_most_viewed_items = spark.sql(
        "SELECT product_id as top_10_most_viewed_items, count(*) as nb_of_views " +
        "FROM parcours " +
        "GROUP BY product_id " +
        "ORDER BY nb_of_views DESC " +
        "LIMIT 10").show(false)

      // get the most viewed categories for hot-categories recommandations
      // changing spark/mongodb config
      collectionName = "products"
      val dataframeProducts = MongoSpark.load(spark, ReadConfig(Map(
        "uri" -> "mongodb://127.0.0.1",
        "database" -> databaseName,
        "collection" -> collectionName)
      ))
      dataframeProducts.createOrReplaceTempView("products")
      dataframeProducts.printSchema()
      /*val top_10_most_viewed_categories = spark.sql(
        "SELECT distinct(category_id) as top_10_most_viewed_categories " +
        "FROM parcours " +
        "INNER JOIN products ON parcours.product_id = products.product_id " +
        "WHERE products.product_id = 'e9b12d1a4129a1037fedce747480f5234278b0cb' " +
        "LIMIT 10 ").show(false)*/
      val top_10_most_viewed_categories = spark.sql(
        "SELECT distinct(category_id) as top_10_most_viewed_categories " +
          "FROM products " +
          "WHERE products.product_id IN(" +
          "    SELECT product_id " +
          "    FROM parcours " +
          "    GROUP BY product_id " +
          "    ORDER BY count(*) DESC " +
          "    LIMIT 10" +
          ")").show(false)


      //spark.sql("SELECT count(distinct product_id) FROM parcours").show(false)

      /*spark.sql("SELECT visitor_id, count(*) as nb_item_viewed, product_id " +
        "FROM parcours " +
        "GROUP BY visitor_id, product_id " +
        "HAVING count(*) > 1 " +
        "ORDER BY nb_item_viewed DESC " +
        "LIMIT 100").show(100,false)*/


  }

}
