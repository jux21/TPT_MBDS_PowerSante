package mbds.tpt

import com.mongodb.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object App {

    def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://localhost/tpt_power_sante.products")
      .config("spark.mongodb.output.uri", "mongodb://localhost/tpt_power_sante.products")
      .getOrCreate()

    val sc = SparkContext.getOrCreate();

    val rdd = MongoSpark.load(spark)

    rdd.createOrReplaceTempView("characters")
    rdd.printSchema()

    spark.sql("SELECT * FROM characters WHERE category_id = '0cce5e2ff34bd08442f322139c66536d314d6c4c'").show()

  }

}
