package tpt.predictor.controler

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{DataTypes, DateType, TimestampType}
import org.apache.spark.sql.functions.udf

class Predictor() {

  private val COLLECTION = "COLLECTION"
  private val CUSTOMERS = "customers"
  private val PRODUCTS = "products"
  private val PARCOURS = "parcours"
  private val VISITORS = "visitors"

  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://localhost/tpt_power_sante.customers")
    .config("spark.mongodb.output.uri", "mongodb://localhost/tpt_power_sante.customers")
    .getOrCreate()

  // Create a JavaSparkContext using the SparkSession's SparkContext object
  val jsc = new JavaSparkContext(sparkSession.sparkContext)

  // Load VISITORS
  val visitorsDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, VISITORS), classOf[Character]).toDF
    .select(col("visitor_id"), col("browser_lang"),col("country_ip"), col("city_ip"))
  visitorsDs.cache

  // Load PARCOURS
  val parcoursDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, PARCOURS), classOf[Character]).toDF.drop("_id")
  parcoursDs.cache

  // Load PRODUCTS
  val productsDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, PRODUCTS), classOf[Character]).toDF.drop("_id")
  productsDs.cache

  // Load CUSTOMERS
  val customersDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, CUSTOMERS), classOf[Character]).toDF.drop("_id")
  customersDs.cache

  // --- Most viewed item for hot-items recommandations
  val top_10_most_viewed_items = parcoursDs.groupBy("product_id")
    .agg(count("*").alias("numberOfViewsForItem"))
    .sort(desc("numberOfViewsForItem"))
    .where(col("product_id").isNotNull)
    .limit(10)
  top_10_most_viewed_items.cache

  // --- Most viewed categories for hot-categories recommandations
  val top_10_most_viewed_categories = productsDs.join(parcoursDs, productsDs.col("product_id").equalTo(parcoursDs.col("product_id")))
    .groupBy(col("category_id")).agg(count(productsDs.col("product_id")).as("numberOfViewsForCategorie"))
    .sort(desc("numberOfViewsForCategorie"))
    .where("category_id != '' ")
    .limit(10)
  top_10_most_viewed_categories.cache

  // ----------------------------
  // PARCOURS DES CUSTOMERS ...
  // ----------------------------

  var products = productsDs.withColumn("category_id", when(col("category_id").equalTo(lit("")), null)
    .otherwise(col("category_id"))).filter("category_id is not null")
  products.cache

  var parcours = parcoursDs.withColumn("product_id", when(col("product_id").equalTo(lit("")), null)
    .otherwise(col("product_id")))
    .filter("product_id is not null")
    .withColumn("customer_id", when(col("customer_id").equalTo(lit("")), null)
      .otherwise(col("customer_id")))
    .filter("customer_id is not null")
  parcours.cache

  /*
    var customersHistory = parcours.join(products, products("product_id") === parcours("product_id"))
      // .toDF("visitor_id", "customer_id", "produit_product_id", "timestamp", "parcours_product_id", "category_id")
      //.filter(expr("category_id != '' "))
      .orderBy("timestamp")
      .groupBy("customer_id").agg(collect_list(parcours.col("product_id")).as("listes_produits"), collect_list("category_id").as("categorie_produits"), collect_list("timestamp").as("ts_consultation"))
      // .where("customer_id is not null ")
      .where("listes_produits[0] is not null ")
      .where("listes_produits[1] is not null ")
      .where("listes_produits[2] is not null ")
      .select(col("customer_id"), expr("ts_consultation[0]"), expr("ts_consultation[1]"), expr("ts_consultation[2]"), expr("listes_produits[0]"), expr("listes_produits[1]"), expr("listes_produits[2]"), expr("categorie_produits[0]"), expr("categorie_produits[1]"), expr("categorie_produits[2]"))
    customersHistory.cache()
  */
  // Feature engineering sur les timestamp
  def roundInt = udf((value: Int) => {
    Math.round(((value + 5) / 10) * 10)
  })

  /*
   for (i <- 0 to 2) {
     var colName = "ts_consultation[" + i + "]"
     customersHistory = customersHistory.withColumn(colName + "_TS", customersHistory(colName).cast(TimestampType))
     customersHistory = customersHistory.withColumn("dayOfWeek_view_item_" + i, from_unixtime(customersHistory(colName), "EEEE"))
     customersHistory = customersHistory.withColumn("hour_view_item_" + i, split(col(colName + "_TS"), "\\ ").getItem(1))
     customersHistory = customersHistory.withColumn("hour_view_item_" + i, split(col("hour_view_item_" + i), "\\:").getItem(0))
     customersHistory = customersHistory.withColumn("dayOfMonth_view_item_" + i, split(col(colName + "_TS"), "\\ ").getItem(0))
     customersHistory = customersHistory.withColumn("dayOfMonth_view_item_" + i, split(col("dayOfMonth_view_item_" + i), "\\-").getItem(2))
     customersHistory = customersHistory.withColumn("dayOfYear_view_item_" + i, col(colName + "_TS"))
     customersHistory = customersHistory.withColumn("dayOfYear_view_item_" + i, dayofyear(from_unixtime(unix_timestamp(col("dayOfYear_view_item_" + i), "yyyy-MM-dd"))))
   }

   var customersInformations = parcours.join(customersDs, customersDs("customer_id") === parcours("customer_id"))
     //.toDF("parcours_visitor_id", "parcours_customer_id", "product_id", "timestamp", "customer_id", "registration_ts", "country_code", "city", "age_class")
     .select(parcours.col("customer_id"), col("country_code"), col("city"), col("age_class;").as("age_class"))
   customersInformations.cache

   var mostFrequentAgeClass: String = customersInformations.where("age_class <> ';'")
     .groupBy("age_class").count.orderBy(desc("count"))
     .collect()(0)(0).toString
   mostFrequentAgeClass = mostFrequentAgeClass.split(";")(0)
   customersInformations = customersInformations.withColumn("age_class", when(col("age_class") === lit(";"), mostFrequentAgeClass)
     .otherwise(split(col("age_class"), ";").getItem(0)))

   var customersFullData = customersHistory
     .join(customersInformations, customersInformations("customer_id") === customersHistory("customer_id")).distinct()
   customersFullData.cache
   customersFullData.createOrReplaceTempView("customersFullData")

   customersHistory.createOrReplaceTempView("customersHistory") */

  // ----------------------------
  // PARCOURS DES VISITEURS ...
  // ----------------------------

  var parcoursVisitors = parcoursDs.withColumn("visitor_id", when(col("visitor_id").equalTo(lit("")), null)
    .otherwise(col("visitor_id")))
    .filter("visitor_id is not null")

  var visitorsHistory = parcoursVisitors.join(products, products("product_id") === parcoursVisitors("product_id"))
    .orderBy("timestamp")
    .groupBy("visitor_id").agg(collect_list(parcoursVisitors.col("product_id")).as("listes_produits"), collect_list("category_id").as("categorie_produits"), collect_list("timestamp").as("ts_consultation"))
    .where("listes_produits[0] is not null")
    .where("listes_produits[1] is not null")
    .where("listes_produits[2] is not null")
    .select(col("visitor_id"), expr("ts_consultation[0]"), expr("ts_consultation[1]"), expr("ts_consultation[2]"), expr("listes_produits[0]"), expr("listes_produits[1]"), expr("listes_produits[2]"), expr("categorie_produits[0]"), expr("categorie_produits[1]"), expr("categorie_produits[2]"))
  visitorsHistory.cache

  println("aaaaaaaaaaaaa " + visitorsHistory.count())

  for (i <- 0 to 2) {
    var colName = "ts_consultation[" + i + "]"
    visitorsHistory = visitorsHistory.withColumn(colName + "_TS", visitorsHistory(colName).cast(TimestampType))
    visitorsHistory = visitorsHistory.withColumn("dayOfWeek_view_item_" + i, from_unixtime(visitorsHistory(colName), "EEEE"))
    visitorsHistory = visitorsHistory.withColumn("hour_view_item_" + i, split(col(colName + "_TS"), "\\ ").getItem(1))
    visitorsHistory = visitorsHistory.withColumn("hour_view_item_" + i, split(col("hour_view_item_" + i), "\\:").getItem(0))
    visitorsHistory = visitorsHistory.withColumn("dayOfMonth_view_item_" + i, split(col(colName + "_TS"), "\\ ").getItem(0))
    visitorsHistory = visitorsHistory.withColumn("dayOfMonth_view_item_" + i, split(col("dayOfMonth_view_item_" + i), "\\-").getItem(2))
    visitorsHistory = visitorsHistory.withColumn("dayOfYear_view_item_" + i, col(colName + "_TS"))
    visitorsHistory = visitorsHistory.withColumn("dayOfYear_view_item_" + i, dayofyear(from_unixtime(unix_timestamp(col("dayOfYear_view_item_" + i), "yyyy-MM-dd"))))
  }

  var vistorInformations  = parcoursVisitors.join(visitorsDs, visitorsDs("visitor_id")===parcoursVisitors("visitor_id"))
    // .toDF("parcours_visitor_id", "customer_id", "product_id", "timestamp", "visitor_id", "first_visit_ts", "last_visit_ts", "os", "os_version", "browser", "browser_version", "browser_lang", "country_ip", "city_ip", "device_desc")
    .select(parcoursVisitors.col("visitor_id"), col("browser_lang"), col("country_ip"), col("city_ip"))

  var mostFrequentCityIP:String = vistorInformations.where("city_ip <> ';'")
    .groupBy("city_ip")
    .count.orderBy(desc("count"))
    .collect()(0)(0).toString
  mostFrequentCityIP = mostFrequentCityIP.split(";")(0)
  vistorInformations = vistorInformations.withColumn("city_ip", when(col("city_ip").isNull,mostFrequentCityIP).otherwise(col("city_ip")))

  var visitorsFullData = visitorsHistory.join(vistorInformations, vistorInformations("visitor_id")===visitorsHistory("visitor_id")).distinct()

  visitorsFullData.show(false)
  println("rerererererer " + visitorsFullData.count())
  visitorsFullData.createOrReplaceTempView("visitorsFullData")

  visitorsHistory.createOrReplaceTempView("visitorsHistory")

  def predictCostumer() = {
    // top_10_most_viewed_items.show()
  }

  def predictVisitor() = {
    // top_10_most_viewed_items.show()
  }
}
