package tpt.predictor.controler

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

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
  val visitorsDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, VISITORS), classOf[Character]).toDF.drop("_id")
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
    .where(col("category_id").notEqual(lit("")))
    .limit(10)
  top_10_most_viewed_categories.cache

  val parcoursCustomers = parcoursDs.where(col("customer_id").notEqual(lit("")))
  parcoursCustomers.cache

  var customersHistory = parcoursCustomers.join(productsDs, productsDs("product_id")===parcoursCustomers("product_id"))
    .toDF("visitor_id","customer_id","produit_product_id", "timestamp", "parcours_product_id", "category_id")
    //.where(productsDs("category_id").isNotNull)
    .orderBy("timestamp")
    .groupBy("customer_id").agg(collect_list("parcours_product_id").as("listes_produits"),collect_list("category_id").as("categorie_produits"))
    .where("listes_produits[0] is not null")
    .where("listes_produits[1] is not null")
    .where("listes_produits[2] is not null")
    .select(col("customer_id"), expr("listes_produits[0]"), expr("listes_produits[1]"), expr("listes_produits[2]"), expr("categorie_produits[0]"), expr("categorie_produits[1]"), expr("categorie_produits[2]"))
  customersHistory.cache


  var customersInformations  = parcoursCustomers.join(customersDs, customersDs("customer_id")===parcoursCustomers("customer_id"))
    .select(parcoursCustomers.col("customer_id"), col("country_code"), col("city"), col("age_class;").as("age_class"))
  customersInformations.cache

  var mostFrequentAgeClass:String = customersInformations.where("age_class <> ';'")
    .groupBy("age_class")
    .count
    .orderBy(desc("count"))
    .collect()(0)(0).toString

  mostFrequentAgeClass = mostFrequentAgeClass.split(";")(0)

  customersInformations = customersInformations.withColumn("age_class", when(col("age_class")===lit(";"),mostFrequentAgeClass)
    .otherwise(split(col("age_class"), ";").getItem(0)))
  customersInformations.cache

  var customersFullData = customersHistory.join(customersInformations, customersInformations.col("customer_id").equalTo(customersHistory.col("customer_id"))).distinct()
  customersFullData.cache

  println("count info : " + customersInformations.count()+" -- count histo : " + customersHistory.count())

  def computeModele() = {
    // visitorsDs.show()
  }

  def predictCostumer() = {
    // top_10_most_viewed_items.show()
  }

  def predictVisitor() = {
    // top_10_most_viewed_items.show()
  }
}
