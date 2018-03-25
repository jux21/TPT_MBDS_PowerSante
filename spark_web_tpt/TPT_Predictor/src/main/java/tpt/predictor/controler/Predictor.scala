package tpt.predictor.controler

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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
  val visitorsDs = MongoSpark.load(jsc, ReadConfig.create(jsc)
    .withOption(COLLECTION, VISITORS), classOf[Character]).toDF

  // Load PARCOURS
  val parcoursDs = MongoSpark.load(jsc, ReadConfig.create(jsc)
    .withOption(COLLECTION, PARCOURS), classOf[Character]).toDF
  parcoursDs.cache

  // Load PRODUCTS
  val productsDs = MongoSpark.load(jsc, ReadConfig.create(jsc)
    .withOption(COLLECTION, PRODUCTS), classOf[Character]).toDF
  productsDs.cache

  // Load CUSTOMERS
  val customersDs = MongoSpark.load(jsc, ReadConfig.create(jsc)
    .withOption(COLLECTION, CUSTOMERS), classOf[Character]).toDF
  customersDs.cache

  def computeModele() = {
    print("blblblblblbl")
    visitorsDs.show()
  }

  def predict(params: String) = {
    print("blblblblblbl")
    visitorsDs.show()
  }
}
