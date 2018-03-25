package tpt.predictor.modele;

import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

public class Modele {

	private static final String CUSTOMERS = "customers";
	private static final String PRODUCTS = "products";
	private static final String PARCOURS = "parcours";
	private static final String VISITORS = "visitors";
	private static final String COLLECTION = "collection";
	private static final String CUSTOMERS_PATH = "mongodb://localhost/tpt_power_sante.customers";
	private Dataset<Row> visitorsDs;
	private Dataset<Row> parcoursDs;
	private Dataset<Row> productsDs;
	private Dataset<Row> customersDs;

	public Modele() {
		// Initialisation du modèle de données
		initModel();
	}

	/**
	 * Initialisation du modèle de données
	 */
	private void initModel() {

		// SparkSession
		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorTour")
				.config("spark.mongodb.input.uri", CUSTOMERS_PATH).config("spark.mongodb.output.uri", CUSTOMERS_PATH)
				.getOrCreate();

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		// Load VISITORS
		visitorsDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, VISITORS), Character.class)
				.toDF();
		visitorsDs.cache();

		// Load PARCOURS
		parcoursDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, PARCOURS), Character.class)
				.toDF();
		parcoursDs.cache();

		// Load PRODUCTS
		productsDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, PRODUCTS), Character.class)
				.toDF();
		productsDs.cache();

		// Load CUSTOMERS
		customersDs = MongoSpark.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, CUSTOMERS), Character.class)
				.toDF();
		customersDs.cache();

		predict(null);

		jsc.close();

	}

	/**
	 * Retourne une prédiction du parcours de l'utilisateur
	 * 
	 * 
	 * @param queryParams
	 */
	public void predict(Set<String> queryParams) {
		System.out.println("PREDICT");
		parcoursDs.show();
		System.out.println(productsDs.count());
		//
		Dataset<Row> mostViewedProducts = parcoursDs.groupBy("product_id").agg(count("product_id").as("Nb vues"))
				.orderBy(desc("Nb vues"));
		mostViewedProducts.cache();

		productsDs.groupBy("category_id").agg(count("product_id").as("Nb produits")).orderBy(desc("Nb produits"))
				.show();

		productsDs.groupBy("product_id").agg(count("category_id").as("Nb categories")).orderBy(desc("Nb categories"))
				.show();

		Dataset<Row> mostViewedCategories = productsDs.select("product_id", "category_id")
				.join(mostViewedProducts, productsDs.col("product_id").equalTo(mostViewedProducts.col("product_id")))
				.orderBy(desc("Nb vues"));
		mostViewedCategories.show();

	}
}
