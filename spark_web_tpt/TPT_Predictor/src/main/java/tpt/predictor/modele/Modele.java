package tpt.predictor.modele;

import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.MongoClientURI;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

public class Modele {

	private static final String CUSTOMERS = "customers";
	private static final String PRODUCTS = "products";
	private static final String PARCOURS = "parcours";
	private static final String VISITORS = "visitors";
	private static final String COLLECTION = "collection";
	private static final String CUSTOMERS_PATH = "mongodb://localhost/tpt_power_sante.customers";

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
		Dataset<Row> visitorsDs = MongoSpark
				.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, VISITORS), Character.class).toDF();
		visitorsDs.show();

		// Load PARCOURS
		Dataset<Row> parcoursDs = MongoSpark
				.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, PARCOURS), Character.class).toDF();
		parcoursDs.show();

		// Load PRODUCTS
		Dataset<Row> productsDs = MongoSpark
				.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, PRODUCTS), Character.class).toDF();
		productsDs.show();

		// Load CUSTOMERS
		Dataset<Row> customersDs = MongoSpark
				.load(jsc, ReadConfig.create(jsc).withOption(COLLECTION, CUSTOMERS), Character.class).toDF();
		customersDs.show();

		jsc.close();

	}

	/**
	 * Retourne une prédiction du parcours de l'utilisateur
	 * 
	 * 
	 * @param queryParams
	 */
	public void predict(Set<String> queryParams) {
		// TODO Auto-generated method stub

	}
}
