package tpt.predictor.modele;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.mongodb.MongoClientURI;

public class Modele {

	public Modele() {
		// Initialisation du modèle de données
		initModel();
	}

	/**
	 * Initialisation du modèle de données
	 */
	private void initModel() {
		// JavaSparkContext jsc = createJavaSparkContext(args);
		String uriS = "mongodb://localhost/tpt_power_sante.products";
		MongoClientURI uri = new MongoClientURI(uriS);

		SparkConf conf = new SparkConf().setMaster("local").setAppName("MongoSparkConnectorTour")
				.set("spark.app.id", "MongoSparkConnectorTour").set("spark.mongodb.input.uri", uriS)
				.set("spark.mongodb.output.uri", uriS);

		JavaSparkContext jsc = new JavaSparkContext(conf);
		System.out.println("hi");
	}

	/**
	 * Retourne une prédiction du parcours de l'utilisateur
	 * 
	 * @param params
	 * @return
	 */
	private String predict(String params) {
		return "";

	}
}
