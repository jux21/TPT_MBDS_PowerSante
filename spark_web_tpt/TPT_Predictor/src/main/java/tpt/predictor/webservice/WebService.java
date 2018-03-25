package tpt.predictor.webservice;

import static spark.Spark.get;
import static spark.Spark.port;

import org.apache.log4j.BasicConfigurator;

import tpt.predictor.controler.Controler;

public class WebService {

	/** Port utilis� par le WebService */
	private static final int PORT = 8899;

	/** Controleur */
	private Controler controler;

	public WebService(Controler controler) {
		// Lien vers le controleur
		this.controler = controler;

		// Lancement du WebService
		lancementWebService();
	}

	private void lancementWebService() {
		// Pour log4j
		BasicConfigurator.configure();

		// Change le port
		port(PORT);

		// On �coute sur localhost:8899/predict
		get("/predict", (req, res) -> {

			System.out.println("Param�tre : " + req.queryParams());
			System.out.println("Leurs valeurs : ");
			req.queryParams().forEach((k) -> {
				System.out.println("\t" + k + ":" + req.queryParams(k));
			});

			String predicted = controler.getPrediction(req.queryParams());

			return predicted;
		});
	}
}
