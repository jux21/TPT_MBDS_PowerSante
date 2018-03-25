package tpt.predictor.controler;

import java.util.Set;

import tpt.predictor.modele.Modele;
import tpt.predictor.webservice.WebService;

public class Controler {

	/** WebService */
	private WebService webService;

	/** Mod�le de donn�es */
	private Modele modele;

	public Controler() {

		// Initialisation du mod�le de donn�es
		modele = new Modele();

		// Lancement du Web Service
		webService = new WebService(this);

		Predictor p = new Predictor();
		System.out.println(p.process());

	}

	public String getPrediction(Set<String> queryParams) {
		modele.predict(queryParams);
		
		return "Note de Barnini en Ribouchon";
	}
}
