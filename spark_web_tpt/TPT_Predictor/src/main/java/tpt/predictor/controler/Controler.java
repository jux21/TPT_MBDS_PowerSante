package tpt.predictor.controler;

import tpt.predictor.modele.Modele;
import tpt.predictor.webservice.WebService;

public class Controler {

	/** WebService */
	private WebService webService;

	/** Modèle de données */
	private Modele modele;

	public Controler() {
		// Initialisation du modèle de données
		modele = new Modele();
		
		// Lancement du Web Service
		webService = new WebService(this);
	}
}
