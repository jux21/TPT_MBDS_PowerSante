package tpt.predictor.controler;

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
	}
}
