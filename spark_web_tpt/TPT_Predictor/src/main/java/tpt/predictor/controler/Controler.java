package tpt.predictor.controler;

import tpt.predictor.webservice.WebService;

public class Controler {

    /**
     * WebService
     */
    private WebService webService;

    /**
     * Mod�le de donn�es
     */
    private Predictor predictor;

    public Controler() {

        // Initialisation du mod�le de donn�es
        Predictor predictor = new Predictor();

        // Lancement du Web Service
        webService = new WebService(this);

    }


    public String getPrediction(String isCustomerString, String timestampString) {


        if(isCustomerString == null){
            return "ERROR missing 'isCostumer' param";
        }

        // Si c'est un Costumer
        if(isCustomerString.equals("1")) {
            predictor.predictCostumer();
        }
        // Si c'est un Visitor
        else {
            predictor.predictVisitor();
        }

        return "Note de Barnini en Ribouchon";
    }
}
