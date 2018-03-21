import static spark.Spark.*;
import org.apache.log4j.BasicConfigurator;

public class test_webservice {
    public static void main(String[] args) {
    	
    	// Pour log4j
    	BasicConfigurator.configure();
    	
    	// Change le port
    	port(8899);
    	
    	// Init la classe et le model
        //PredictClass pc = new PredictClass();
        //pc.initModel();
    	
    	// On �coute sur localhost:8899/predict
        get("/predict", (req, res) -> {
        	
        	System.out.println("Param�tre : " + req.queryParams());
        	System.out.println("Leurs valeurs : ");
        	req.queryParams().forEach((k) -> {System.out.println("\t" + k + ":" + req.queryParams(k));});
        	
        	String predicted = "PRODUIT1";
        	//predicted = pc.predict(params);
        	return predicted;
        });

    }
}