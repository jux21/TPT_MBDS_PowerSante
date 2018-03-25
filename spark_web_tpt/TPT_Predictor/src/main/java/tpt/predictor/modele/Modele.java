package tpt.predictor.modele;

import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import tpt.predictor.controler.Predictor;

public class Modele {

    public final Predictor predictor = new Predictor();

    public Modele() {
        // Initialisation du mod�le de donn�es
        initModel();
    }

    /**
     * Initialisation du mod�le de donn�es
     */
    private void initModel() {
        predictor.computeModele();
    }

    /**
     * Retourne une pr�diction du parcours de l'utilisateur
     *
     * @param queryParams
     */
    public void predict(Set<String> queryParams) {
        predictor.predict("test");
    }
}
