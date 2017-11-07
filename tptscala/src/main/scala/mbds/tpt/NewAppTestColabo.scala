package mbds.tpt

import com.mongodb.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author ${user.name}
  */
object NewAppTestColabo {

  def main(args: Array[String]) {

    var NOTRE_VISITOR = "'5539fc7691e02ac7152e43230632c3f581588703'";

    /*
      Algo collaboratif :

        Pour un user Albert qui a acheté un produit A et B:
        Test avec {"visitor_id":"5539fc7691e02ac7152e43230632c3f581588703"} // parcours

         1) on récup les objets  de l'utilisateur en question (2 ou 3 max)

         2) pour ces objets on récupère les id de tous les users

              A           B
            XXXXX       JJJJJ
            VVVVV       LLLLL
            IIIII       VVVVV

          3) on garde l'id utilisateur qui est la le plus de fois

          4) on prend la différence de produits pour proposer

     */

    val databaseName = "tpt_power_sante"
    var collectionName = "parcours"
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TPT")
      .config("spark.mongodb.input.uri", "mongodb://localhost/" + databaseName + "." + collectionName)
      .config("spark.mongodb.output.uri", "mongodb://localhost/" + databaseName + "." + collectionName)
      .getOrCreate()

    val sc = SparkContext.getOrCreate();
    sc.setLogLevel("WARN")

    val dataframeParcours = MongoSpark.load(spark)
    dataframeParcours.createOrReplaceTempView("parcours")
    dataframeParcours.printSchema()


    // 1) On récupére les objets de l'utilisateur albert
    var objetsUtilisateur = dataframeParcours.select("product_id").where("visitor_id= " + NOTRE_VISITOR).distinct().limit(3)
    objetsUtilisateur.show(false)
    objetsUtilisateur

    // ?? Utiliser un foreach sur le dataset pour avec une requete dedans ??
    // 2) pour ces objets on récupère les id de tous les users

    dataframeParcours.show(false)
  }

}
