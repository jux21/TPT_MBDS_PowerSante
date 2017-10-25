package testSparkJavaMongoDBConnector;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoConnector;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.spark.sql.helpers.StructFields;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;



public final class TestWithSpark {
	

	  /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     * @throws InterruptedException if a latch is interrupted
     */
    public static void main(final String[] args) throws InterruptedException {
	
    	 //JavaSparkContext jsc = createJavaSparkContext(args);
    	 String uriS = "mongodb://localhost/tpt_power_sante.products";
         MongoClientURI uri = new MongoClientURI(uriS);
         
         SparkConf conf = new SparkConf()
                 .setMaster("local")
                 .setAppName("MongoSparkConnectorTour")
                 .set("spark.app.id", "MongoSparkConnectorTour")
                 .set("spark.mongodb.input.uri", uriS)
                 .set("spark.mongodb.output.uri", uriS);

        JavaSparkContext jsc = new JavaSparkContext(conf);
    	System.out.println("hi");

    	// Loading and analyzing data from MongoDB
    	JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    	System.out.println(rdd.count());
    	System.out.println(rdd.first().toJson());
	
	// Loading data with a custom ReadConfig
	Map<String, String> readOverrides = new HashMap<String, String>();
	readOverrides.put("collection", "products");
	readOverrides.put("readPreference.name", "secondaryPreferred");
	ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

	JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

	System.out.println(customRdd.count());
	System.out.println(customRdd.first().toJson());
	
	// Filtering an rdd using an aggregation pipeline before passing data to Spark
	JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { category_id : '0cce5e2ff34bd08442f322139c66536d314d6c4c' } }")));
	System.out.println("\n YOLOOO \n");
	System.out.println(aggregatedRdd.count());
	System.out.println(aggregatedRdd.first().toJson());
	
	 // Load inferring schema
    Dataset<Row> df = MongoSpark.load(jsc).toDF();
    df.printSchema();
    df.show();

    // Declare the Schema via a Java Bean
    SparkSession sparkSession = SparkSession.builder().getOrCreate();
    Dataset<Row> explicitDF = MongoSpark.load(jsc).toDF(Character.class);
    explicitDF.printSchema();

    // SQL
    df.registerTempTable("characters");
    Dataset<Row> centenarians = sparkSession.sql("SELECT * FROM characters WHERE category_id = '0cce5e2ff34bd08442f322139c66536d314d6c4c'");
    centenarians.show();
    System.out.println(centenarians);
	
	jsc.close();
	
    }
    
   

	
}