//Source : https://avaldes.com/java-connecting-to-mongodb-3-2-examples/

package testSparkJavaMongoDBConnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class test {

  public static void main(String[] args) {
    mongoOldMethods();
  }
  
  public static void mongoOldMethods() {
    MongoClient mongoClient = null;
    
    try {
      System.out.println("Connecting using mongoOldMethods()...");
      mongoClient = new MongoClient();
      
      // Old way to get database - deprecated now
      DB db = mongoClient.getDB("test");

      DBCollection collection = db.getCollection("restaurants");
      System.out.println("collection: " + collection);
      
      System.out.println("yolo");
      
      DBCursor cursor = (DBCursor) collection.find().iterator();
      try {
        while (cursor.hasNext()) {
          System.out.println(cursor.next().toString());
        }
      } finally {
        cursor.close();
      }
      
      

      System.out.println("yolo");

    } catch (Exception e) {
      e.printStackTrace();
    } finally{
      mongoClient.close();
    }
  }
  
  
}