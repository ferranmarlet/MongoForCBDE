package mon;

import java.util.Date;
import java.util.Iterator;

import org.bson.Document;

import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import static java.util.Arrays.asList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import java.util.List;
import java.util.Set;


public class MongoCBDE {
	private MongoClient mongoClient;
	private MongoDatabase db;
	
	public MongoCBDE(){
		mongoClient = new MongoClient("localhost", 27017);
		db = mongoClient.getDatabase("TPC-H");
		MongoIterable<String> ColNoms = db.listCollectionNames();
		Boolean iniciat = false;
		for(String nom : ColNoms){
			if(nom.equals("Region")) iniciat = true;
		}
		
		db.getCollection("Region").drop();
		//if(!iniciat){
			db.createCollection("Region");
		//}

		System.out.println("fem inserts");
		inserts();
	}
	
	public void inserts(){
		db.getCollection("Region").insertOne(				
			new Document("RegionKey", "1")
				.append("RegionName", "Europa")
				.append("Comment", "ok")
				.append("Nations",asList(
				new Document("NationKey",1)
					.append("RegionKey", 1)
					.append("Name","Catalonia")
					.append("Customers",asList(
						new Document("CustKey",1)
							.append("NationKey", 1)
							.append("Name","Ferran")
							.append("Orders",asList(
									new Document("OrderKey",1)
									.append("CustKey",1)
									.append("LineItems", asList(
											new Document("OrderKey",1)
											.append("PartKey", 1)
											.append("SupKey",1)
											)),
									new Document("OrderKey",2)
									.append("CustKey",1)
									)),
						new Document("CustKey",2)
							.append("NationKey", 1)
							.append("Name","Pau"),
						new Document("CustKey",3)
							.append("NationKey", 1)
							.append("Name","Oscar"),
						new Document("CustKey",4)
							.append("NationKey", 1)
							.append("Name","Albert")
					)
				),
				new Document("NationKey",2)
					.append("RegionKey", 1)
					.append("Name","Spain")
					.append("Customers",asList(
							new Document("CustKey",5)
								.append("NationKey", 2)
								.append("Name","Ferran2"),
							new Document("CustKey",6)
								.append("NationKey", 2)
								.append("Name","Pau2"),
							new Document("CustKey",7)
								.append("NationKey", 2)
								.append("Name","Oscar2"),
							new Document("CustKey",8)
								.append("NationKey", 2)
								.append("Name","Albert2")
						)
					),
				new Document("NationKey",3)
					.append("RegionKey", 1)
					.append("Name","Portugal")
					.append("Customers",asList(
							new Document("CustKey",9)
								.append("NationKey", 3)
								.append("Name","Ferran"),
							new Document("CustKey",10)
								.append("NationKey", 3)
								.append("Name","Pau"),
							new Document("CustKey",11)
								.append("NationKey", 3)
								.append("Name","Oscar"),
							new Document("CustKey",12)
								.append("NationKey", 3)
								.append("Name","Albert")
						)
					),
				new Document("NationKey",4)
					.append("RegionKey", 1)
					.append("Name","Germany")
					.append("Customers",asList(
							new Document("CustKey",13)
								.append("NationKey", 4)
								.append("Name","Ferran"),
							new Document("CustKey",14)
								.append("NationKey", 4)
								.append("Name","Pau"),
							new Document("CustKey",15)
								.append("NationKey", 4)
								.append("Name","Oscar"),
							new Document("CustKey",16)
								.append("NationKey", 4)
								.append("Name","Albert")
						)
					),
				new Document("NationKey",5)
					.append("RegionKey", 1)
					.append("Name","France")
					.append("Customers",asList(
							new Document("CustKey",17)
								.append("NationKey", 5)
								.append("Name","Ferran"),
							new Document("CustKey",18)
								.append("NationKey", 5)
								.append("Name","Pau"),
							new Document("CustKey",19)
								.append("NationKey", 5)
								.append("Name","Oscar"),
							new Document("CustKey",20)
								.append("NationKey", 5)
								.append("Name","Albert")
						)
					)
				)
			)
		);
		
		FindIterable<Document> iterable = db.getCollection("Region").find();
		iterable.forEach(new Block<Document>() {
		    
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});		
	}
	
	public void createDB(){
		//potser no fa falta si getDatabase ja la crea
	}
	
	public String query1(Date d){
		String result = "";
		return result;
	}
	
	public String query2(Integer size, String type, String region){
		String result = "";
		return result;
	}
	
	public String query3(String segment, Date d1, Date d2){
		String result = "";
		return result;
	}
	
	public String query4(Date d, String region){
		String result = "";
		return result;
	}
}
