package mon;

import java.util.Date;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

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
		if(!iniciat){
			System.out.println("fem inserts");
			db.createCollection("Region");
			inserts();
		}
	}
	
	public void inserts(){
		
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
