package mon;

import static java.util.Arrays.asList;

import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
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
	
	/*
	 * Els valors a guardar de cada taula són:
	 *  lineitem: returnflag, linestatus, quantity, extendedprice
	 *  			discount, tax, shipdate, orderkey
	 *  
	 *  customers: marketsegment
	 *  
	 *  orders: orderdate, shippriority
	 *  
	 *  suppliers: acctbal, name, address, phone, comment, suppkey,
	 *  
	 *   nation: name
	 *   
	 *   region: name
	 *   
	 *   partners: partkey, mfgr, size, type,  
	 *   
	 *   partnersupplier: partkey (? potser no cal en el propi doc sino nomes en l'anidat)
	 *   				suppkey (? idem a l'anterior)
	 *   				supplycost
	 *   				
	 */
	public void inserts(){
		/*MongoCollection<Document> c = db.getCollection("Region");
		Document rEuropa = new Document("Region","Europa");
		rEuropa.append("RegionKey", "1");
		rEuropa.append("RegionName", "Europa");
		
		Document nEspanya = new Document("Nation", "Espanya");
		nEspanya.append("NationKey", "1");
		nEspanya.append("Name", "Espanya");
		
		Document Supplier1 = new Document("Supplier", "Sup1");
		Supplier1.append("SupplierKey", "1");
		Supplier1.append("Name", "Suplier u");
		Supplier1.append("Address", "C\\sup");
		Supplier1.append("Phone", "666554433");
		Supplier1.append("Comment", "Primer");
		Supplier1.append("Acctbal", "npi");
		
		Document Customer1 = new Document("Customer", "Cust1");
		Customer1.append("CustKey", "1");
		Customer1.append("MarketSegment", "MK1");
		Customer1.append("Name", "John");
		
		Document PartSupp1 = new Document("PartSupp", "PS1");
		PartSupp1.append("PSKey", "1");
		PartSupp1.append("SupplyCost", "1000");
		
		Document Part1 = new Document("Partner", "Pa1");
		PartSupp1.append("PartnerKey", "1");
		PartSupp1.append("Mfgr", "1");
		PartSupp1.append("Size", "10");
		PartSupp1.append("Type", "a");
		
		Document Order1 = new Document("Order", "Ord1");
		Order1.append("OrderKey", "1");
		Order1.append("OrderDate", "2015-11-13");
		Order1.append("Shippriority", "1");
		
		Document LineItem1 = new Document("LineItem", "LI1");
		LineItem1.append("LinteItemKey", "1");
		LineItem1.append("ReturnFlag", "F");
		LineItem1.append("LineStatus", "OK");
		LineItem1.append("Quantity", "2");
		LineItem1.append("ExtendedPrice", "23");
		LineItem1.append("Discount", "0");
		LineItem1.append("Tax", "24");
		LineItem1.append("ShipDate", "2015-11-13");
		
		Order1.append("LineItems", LineItem1);
		Customer1.append("Orders", Order1);
		
		c.insertOne(rEuropa);
		//c.insertOne("Europa");*/
		Document nouDoc = new Document("_id", "Europa")
				.append("RegionKey", "1")
				.append("RegionName", "Europa")
				.append("Nations",asList(
				new Document("NationKey",1)
					.append("Name","Catalonia")
					.append("Customers",asList(
						new Document("CustKey",1)
							.append("Name","Ferran")
							.append("MarketSegment","Jocs")
							.append("Orders",asList(
									new Document("OrderKey",1)
									.append("OrderDate","2015-11-23")
									.append("ShipPriority","3")
									.append("LineItems", asList(
											new Document("OrderKey",1)
											.append("PartKey", 1)
											.append("SupKey",1)
											.append("ReturnFlag", "F")
											.append("LineStatus", "OK")
											.append("Quantity", "2")
											.append("ExtendedPrice", "23")
											.append("Discount", "0")
											.append("Tax", "24")
											.append("ShipDate", "2015-10-13")
											,
											new Document("OrderKey",1)
											.append("PartKey", 1)
											.append("SupKey",1)
											.append("ReturnFlag", "F")
											.append("LineStatus", "OK")
											.append("Quantity", "2")
											.append("ExtendedPrice", "23")
											.append("Discount", "0")
											.append("Tax", "24")
											.append("ShipDate", "2015-09-13")
											)
											),
									new Document("OrderKey",2)
									.append("OrderDate","2015-11-23")
									.append("ShipPriority","3")
									.append("LineItems", asList(
											new Document("OrderKey",3)
											.append("PartKey", 1)
											.append("SupKey",1)
											.append("ReturnFlag", "F")
											.append("LineStatus", "OK")
											.append("Quantity", "2")
											.append("ExtendedPrice", "23")
											.append("Discount", "0")
											.append("Tax", "24")
											.append("ShipDate", "2015-11-13")
											)
											)
									)),
						new Document("CustKey",2)
							.append("MarketSegment","Aliments")
							.append("Name","Pau"),
						new Document("CustKey",3)
							.append("MarketSegment","Prova")
							.append("Name","Oscar"),
						new Document("CustKey",4)
							.append("MarketSegment","Jocs")
							.append("Name","Albert")
					)
						
				)
					.append("Suppliers", asList(
							new Document("SupplierKey", "s1")
							.append("Name", "Suplier u")
							.append("Address", "C\\sup")
							.append("Phone", "666554433")
							.append("Comment", "Primer")
							.append("Acctbal", "npi")
							.append("PartSupps", asList(
									new Document("SupplyCost", "30")
									.append("Part", new Document("PartKey","P1")
											.append("Mfgr", "wat")
											.append("size", "21")
											.append("type", "joquese"))
									,
									new Document("SupplyCost", "33")
									.append("Part", new Document("PartKey","P2")
											.append("Mfgr", "wats")
											.append("size", "15")
											.append("type", "joquese2"))
									)
									
									),
							new Document("SupplierKey", "s2")
							.append("Name", "Suplier dos")
							.append("Address", "C\\sup, 123")
							.append("Phone", "666554477")
							.append("Comment", "Segon")
							.append("Acctbal", "nose")
							
							)),
				new Document("NationKey",2)
					.append("Name","Spain")
					.append("Customers",asList(
							new Document("CustKey",5)
								.append("MarketSegment","Jocs")
								.append("Name","Ferran2"),
							new Document("CustKey",6)
							.append("MarketSegment","asdf")
								.append("Name","Pau2"),
							new Document("CustKey",7)
							.append("MarketSegment","segment")
								.append("Name","Oscar2"),
							new Document("CustKey",8)
							.append("MarketSegment","Prova")
								.append("Name","Albert2")
						)
					),
				new Document("NationKey",3)
					.append("RegionKey", 1)
					.append("Name","Portugal")
					.append("Customers",asList(
							new Document("CustKey",9)
							.append("MarketSegment","segment")
								.append("Name","Ferran"),
							new Document("CustKey",10)
							.append("MarketSegment","asdf")
								.append("Name","Pau"),
							new Document("CustKey",11)
							.append("MarketSegment","Prova")
								.append("Name","Oscar"),
							new Document("CustKey",12)
							.append("MarketSegment","Jocs")
								.append("Name","Albert")
						)
					),
				new Document("NationKey",4)
					.append("Name","Germany")
					.append("Customers",asList(
							new Document("CustKey",13)
							.append("MarketSegment","asdf")
								.append("Name","Ferran"),
							new Document("CustKey",14)
							.append("MarketSegment","Jocs")
								.append("Name","Pau"),
							new Document("CustKey",15)
							.append("MarketSegment","Prova")
								.append("Name","Oscar"),
							new Document("CustKey",16)
							.append("MarketSegment","Prova")
								.append("Name","Albert")
						)
					),
				new Document("NationKey",5)
					.append("Name","France")
					.append("Customers",asList(
							new Document("CustKey",17)
							.append("MarketSegment","asdf")
								.append("Name","Ferran"),
							new Document("CustKey",18)
							.append("MarketSegment","asdf")
								.append("Name","Pau"),
							new Document("CustKey",19)
							.append("MarketSegment","asdf")
								.append("Name","Oscar"),
							new Document("CustKey",20)
							.append("MarketSegment","Jocs")
								.append("Name","Albert")
						)
					)
				)
			); 
		
		db.getCollection("Region").drop();
		db.getCollection("Region").insertOne(nouDoc	);
		/*	
			FindIterable<Document> iterable = db.getCollection("Region").find();
			iterable.forEach(new Block<Document>() {
			    
			    public void apply(final Document document) {
			        System.out.println(document);
			    }
			});		*/
		
		System.out.println(nouDoc.toJson());
		
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
		/*l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
		o_orderdate, o_shippriority
		FROM customer, orders, lineitem
		WHERE c_mktsegment = '[SEGMENT]' AND c_custkey = o_custkey AND l_orderkey =
		o_orderkey AND o_orderdate < '[DATE1]' AND l_shipdate > '[DATE2]'
		GROUP BY l_orderkey, o_orderdate, o_shippriority
		ORDER BY revenue desc, o_orderdate;
		*/
		final Query3Result qres = new Query3Result();
		MongoCollection<Document> collection = db.getCollection("Region");	
		//System.out.println("col count: "+collection.count());
		FindIterable<Document> iterable = collection.find(new Document("RegionName", "Europa"));
		//FindIterable<Document> iterable = collection.find(new Document("Regions.Nation.Customer.MarketSegment", segment).append("Regions.Nation.Customer.Orders.OrderDate", new Document("$lt", d1)).append("Regions.Nation.Customer.Orders.LineItems", new Document("$lt", d2)));
		iterable.forEach(new Block<Document>(){

			public void apply(Document arg0) {
				//Document orders = (Document)arg0.get("Orders");
				List<Document> nacions = (List<Document>)arg0.get("Nations");
				for(Document nac : nacions){
					List<Document> customers = (List<Document>)nac.get("Customers");
					for(Document cust : customers){
						List<Document>orders = (List<Document>)cust.get("Orders");
						for(Document ord : orders){
							Double revenue = 0.;
							String orderKey = ord.getString("OrderKey");
							String orderDate = ord.getString("OrderDate");
							String orderShip = ord.getString("ShipPriority");
							List<Document>lineItems = (List<Document>)ord.get("LineItems");
							for(Document li : lineItems){
								revenue += li.getInteger("ExtendedPrice")*1-li.getDouble("Discount");
							}
							qres.push(revenue, orderKey, orderDate, orderShip);
						}
					}
				}
				qres.sort();
				System.out.println(qres.list);
				
				//System.out.println(arg0.get("Nations").toString());
				//System.out.println(arg0.toJson());
			}
			
			
		});
		String result = "";
		return result;
	}
	
	public String query4(Date d, String region){
		String result = "";
		return result;
	}
	
}
