package mon;

public class Query3Container {
		 public Double revenue;
		 public String orderKey;
		 public String orderDate;
		 public String orderShip;
		 
		 public String toString(){
			return "(revenue: "+revenue+" | orderKey: "+orderKey+" | orderDate: "+orderDate + " | orderShip " + orderShip + ")";
			 
		 }
	}