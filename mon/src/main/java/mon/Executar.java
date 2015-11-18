 	package mon;

import java.util.Calendar;
import java.util.Date;

public class Executar {
	public static void main(String args[]){
		try{
			MongoCBDE mongo = new MongoCBDE();
			mongo.inserts();
			//System.out.println(":)");
			Calendar cal = Calendar.getInstance();
			/*cal.set(2015, 12, 28);
			//System.out.println(cal.getTime().toString());
			Date d1 = cal.getTime();
			cal.set(2013, 12, 28);
			Date d2 = cal.getTime();
			System.out.println("Resultats query 3");
			System.out.println("-----------------------");
			mongo.query3("Jocs", d1, d2);
			cal.set(2014, 12, 28);
			Date d3 = cal.getTime();
			System.out.println("Resultats query 4");
			System.out.println("-----------------------");
			mongo.query4(d3, "Europa");*/

			//mongo.query1("2015-10-14");
			mongo.query2(21, "tipusabc", "Europa");
			System.out.println("Fin");
		}
		catch(Exception ex){
			ex.printStackTrace(System.out);
				//System.out.println("Error: "+ex.getMessage());
		}
	}
}
