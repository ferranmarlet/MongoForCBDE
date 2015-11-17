 	package mon;

import java.util.Calendar;
import java.util.Date;

public class Executar {
	public static void main(String args[]){
		try{
			MongoCBDE mongo = new MongoCBDE();
			mongo.inserts();
			System.out.println(":)");
			Calendar cal = Calendar.getInstance();
			cal.set(2015, 12, 28);
			Date d1 = cal.getTime();
			cal.set(2013, 12, 28);
			Date d2 = cal.getTime();
			mongo.query3("Jocs", d1, d2);
			System.out.println("Fin");
		}
		catch(Exception ex){
			ex.printStackTrace(System.out);
				//System.out.println("Error: "+ex.getMessage());
		}
	}
}
