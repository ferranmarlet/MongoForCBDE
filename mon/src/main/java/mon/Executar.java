 	package mon;

public class Executar {
	public static void main(String args[]){
		try{
			MongoCBDE mongo = new MongoCBDE();
			mongo.inserts();
			System.out.println(":)");
		}
		catch(Exception ex){
				System.out.println("Error: "+ex.getMessage());
		}
	}
}
