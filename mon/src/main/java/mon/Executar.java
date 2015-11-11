package mon;

public class Executar {
	public static void main(String args[]){
		try{
			MongoCBDE mongo = new MongoCBDE();
			System.out.println(":)");
		}
		catch(Exception ex){
				System.out.println("Error: "+ex.getMessage());
		}
	}
}
