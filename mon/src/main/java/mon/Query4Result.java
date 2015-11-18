package mon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Query4Result {
	public ArrayList<Query4Container> list;
	public Query4Result(){
		list = new ArrayList<Query4Container>();
	}
	public void push(Double d, String n_name){
		Query4Container q4 = new Query4Container();
		q4.revenue = d;
		q4.n_name = n_name;
		list.add(q4);
	}
	 public void sort(){
	        Collections.sort(list, new Comparator() {
				public int compare(Object o1, Object o2) {
					return (((Query4Container)o1).revenue < ((Query4Container)o2).revenue) ? 1 : -1;
				}
	        });
		 }
}
