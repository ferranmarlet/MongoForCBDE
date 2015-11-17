package mon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Query3Result {
	public ArrayList<Query3Container> list;
	public Query3Result(){
		list = new ArrayList<Query3Container>();
	}
	public void push(Double d, String orderKey, String orderDate, String orderShip){
		Query3Container q3 = new Query3Container();
		q3.orderDate = orderDate;
		q3.orderKey = orderKey;
		q3.orderShip = orderShip;
		q3.revenue = d;
		list.add(q3);
	}
	 public void sort(){
	        Collections.sort(list, new Comparator() {
				public int compare(Object o1, Object o2) {
					return (((Query3Container)o1).revenue > ((Query3Container)o2).revenue) ? 1 : -1;
				}
	        });
		 }
}
