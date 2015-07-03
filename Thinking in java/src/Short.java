import java.util.*;
public class Short {
	
	static int minpath(int[][] colist, int flag){
		int minVa = -1;
		int tag = flag;
		if (tag == 0)
			return 0;
		for(int i = colist.length - 1; i >= 0; i--){
			if(i != flag && colist[i][0] >= 0){
				int a = Math.abs(colist[flag][0] - colist[i][0]) <=
						  Math.abs(colist[flag][1] - colist[i][1])? Math.abs(colist[flag][0] - colist[i][0])
							:Math.abs(colist[flag][1] - colist[i][1]);
				if(minVa < 0 || minVa > a){
					minVa = a;
					tag = i;
				}
			}
		}
		colist[flag][0] = -1;
		return minpath(colist, tag) + minVa;
	}
	
	public static void main(String[] args){
		Scanner cin = new Scanner(System.in);
        int n = 0;
        n = cin.nextInt();
        int[][] coordinate = new int[n][2];
        for (int i = 0; i < n; ++i) {
        	coordinate[i][0] = cin.nextInt();
        	coordinate[i][1] = cin.nextInt();
        }
        System.out.println(minpath(coordinate, coordinate.length - 1));
	}      
}
