import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;


public class Serilize implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int a;
	int b;
	String name;
	
	Serilize(){			
	}
	
	Serilize(int a, int b, String name){
		this.a = a;
		this.b = b;
		this.name = name;				
	}
	
	void print(){
		System.out.print(name + " " +  a + " " + b + "\n");
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException{
		Serilize se = new Serilize(1,2,"No.1");
		se.print();
		ObjectOutputStream oout = new ObjectOutputStream(new FileOutputStream("sb"));
		oout.writeObject(se);
		oout.close();
		ObjectInputStream in = new ObjectInputStream(new FileInputStream("sb"));
		Serilize se1 = (Serilize)in.readObject();
		se1.print();
	}

}
