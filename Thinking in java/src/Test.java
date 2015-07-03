class Animal{
		
	void info(){
		System.out.println("animal");
	}
		
}
	
class Pig extends Animal{
		
	void info(){
		System.out.println("pig");
	}
}

class Goat extends Animal{
		
	void info(){
		System.out.println("goat");
	}
}

class Human{
	
	void info(){
		System.out.println("human");
	}
	
	<T extends Double,K extends  Double> void sum(T t ,K k){
		System.out.println(t + k);
	}
}


public class Test {
	
	<T extends Animal> void getInfo(T t){
	//	System.out.println(t.getClass().getName());
		t.info();
	}

	public static void main(String[] args){
		
		Test test = new Test();
		Animal animal = new Animal();
		Pig pig = new Pig();
		Goat goat = new Goat();
		test.getInfo(pig);
		test.getInfo(goat);			
	}


}
