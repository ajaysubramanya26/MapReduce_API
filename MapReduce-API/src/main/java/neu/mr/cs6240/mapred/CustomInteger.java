package neu.mr.cs6240.mapred;

import java.io.Serializable;

public class CustomInteger implements Serializable, Comparable<CustomInteger>, KeyInterface{
	private int i;
	
	/**
	 * default Constructor for getting the CustomInteger
	 */
	public CustomInteger(){}

	/**
	 * Creates a CustomInteger object with given i as an
	 * integer member of that object
	 * @param i
	 */
	public CustomInteger(int i) {this.i = i;}
	
	/**
	 * Constructor which will return CustomInteger object
	 * after parsing given string to int 
	 * @param val
	 */
	public CustomInteger(String val) throws NumberFormatException{
			this.i = Integer.valueOf(val);
	}

	/**
	 * Metod to get the int member of the CutomInteger 
	 * @return int
	 */
	public int get() {return i;}

	/**
	 * Set given i as a member of this CustomInteger
	 * @param i
	 */
	public void set(int i) {this.i = i;}
	
	@Override
	/**
	 * @return String representation of this CustomInteger object
	 */
	public String toString(){return ""+i;}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + i;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomInteger other = (CustomInteger) obj;
		if (i != other.i)
			return false;
		return true;
	}
	
	/**
	 * @param CustomInteger object to be compared
	 * returns 0 if current object holds the same int value as the int value contained in passed object
	 * returns 1 if current object holds the higher int value as the int value contained in passed object
	 * else return -1
	 */
	@Override
	public int compareTo(CustomInteger o) {
		Integer thisInt = new Integer(this.i);
		Integer oInt = new Integer(o.get());
		return thisInt.compareTo(oInt);
	}
	
	
	public void setObj(String val){
		this.i = Integer.valueOf(val);
	}
	
}
