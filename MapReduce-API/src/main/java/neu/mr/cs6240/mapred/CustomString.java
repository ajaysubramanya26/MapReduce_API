package neu.mr.cs6240.mapred;

import java.io.Serializable;

public class CustomString implements Serializable, Comparable<CustomString>, KeyInterface{
	private String text;
	
	/**
	 * default constructor
	 */
	public CustomString() {}
	
	/**
	 * Constructor that takes a string to be set 
	 * @param text
	 */
	public CustomString(String text) {this.text = text;}

	/**
	 * method to retrieve the string  
	 * @return a string set in this object
	 */
	public String get(){return text;}

	/**
	 * sets the String text member
	 * @param text
	 */
	public void set(String text) {this.text = text;}

	@Override
	/**
	 * @return: returns the string representation
	 */
	public String toString() {
		return text;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((text == null) ? 0 : text.hashCode());
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
		CustomString other = (CustomString) obj;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}
	
	/**
	 * @param CustomString object to be compared
	 * returns 0 if current object holds the same string value as the value contained in passed object
	 * returns 1 if current object holds the String vale greater than the value contained in passed object
	 * else return -1
	 */
	@Override
	public int compareTo(CustomString o) {
		return this.text.compareTo(o.get());
	}
	
	
	public void setObj(String val){
		this.text = val;
	}
	
}
