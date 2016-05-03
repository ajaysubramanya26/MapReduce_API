package neu.mr.cs6240.mapred;

import java.io.Serializable;



public class CustomDouble implements Serializable,Comparable<CustomDouble> , KeyInterface{
	private double d;

	/**
	 * Default constructor to create a CustomDouble object
	 */
	public CustomDouble(){}
	/**
	 * Constructor to set the double element of the this 
	 * CustomDouble object
	 * @param d
	 */
	public CustomDouble(double d){this.d=d;}

	/**
	 * Constructor to set the double element of the this 
	 * CustomDouble object after parsing the given string to double
	 * @param dVal
	 * @throws NumberFormatException
	 */
	public CustomDouble(String dVal) throws NumberFormatException{
		this.d=Double.valueOf(dVal);
	}
	/**
	 * @return the double value
	 */
	public double get() {return d;}

	/**
	 * @param d the to set the double value of the class
	 */
	public void set(double d) {this.d = d;}

	@Override
	/**
	 * @return : string representation of this CustomDouble object
	 */
	public String toString() {return ""+d ;}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(d);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		CustomDouble other = (CustomDouble) obj;
		if (Double.doubleToLongBits(d) != Double.doubleToLongBits(other.d))
			return false;
		return true;
	}
	/**
	 * @param CustomeDouble object to be compared
	 * returns 0 if current object holds the same double value as the passed value
	 * returns 1 if current object holds the double vale greater than the passed object value
	 * else return -1
	 */
	@Override
	public int compareTo(CustomDouble o) {
		Double thisDouble = new Double(this.d);
		Double oDouble = new Double(o.get());
		return thisDouble.compareTo(oDouble);
	}
	
	public void setObj(String val){
		this.d = Double.valueOf(val);
	}
}
