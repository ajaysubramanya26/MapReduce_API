package neu.mr.cs6240.mapred;

/**
 * @author Swapnil Mahajan
 */
import java.io.Serializable;

public class CustomLong implements Serializable, Comparable<CustomLong> , KeyInterface{
	private long l;
	
	/**
	 * Default constructor for CustomLong object
	 */
	public CustomLong() {}
	
	/**
	 * takes long value to create a CustomLong object
	 * @param l
	 */
	public CustomLong(long l) {this.l=l;}	
	
	/**
	 * takes string value to create a CustomLong object
	 * @param lVal
	 * @throws NumberFormatException
	 */
	public CustomLong(String lVal) throws NumberFormatException{
		this.l=Long.valueOf(lVal);
	}

	@Override
	/**
	 * returns the string representation of the current CustomLong object
	 */
	public String toString() {return ""+ l;}

	/**
	 * get the long value of the current CustomLong object
	 * @return
	 */
	public long get() {return l;}
	
	/**
	 * set the long value of this CustomLong object
	 * @param l
	 */
	public void set(long l) {this.l = l;} 
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (l ^ (l >>> 32));
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
		CustomLong other = (CustomLong) obj;
		if (l != other.l)
			return false;
		return true;
	}
	
	/**
	 * @param CustomLong object to be compared
	 * returns 0 if current object holds the same long value as the long value contained in passed object
	 * returns 1 if current object holds the higher long value as the long value contained in passed object
	 * else return -1
	 */
	@Override
	public int compareTo(CustomLong o) {
		Long thisLong = new Long(this.l);
		Long oLong = new Long(o.get());
		return thisLong.compareTo(oLong);
	}
	
	
	public void setObj(String val){
		this.l = Long.valueOf(val);
	}
	
}
