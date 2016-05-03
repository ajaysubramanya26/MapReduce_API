package neu.mr.cs6240.common;

public class AirlineMonthPrice {

	/**
	 * Airline Name
	 */
	private String airline;

	/**
	 * Month
	 */
	private int month;

	/**
	 * Mean or Median price
	 */
	private double price;

	/**
	 * Constructor
	 * 
	 * @param airline
	 * @param month
	 * @param price
	 */
	public AirlineMonthPrice(String airline, int month, double price) {
		super();
		this.airline = airline;
		this.month = month;
		this.price = price;
	}

	/**
	 * getters and setters
	 */
	public String getAirline() {
		return airline;
	}

	public void setAirline(String airline) {
		this.airline = airline;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}
}
