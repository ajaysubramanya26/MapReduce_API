package neu.mr.cs6240.common;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 02/05/2016
 * @info Assignment 3 Class for sanity checks
 */
public class DataValidation {
	/**
	 * Validating each row from the CSV
	 *
	 * @param line
	 *            String array containing individual columns of a row
	 * @return boolean the result of the sanity test
	 */
	public static boolean isSane(String[] nextLine) {

		/**
		 * Corruption test - making sure there are as many columns in each row
		 * as there are in the header row
		 */
		if (nextLine.length != 110 || nextLine.length < 0) {
			return false;
		}

		try {
			int CRSElapsedTime, OriginAirportID, OriginAirportSeqID, OriginCityMarketID, OriginStateFips, OriginWac,
					DestAirportID, DestAirportSeqID, DestCityMarketID, DestStateFips, DestWac, Cancelled;

			/**
			 * Verifying the read data here
			 */
			String CRSArrTime = nextLine[40];
			String CRSDepTime = nextLine[29];

			String Origin = nextLine[14];
			String OriginCityName = nextLine[15];
			String OriginStateName = nextLine[18];

			String Destination = nextLine[23];
			String DestCityName = nextLine[24];
			String DestStateName = nextLine[27];

			String ArrTime = nextLine[41];
			String DepTime = nextLine[30];
			String ActualElapsedTime = nextLine[51];

			String ArrDelay = nextLine[42];
			String ArrDel15 = nextLine[44];
			String ArrDelMin = nextLine[43];
			String UniqueCarrier = nextLine[8];
			String AvgTicketPrice = nextLine[109];

			String Month = nextLine[2];
			String Year = nextLine[0];

			try {
				CRSElapsedTime = Integer.parseInt(nextLine[50]);
				OriginAirportID = Integer.parseInt(nextLine[11]);
				OriginAirportSeqID = Integer.parseInt(nextLine[12]);
				OriginCityMarketID = Integer.parseInt(nextLine[13]);
				OriginStateFips = Integer.parseInt(nextLine[17]);
				OriginWac = Integer.parseInt(nextLine[19]);
				DestAirportID = Integer.parseInt(nextLine[20]);
				DestAirportSeqID = Integer.parseInt(nextLine[21]);
				DestCityMarketID = Integer.parseInt(nextLine[22]);
				DestStateFips = Integer.parseInt(nextLine[26]);
				DestWac = Integer.parseInt(nextLine[28]);
				Cancelled = Integer.parseInt(nextLine[47]);
			} catch (NumberFormatException e) {
				throw new SanityFailedException("");
			}

			/**
			 * Sanity Test CRSArrTime && CRSDepTime should not be zero timeZone
			 * = CRSArrTime - CRSDepTime - CRSElapsedTime; timeZone % 60 should
			 * be 0 AirportID, AirportSeqID, CityMarketID, StateFips, Wac should
			 * be larger than 0 Origin, Destination, CityName, State, StateName
			 * should not be empty For flights that not Cancelled: ArrTime -
			 * DepTime - ActualElapsedTime - timeZone should be zero if ArrDelay
			 * > 0 then ArrDelay should equal to ArrDelayMinutes if ArrDelay < 0
			 * then ArrDelayMinutes should be zero if ArrDelayMinutes >= 15 then
			 * ArrDel15 should be false
			 */

			if (OriginAirportID <= 0 || DestAirportID <= 0 || OriginAirportSeqID <= 0 || DestAirportSeqID <= 0
					|| OriginCityMarketID <= 0 || DestCityMarketID <= 0 || Origin == null || Destination == null
					|| OriginStateName == null || DestStateName == null || OriginCityName == null
					|| DestCityName == null || CRSArrTime == null || CRSArrTime.isEmpty() || CRSDepTime == null
					|| CRSDepTime.isEmpty()) {
				throw new SanityFailedException("");
			}

			int dCRSArrTime = Integer.MIN_VALUE;
			int dCRSDepTime = Integer.MIN_VALUE;

			try {
				dCRSArrTime = Integer.parseInt(CRSArrTime);
				dCRSDepTime = Integer.parseInt(CRSDepTime);

			} catch (NumberFormatException e) {

				throw new SanityFailedException("");
			}

			if (dCRSArrTime == 0 || dCRSDepTime == 0) {
				throw new SanityFailedException("");
			}

			/**
			 * calculate CRSArrTime - CRSDepTime
			 */
			int totalMinutes = calcuateMins(CRSArrTime, CRSDepTime);
			int timeZone = (totalMinutes - CRSElapsedTime);

			if ((timeZone % 60) != 0) {
				throw new SanityFailedException("");
			}

			if (OriginStateFips <= 0 && DestStateFips <= 0) {
				throw new SanityFailedException("");
			}

			if (OriginWac <= 0 && DestWac <= 0) {
				throw new SanityFailedException("");
			}

			if (Cancelled == 0) {
				if (ArrTime == null || ArrTime.isEmpty() || ArrTime.length() == 0 || DepTime == null
						|| DepTime.isEmpty() || DepTime.length() == 0 || ArrDel15 == null || ArrDel15.isEmpty()
						|| ArrDelay == null || ArrDelay.isEmpty() || ActualElapsedTime == null
						|| ActualElapsedTime.isEmpty()) {
					throw new SanityFailedException("");
				} else {
					int dActualElapsedTime = Integer.parseInt(ActualElapsedTime);

					/**
					 * calculate ArrTime - DepTime
					 */
					int totalArrDepMinutes = calcuateMins(ArrTime, DepTime);

					/**
					 * ArrTime - DepTime - ActualElapsedTime -timeZone should be
					 * zero
					 */
					if ((totalArrDepMinutes - dActualElapsedTime) % 60 != 0) {
						throw new SanityFailedException("");
					}

					double dArrDelay = Double.parseDouble(ArrDelay);
					double dArrDelayMinutes = Double.parseDouble(ArrDelMin);
					double dArrDel15 = Double.parseDouble(ArrDel15);

					/**
					 * if ArrDelay > 0 then ArrDelay should equal to
					 * ArrDelayMinutes
					 */
					if (dArrDelay > 0 && dArrDelay != dArrDelayMinutes) {
						throw new SanityFailedException("");
					}

					/**
					 * if ArrDelay < 0 then ArrDelayMinutes should be zero
					 */
					if (dArrDelay < 0 && dArrDelayMinutes != 0) {
						throw new SanityFailedException("");
					}

					/**
					 * if ArrDelayMinutes >= 15 then ArrDel15 should be true
					 */
					if (dArrDelayMinutes >= 15 && dArrDel15 != 1.0) {
						throw new SanityFailedException("");
					}

					if (UniqueCarrier == null || AvgTicketPrice == null || UniqueCarrier.isEmpty()
							|| AvgTicketPrice.isEmpty() || AvgTicketPrice.equals("999999999") || Month == null
							|| Year == null || Month.isEmpty() || Year.isEmpty()) {
						throw new SanityFailedException("");
					}

				}
			}
		} catch (SanityFailedException e) {
			return false;
		}
		return true;
	}

	/**
	 * Calculate minutes elapsed for Arrival and Departure
	 * 
	 * @param ArrTime
	 * @param DepTime
	 * @return
	 */
	private static int calcuateMins(String ArrTime, String DepTime) {
		int hoursArrival = Integer.parseInt(ArrTime.substring(0, 2));
		int hoursDeparture = Integer.parseInt(DepTime.substring(0, 2));
		int minsArrival = Integer.parseInt(ArrTime.substring(2, 4));
		int minsDeparture = Integer.parseInt(DepTime.substring(2, 4));

		int diffHours = hoursArrival - hoursDeparture;
		int diffMins = minsArrival - minsDeparture;
		int totalMinutes = 0;

		/**
		 * case 1423 1215
		 */
		if (diffHours > 0) {
			totalMinutes += diffHours * 60;
		} else {
			/**
			 * 0905 2315
			 */
			totalMinutes += (diffHours + 24) * 60;
		}
		totalMinutes += diffMins;

		return totalMinutes;
	}
}
