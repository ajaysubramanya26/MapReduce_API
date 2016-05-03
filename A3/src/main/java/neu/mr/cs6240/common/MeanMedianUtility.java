package neu.mr.cs6240.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import com.google.common.collect.Table;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 02/05/2016
 * @info Assignment 3 : Class contains methods for calculating mean ,median
 */
public class MeanMedianUtility {

	private static final int CENTS_100 = 100;

	/**
	 * calculate the mean for all airlines and the months that they were active
	 * 
	 * @param airlineMonthTable
	 *            the hash-table that contains the mean for all airlines and the
	 *            months they were active in
	 * @return
	 */
	public static ArrayList<AirlineMonthPrice> calculateMeanForAllAirlines(
			Table<String, Integer, ArrayList<Integer>> airlineMonthTable) {
		ArrayList<AirlineMonthPrice> results = new ArrayList<AirlineMonthPrice>();
		for (String flight : airlineMonthTable.rowKeySet()) {
			for (int month = 1; month <= 12; month++) {
				ArrayList<Integer> avgPLst = airlineMonthTable.get(flight, month);
				if (avgPLst != null) {
					results.add(new AirlineMonthPrice(flight, month, calculateMean(avgPLst)));
				}
			}
		}
		return results;
	}

	/**
	 * to calculate the mean of the passed array list of integers
	 * 
	 * @param avgPLst
	 *            the list from which we need to get the mean
	 * @return the mean of the list
	 */
	public static double calculateMean(ArrayList<Integer> avgPLst) {
		double sum = 0;
		for (Integer avgP : avgPLst) {
			sum += avgP;
		}
		return (sum / avgPLst.size()) / CENTS_100;
	}

	/**
	 * Calculates the median for all the airlines for a month and appends them
	 * to an array list of AirlineMonthPrice object
	 * 
	 * @param airlineMonthTable
	 *            <AirlineName, Month , listOfPrice>
	 * @return a hash-table called results which contains the median for all
	 *         airlines and their months
	 */
	public static ArrayList<AirlineMonthPrice> calculateMedianForAllAirlines(
			Table<String, Integer, ArrayList<Integer>> airlineMonthTable) {
		ArrayList<AirlineMonthPrice> results = new ArrayList<AirlineMonthPrice>();
		for (String flight : airlineMonthTable.rowKeySet()) {
			for (int month = 1; month <= 12; month++) {
				ArrayList<Integer> avgPLst = airlineMonthTable.get(flight, month);
				if (avgPLst != null) {

					results.add(new AirlineMonthPrice(flight, month, calculateMedian(avgPLst)));
				}
			}
		}
		return results;
	}

	/**
	 * Used to calculate the median of the passed in array list of Integers
	 * Median Price Calculation : (ex1) 1 , 3, 5, 6, 7, 10 count = 6 index 2, 3
	 * (ex2) 1 , 3, 6, 7, 10 count = 5 index 2
	 * 
	 * @param avgPLst
	 *            the list whose median we need to find after sorting
	 * @return the median of the passed list
	 */
	public static double calculateMedian(ArrayList<Integer> avgPLst) {
		Collections.sort(avgPLst);
		int count = avgPLst.size();
		double median = 0.0;
		if ((count % 2) == 0) {
			median = ((avgPLst.get((count / 2) - 1) + avgPLst.get(count / 2)) / 2);
		} else {
			median = (avgPLst.get((int) Math.floor(count / 2)));
		}
		return (median / CENTS_100);
	}

	/**
	 * FAST MEDIAN
	 */

	/**
	 * partitions the array into left and right side the left side of the array
	 * is always less than the randomly selected pivot and the right side of the
	 * array is always greater than the randomly selected pivot
	 *
	 * @author http://rosettacode.org/wiki/Quickselect_algorithm#Java
	 * @maintainers Smitha Bangalore && Ajay Subramanya
	 * @param arr
	 *            the array to get the median from
	 * @param left
	 *            a pointer (not the c/cpp one) which separates the unsorted
	 *            array from the sorted
	 * @param right
	 *            a pointer which keeps track of the right end of the array
	 * @param pivot
	 *            a randomly selected index in the array
	 * @return
	 */
	private static int partition(Integer[] arr, int left, int right, int pivot) {
		double pivotVal = arr[pivot];
		swap(arr, pivot, right);
		int storeIndex = left;
		for (int i = left; i < right; i++) {
			if (arr[i] < pivotVal) {
				swap(arr, i, storeIndex);
				storeIndex++;
			}
		}
		swap(arr, right, storeIndex);
		return storeIndex;
	}

	/**
	 * This function gets the median for the passed input array and position `n`
	 *
	 * @author http://rosettacode.org/wiki/Quickselect_algorithm#Java
	 * @maintainers Smitha Bangalore && Ajay Subramanya
	 * @param arr
	 *            the array to get the median from
	 * @param n
	 *            the median value
	 * @return the median
	 */
	public static double calculateFastMedian(Integer[] arr, int n) {
		int left = 0;
		int right = arr.length - 1;
		Random rand = new Random();
		while (right >= left) {
			int pivotIndex = partition(arr, left, right, rand.nextInt(right - left + 1) + left);
			if (pivotIndex == n) {
				return arr[pivotIndex];
			} else if (pivotIndex < n) {
				left = pivotIndex + 1;
			} else {
				right = pivotIndex - 1;
			}
		}
		return -1;
	}

	/**
	 * used to swap two passed integers
	 *
	 * @author http://rosettacode.org/wiki/Quickselect_algorithm#Java
	 * @maintainers Smitha Bangalore && Ajay Subramanya
	 * @param arr
	 *            the array whose elements will be swapped
	 * @param i1
	 *            the element to swap
	 * @param i2
	 *            the element to swap
	 */
	private static void swap(Integer[] arr, int i1, int i2) {
		if (i1 != i2) {
			int temp = arr[i1];
			arr[i1] = arr[i2];
			arr[i2] = temp;
		}
	}

	/**
	 * Validating if the length of the array is either even or odd. based on
	 * which we calculate the fast median
	 *
	 * @param arr
	 *            the array using which we get the fast median
	 * @return the fast median
	 */
	public static double getFastMedian(ArrayList<Integer> arr) {
		int len = arr.size();

		if (len % 2 != 0) {
			// odd
			return calculateFastMedian(arr.toArray(new Integer[len]), len / 2);
		} else {
			// even
			double med1 = calculateFastMedian(arr.toArray(new Integer[len]), len / 2);
			double med2 = calculateFastMedian(arr.toArray(new Integer[len]), (len / 2) - 1);
			return (med1 + med2) / 2;
		}
	}

}
