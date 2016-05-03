package neu.mr.cs6240.pseudo_cloud;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 01/29/2016
 * @info Assignment 3
 * Reducer class -
 * Gets called for each Airline and data is arranged by Month using partitioner and
 * group sorting. Values are checked if flight is active for 2015 and average for
 * each month is calculated and flight frequency is calculated.
 * Output : AirlineName, (month, avg price)..., frequency
 * */


import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import neu.mr.cs6240.common.MeanMedianUtility;
import neu.mr.cs6240.mapred.CustomString;
import neu.mr.cs6240.mapred.Reducer;
import neu.mr.cs6240.mapred.ReducerContext;

public class MedianPerMonthReducer extends Reducer<CustomString, CustomString, CustomString, CustomString> {

	@Override
	public void reduce(CustomString key, Iterable<CustomString> values, 
			ReducerContext<CustomString, CustomString, CustomString, CustomString> context){
		Map<Short, ArrayList<Integer>> prcMonth = new TreeMap<>();
		Boolean isActive = false;
		int price = 0;
		short month = 0;
		for (CustomString val : values) {
			String[] params = val.toString().split(",");
			if (params[2].equals("2015")) isActive = true;
			price = Integer.parseInt(params[1]);
			month = Short.parseShort(params[0]);
			if (prcMonth.containsKey(month)) {
				prcMonth.get(month).add(price);

			} else {
				ArrayList<Integer> prices = new ArrayList<>();
				prices.add(price);
				prcMonth.put(month, prices);
			}
		}
		if (isActive) {
			for (Short mon : prcMonth.keySet()) {
				double avgP = MeanMedianUtility.calculateMedian(prcMonth.get(mon));
				context.write(new CustomString(mon + " "), new CustomString(key.toString() + " " + avgP));
			}
		}
	}
}
