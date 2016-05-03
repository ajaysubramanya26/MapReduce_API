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

import java.util.Map;
import java.util.TreeMap;

import neu.mr.cs6240.common.FlightContants;
import neu.mr.cs6240.mapred.CustomString;
import neu.mr.cs6240.mapred.Reducer;
import neu.mr.cs6240.mapred.ReducerContext;

public class MeanPerMonthReducer extends Reducer<CustomString, CustomString, CustomString, CustomString> {

	@Override
	public void reduce(CustomString key, Iterable<CustomString> values, 
			ReducerContext<CustomString, CustomString, CustomString, CustomString> context){
		Map<Short, Long[]> prcMonth = new TreeMap<>();
		Boolean isActive = false;
		for (CustomString val : values) {
			String[] params = val.toString().split(",");
			if (params[2].equals("2015")) isActive = true;
			int price = Integer.parseInt(params[1]);
			short month = Short.parseShort(params[0]);
			if (prcMonth.containsKey(month)) {
				prcMonth.get(month)[0] += price;
				prcMonth.get(month)[1]++;
			} else {
				Long[] sumCount = new Long[2];
				sumCount[0] = (long)price;
				sumCount[1] = 1L;
				prcMonth.put(month, sumCount);
			}
		}

		if (isActive) {
			for (Short mon : prcMonth.keySet()) {
				Long[] sumCount = prcMonth.get(mon);
				double avgP = ((double) sumCount[0] / sumCount[1]) / FlightContants.CENTS;
				context.write(new CustomString(mon + " "), new CustomString(key.toString() + " " + avgP));
			}
		}
	}
}
