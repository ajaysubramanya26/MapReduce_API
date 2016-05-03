package neu.mr.cs6240;

import neu.mr.cs6240.mapred.CustomInteger;
import neu.mr.cs6240.mapred.CustomString;
import neu.mr.cs6240.mapred.Reducer;
import neu.mr.cs6240.mapred.ReducerContext;

public class WordCountReducer extends Reducer<CustomString, CustomInteger, CustomString, CustomInteger> {
	private CustomInteger result = new CustomInteger();

	@Override
	public void reduce(CustomString key, Iterable<CustomInteger> values,
			ReducerContext<CustomString, CustomInteger, CustomString, CustomInteger> context) {
		int sum = 0;
		for (CustomInteger val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
