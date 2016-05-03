package neu.mr.cs6240;

import neu.mr.cs6240.mapred.CustomInteger;
import neu.mr.cs6240.mapred.CustomLong;
import neu.mr.cs6240.mapred.CustomString;
import neu.mr.cs6240.mapred.Mapper;
import neu.mr.cs6240.mapred.MapperContext;

public class WordCountMapper extends Mapper<CustomLong, CustomString, CustomString, CustomInteger> {

	private CustomInteger result = new CustomInteger(1);

	@Override
	protected void map(CustomLong key, CustomString value,
			MapperContext<CustomLong, CustomString, CustomString, CustomInteger> context){
		String[] words = value.toString().split(" ");
		if (words.length > 1) {
			for (String w : words) {
				context.write(new CustomString(w), result);
			}

		}
	}

}
