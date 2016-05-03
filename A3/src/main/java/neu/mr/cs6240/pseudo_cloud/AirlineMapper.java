package neu.mr.cs6240.pseudo_cloud;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 01/29/2016
 * @info Assignment 3
 * Mapper class -
 * Gets called for each line in the file read. CSVParser is used to split line.
 * Data is then check for sanity. If data is sane then AirlineName and Month is written
 * as Composite Key and then Avg Price and Year is written as MapWritable.
 * Output : (Key=(AirlineName, Month), Values((1,AvgPrice),(2,Year))
 * */
import java.io.IOException;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVParser;
import neu.mr.cs6240.common.DataValidation;
import neu.mr.cs6240.common.FlightContants;
import neu.mr.cs6240.mapred.CustomLong;
import neu.mr.cs6240.mapred.CustomString;
import neu.mr.cs6240.mapred.Mapper;
import neu.mr.cs6240.mapred.MapperContext;

public class AirlineMapper extends Mapper<CustomLong, CustomString, CustomString, CustomString> {
	CSVParser csvReader = new CSVParser(',', '"');
	final Logger logger = Logger.getLogger(AirlineMapper.class);

	@Override
	public void map(CustomLong offset, CustomString line,
			MapperContext<CustomLong, CustomString, CustomString, CustomString> context){
		String[] eachLine;
		try {
			eachLine = csvReader.parseLine(line.toString());
			if (DataValidation.isSane(eachLine)) {
				int avgInCents = (int) (Double.parseDouble(eachLine[109]) * FlightContants.CENTS);
				context.write(new CustomString(eachLine[8]),
						new CustomString(eachLine[2] + "," + avgInCents + "," + eachLine[0]));
			}
		} catch (IOException e) {
			logger.error("Exception in Parsing the csv record" + e);
		}
		
	}
}
