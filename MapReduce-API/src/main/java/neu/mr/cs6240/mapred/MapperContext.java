package neu.mr.cs6240.mapred;

import static neu.mr.cs6240.mapred.Customizations.*;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

/**
 * @author Swapnil Mahajan
 * @author Prasad Memane
 * @author ajay subramanya
 */
public class MapperContext<KeyIn, ValIn, KeyOut, ValOut> {
	/**
	 * Local Variables used by MapperContext
	 */
	final Logger logger = Logger.getLogger(MapperContext.class);
	private BufferedReader br;
	private String separator = keyValSep;
	private String taskId;
	private CustomLong key;
	private long lineNo = noRead;
	private CustomString val = null;
	private Map<KeyOut, List<ValOut>> mapOut;

	/**
	 * Constructor to get the MapperContext Object
	 * @param file : input file on which mapper is to be run 
	 * @param taskId : Mapper Task Id given by Slave
	 */
	public MapperContext(String file, String taskId){
		this.taskId = taskId;
		mapOut = new HashMap<>();
		FileInputStream fileInputStream;
		try {
			fileInputStream = new FileInputStream(file);
			setReader(file, fileInputStream);
		} catch (FileNotFoundException e) {
			logger.error("Unable to get the reader for input file :" + file +"\n" + e.getMessage());
		}
	}

	/**
	 * Constructor with different separator for writing
	 * @param file : input file on which mapper is to be run 
	 * @param taskId : Mapper Task Id given by Slave
	 * @param sep : user defined separator for writing output to file 
	 */
	public MapperContext(String file, String taskId, String sep){
		this(file, taskId);
		separator = sep;
	}

	/**
	 * writes the given key and value to a temporary data structure
	 * @param key
	 * @param val
	 */
	public synchronized void write(KeyOut key, ValOut val){
		List<ValOut> values = null;
		if (mapOut.containsKey(key)) {
			values = mapOut.get(key);
		} else {
			values = new ArrayList<>();
		}
		values.add(val);
		mapOut.put(key, values);
	}
	
	/**
	 * sets the reader object for the given input file
	 * @param file
	 * @param fileInputStream
	 */
	private void setReader(String file, FileInputStream fileInputStream) {
		InputStreamReader reader = null;
		if (file.endsWith(zipExt)) {
			GZIPInputStream gzipInputStream;
			try {
				gzipInputStream = new GZIPInputStream(fileInputStream);
				reader = new InputStreamReader(gzipInputStream);
			} catch (IOException e) {
				logger.error("Exception while reading the input file from local disk \n Exception :" + e.getMessage());
			}
		} else {
			reader = new InputStreamReader(fileInputStream);
		}
		br = new BufferedReader(reader);
	}

	/**
	 * 
	 * @return true if a line is read from the file, false if EOF
	 */
	public boolean getNext() {
		String line = null;
		try {
			line = br.readLine();
			// close the writer if last line is read
			if (line == null) {
				br.close();
				return false;
			}
		} catch (IOException e) {
			logger.error("Exception while reading the data in the input file" + e.getMessage());
		}

		if (key == null) {
			key = new CustomLong();
			val = new CustomString();
		}
		key.set(lineNo++);
		val.set(line);
		return true;
	}

	/**
	 * Closes the writer object for the MapperContext after the cleanup is performed
	 */
	public void closeWriter() {
		String mapOutput = mapIntPath + taskId + "/";
		for (KeyOut k : mapOut.keySet()) {
			try (OutputStream file = new FileOutputStream(mapOutput + k.toString() + mapOutPattern + taskId);
					OutputStream buffer = new BufferedOutputStream(file);
					ObjectOutput output = new ObjectOutputStream(buffer);) {
				output.writeObject(k);
				output.writeObject(mapOut.get(k));
				output.flush();
				output.close();
			} catch (IOException ioe) {
				logger.error("Exception while writing the mapper outout to disk " + ioe.getMessage());
			}
		}
	}

	/**
	 * @return separator for this MapperContext
	 */
	public String getSeparator() {
		return separator;
	}

	/**
	 * sets the separator for this MapperContext
	 * @param separator
	 */
	public void setSeparator(String separator) {
		this.separator = separator;
	}

	/**
	 * @return CustomLong key of the latest record read by reader
	 */
	public CustomLong getKey() {
		return key;
	}

	/**
	 * sets the key 
	 * @param key
	 */
	public void setKey(CustomLong key) {
		this.key = key;
	}

	/**
	 * @return CustomString value of the last record read by reader
	 */
	public CustomString getVal() {
		return val;
	}

	/**
	 * sets the CostomString val object 
	 * @param val
	 */
	public void setVal(CustomString val) {
		this.val = val;
	}

}
