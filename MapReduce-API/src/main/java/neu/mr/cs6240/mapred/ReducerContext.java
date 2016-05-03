package neu.mr.cs6240.mapred;

import static neu.mr.cs6240.mapred.Customizations.encoding;
import static neu.mr.cs6240.mapred.Customizations.keyValSep;
import static neu.mr.cs6240.mapred.Customizations.noWrite;
import static neu.mr.cs6240.mapred.Customizations.padChar;
import static neu.mr.cs6240.mapred.Customizations.redInpPath;
import static neu.mr.cs6240.mapred.Customizations.redOutPath;
import static neu.mr.cs6240.mapred.Customizations.redOutPattern;

/**
 * @author Swapnil Mahajan
 * @author Prasad Memane
 */
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Strings;

public class ReducerContext<KeyIn, ValIn, KeyOut, ValOut> {
	final Logger logger = Logger.getLogger(ReducerContext.class);
	private BufferedWriter writer;
	private String taskId;
	private String separator = keyValSep;
	private KeyIn key;
	private Iterable<ValIn> values = null;
	private List<File> allFiles;

	/**
	 * constructor for the ReducerContext
	 * 
	 * @param taskId
	 */
	public ReducerContext(String taskId) {
		this.taskId = taskId;
		setReader();
		setWriter();
	}

	/**
	 * Constructor with different separator for writing
	 * 
	 * @param taskId
	 * @param sep
	 */
	public ReducerContext(String taskId, String sep) {
		this(taskId);
		separator = sep;
	}

	/**
	 * This method get the list of all files to be read from the files
	 * downloaded from s3
	 */
	protected void setReader() {
		String filesLoc = redInpPath + taskId + "/";
		if (!filesLoc.isEmpty() && filesLoc != null) {
			File ReadFromDIR = new File(filesLoc);
			allFiles = new ArrayList<File>(Arrays.asList(ReadFromDIR.listFiles()));
			for (File file : allFiles)
				logger.info("File name present: " + file.getName());
		}
	}

	/**
	 * This method sets the writer object that will be used for writing the
	 * output from this ReducerContext
	 */
	protected void setWriter() {
		// Create pattern of the output file part-r-0000X
		String pat = redOutPattern + Strings.padStart(taskId, 5, padChar);
		try {
			FileOutputStream outFile = new FileOutputStream(redOutPath + taskId + "/" + pat);
			writer = new BufferedWriter(new OutputStreamWriter(outFile, encoding));
		} catch (IOException e) {
			logger.error("Error while setting writer for ReducerContext" + e.getMessage());
		}
	}

	/**
	 * writes the given key and value to the output file in String format
	 * separated by default separator or a separator defined by user while
	 * setting the Reducer object
	 * 
	 * @param key
	 * @param val
	 */
	public synchronized void write(KeyOut key, ValOut val) {
		try {
			writer.write(key.toString() + separator + val.toString());
			writer.newLine();
		} catch (IOException e) {
			logger.error("Exception while writing in ReduceContext " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	/**
	 * this method reads all the file related to a specific key and sets the
	 * iterable of the values
	 * 
	 * @return true if values are set else false
	 */
	public boolean getNext() {
		if (values != null) return false;
		// All files are exhausted for reading
		List<ValIn> tempVal = new ArrayList<>();
		for (File f : allFiles) {
			ObjectInputStream reader;
			InputStream fileInputStream;
			try {
				fileInputStream = new FileInputStream(f);
				InputStream buffer = new BufferedInputStream(fileInputStream);
				reader = new ObjectInputStream(buffer);

				try {
					key = (KeyIn) reader.readObject();
					if (key == null) continue;
					tempVal.addAll((List<ValIn>) reader.readObject());
				}
				catch (ClassNotFoundException e) {
					logger.error("Class not found for key or value"+ e);
				}			
				reader.close();
			}catch (FileNotFoundException e) {
				logger.error("Exception opening the file :" + f +"\nEXCEPTION: "+ e);
			}catch(IOException e){
				logger.error("Exception reading the data from File: "+f+"\nEXCEPTION:"+ e);
			} 
		}
		values = tempVal;
		return true;
	}

	/**
	 * Close the writer object after the reducer finishes
	 */
	public void closeWriter() {
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			logger.error("Exception closing the writer in ReducerContext" + e.getMessage());
		}

	}

	/**
	 * 
	 * @return current key of the ReducerContext Object
	 */
	public KeyIn getKey() {
		return key;
	}

	/**
	 * Sets current key of the ReducerContext
	 * @param key
	 */
	public void setKey(KeyIn key) {
		this.key = key;
	}

	/**
	 * 
	 * @return current iterable over all the values 
	 */
	public Iterable<ValIn> getValues() {
		return values;
	}

	/**
	 * Sets the iterable of the ReducerContext
	 * @param values
	 */
	public void setValues(Iterable<ValIn> values) {
		this.values = values;
	}
}
