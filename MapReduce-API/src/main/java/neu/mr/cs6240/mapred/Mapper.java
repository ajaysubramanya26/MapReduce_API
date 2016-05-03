package neu.mr.cs6240.mapred;
/**
 * @author Swapnil Mahajan
 * @author Prasad Memane
 */

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * This class is a custom implementation of mapper
 * @param <KeyIn>
 * @param <ValIn>
 * @param <KeyOut>
 * @param <ValOut>
 */
public class Mapper<KeyIn, ValIn, KeyOut, ValOut>{
	final Logger logger = Logger.getLogger(Mapper.class);
	//Context object for this mapper
	//private MapperContext<KeyIn, ValIn, KeyOut, ValOut> context; 
	
	/**
	 * Method to be overridden for custom implementation
	 * Called once at the beginning of the task.
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void setup(MapperContext<KeyIn, ValIn, KeyOut, ValOut> context){
		// Override if required in userImplementation
	}
	
	/**
	 * Default implementation of the map method which writes 
	 * the key and value passed to the file 
	 * @param key
	 * @param val
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	protected void map(KeyIn key, ValIn val, MapperContext<KeyIn, ValIn, KeyOut, ValOut> context){
		context.write((KeyOut)key, (ValOut)val);
	}
	
	/**
	 * this method is called once at the end of the task 
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void cleanup(MapperContext<KeyIn, ValIn, KeyOut, ValOut> context){
		// Override if required in userImplementation
	}
	
	/**
	 * create a Mapper context to be used for Map Task
	 * @param file
	 * @param taskId
	 * @return a MapperContext Object
	 * @throws IOException
	 */
	public MapperContext<KeyIn, ValIn, KeyOut, ValOut> init(String file , String taskId){
		return new MapperContext<KeyIn, ValIn, KeyOut, ValOut>(file, taskId);	
	}
	
	/**
	 * create a Mapper context to be used for Map Task
	 * with separator that can be used to write the key and value
	 * @param file
	 * @param taskId
	 * @param sep
	 * @return a MapperContext Object
	 * @throws IOException
	 */
	public MapperContext<KeyIn, ValIn, KeyOut, ValOut> init(String file , String taskId, String sep){
		return new MapperContext<KeyIn, ValIn, KeyOut, ValOut>(file, taskId, sep);
	}
	
	@SuppressWarnings("unchecked")
	/**
	 * called to execute the map task
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run(MapperContext<KeyIn, ValIn, KeyOut, ValOut> context){
		setup(context);
		while (context.getNext()){
				map((KeyIn)context.getKey(), (ValIn)context.getVal(), context);
		}
		cleanup(context);
		context.closeWriter();
	}
}
