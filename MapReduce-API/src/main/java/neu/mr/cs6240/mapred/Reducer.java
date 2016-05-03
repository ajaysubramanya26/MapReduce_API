package neu.mr.cs6240.mapred;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * @author Swapnil Mahajan
 * @author Prasad Memane
 */

public class Reducer<KeyIn, ValIn, KeyOut, ValOut> {

	final Logger logger = Logger.getLogger(Reducer.class);

	/**
	 * Called once at the beginning of the task.
	 * 
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void setup(ReducerContext<KeyIn, ValIn, KeyOut, ValOut> context) {
		// do nothing
	}

	@SuppressWarnings("unchecked")
	/**
	 * Default implementation of the reduce method which writes the key and
	 * value passed to the file
	 * 
	 * @param key
	 * @param values
	 * @param context
	 */
	protected void reduce(KeyIn key, Iterable<ValIn> values, ReducerContext<KeyIn, ValIn, KeyOut, ValOut> context) {
		for (ValIn value : values) {
			context.write((KeyOut) key, (ValOut) value);
		}
	}

	/**
	 * Called once at the end of the reducer task
	 * 
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void cleanup(ReducerContext<KeyIn, ValIn, KeyOut, ValOut> context){
		// do nothing
	}

	/**
	 * 
	 * create a Reducer context to be used for Rap Task
	 * 
	 * @param taskId
	 * @return ReducerContext object
	 */
	public ReducerContext<KeyIn, ValIn, KeyOut, ValOut> init(String taskId) {
		return new ReducerContext<KeyIn, ValIn, KeyOut, ValOut>(taskId);
	}

	/**
	 * * create a Reducer context to be used for Reduce Task with separator that
	 * can be used to write the key and value
	 * 
	 * @param taskId
	 * @param sep
	 * @return ReducerContext object
	 */
	public ReducerContext<KeyIn, ValIn, KeyOut, ValOut> init(String taskId, String sep) {
		return new ReducerContext<KeyIn, ValIn, KeyOut, ValOut>(taskId, sep);
	}

	/**
	 * called to execute the reduce task
	 * 
	 * @param ReducerContext
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void run(ReducerContext<KeyIn, ValIn, KeyOut, ValOut> context){
		setup(context);
		while (context.getNext()) {
			reduce(context.getKey(), context.getValues(), context);
		}
		cleanup(context);
		context.closeWriter();
	}
}
