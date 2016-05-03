package neu.mr.cs6240.utils.reducer;

import static neu.mr.cs6240.utils.constants.CONSTS.REDUCE_INIT_METHOD;
import static neu.mr.cs6240.utils.constants.CONSTS.REDUCE_RUN_METHOD;

import org.apache.log4j.Logger;

import neu.mr.cs6240.mapred.ReducerContext;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.utils.reflection.DynamicLoader;
import neu.mr.cs6240.utils.reflection.RefUtils;

/**
 * Used to run the user map program
 * 
 * @author ajay subramanya
 *
 */
public class ExecuteReduce {
	final static Logger logger = Logger.getLogger(ExecuteReduce.class);

	/**
	 * executes the user supplied reducer
	 * 
	 * @param task
	 *            the task that is sent from MRAppMaster
	 */
	public static void start(Task task) {
		String localTemp = RefUtils.setup(task);
		DynamicLoader loader = new DynamicLoader(RefUtils.jarFile(task.getJarPath()));
		loader.loadClass(task.getClassName());

		Class<?>[] initParams = RefUtils.getParams(new Class<?>[] { String.class });
		Object init = loader.invokeMethod(REDUCE_INIT_METHOD, initParams, new String[] { task.getTaskId().toString() });

		Class<?>[] runParams = RefUtils.getParams(new Class<?>[] { ReducerContext.class });
		loader.invokeMethod(REDUCE_RUN_METHOD, runParams, new Object[] { init });

		RefUtils.clean(task, localTemp);
	}
}
