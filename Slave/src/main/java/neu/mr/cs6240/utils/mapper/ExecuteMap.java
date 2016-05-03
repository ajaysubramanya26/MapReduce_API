package neu.mr.cs6240.utils.mapper;

import static neu.mr.cs6240.utils.constants.CONSTS.MAP_INIT_METHOD;
import static neu.mr.cs6240.utils.constants.CONSTS.MAP_INPUT_DIR;
import static neu.mr.cs6240.utils.constants.CONSTS.MAP_RUN_METHOD;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import neu.mr.cs6240.mapred.MapperContext;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.utils.reflection.DynamicLoader;
import neu.mr.cs6240.utils.reflection.RefUtils;

/**
 * Use to run the user map program
 * 
 * @author ajay subramanya
 *
 */
public class ExecuteMap {
	final static Logger logger = Logger.getLogger(ExecuteMap.class);

	/**
	 * uses java reflection and loads the jar class dynamically and invokes the
	 * map method. the map method takes the input sent by master and executes
	 * the user supplied map method and finally writes the data to s3
	 * 
	 * @author ajay subramanya
	 * 
	 * @throws IOException
	 * 
	 */
	public static void start(Task task) {
		String localTemp = RefUtils.setup(task);

		DynamicLoader loader = new DynamicLoader(RefUtils.jarFile(task.getJarPath()));
		loader.loadClass(task.getClassName());

		Class<?>[] initParams = RefUtils.getParams(new Class<?>[] { String.class, String.class });
		Object init = loader.invokeMethod(MAP_INIT_METHOD, initParams, initArgs(task));

		Class<?>[] runParams = RefUtils.getParams(new Class<?>[] { MapperContext.class });
		loader.invokeMethod(MAP_RUN_METHOD, runParams, new Object[] { init });

		RefUtils.clean(task, localTemp);
	}

	/**
	 * 
	 * @author ajay subramanya
	 * @param task
	 *            the task object sent from the MRAppMaster
	 * @return the args needed to invoke the init method of Mapper
	 */
	private static String[] initArgs(Task task) {
		return new String[] {
		        MAP_INPUT_DIR + task.getTaskId() + "/" + StringUtils.substringAfterLast(task.getInputPath(), "/"),
		        task.getTaskId().toString() };
	}

}
