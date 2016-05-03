package neu.mr.cs6240.utils.reflection;

import static neu.mr.cs6240.utils.constants.CONSTS.JAR_DIR;
import static neu.mr.cs6240.utils.constants.CONSTS.MAP_INPUT_DIR;
import static neu.mr.cs6240.utils.constants.CONSTS.REDUCE_INPUT_DIR;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import neu.mr.cs6240.StateMachine.SlaveStateMachine;
import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskType;
import neu.mr.cs6240.utils.io.S3IO;

/**
 * ReflectionUtils is used to hold the utils needed by both ExecuteMap and
 * ExectueReduce to load the jar dynamically.
 * 
 * @author ajay subramanya
 *
 */
public class RefUtils {
	final static Logger logger = Logger.getLogger(RefUtils.class);
	private static SlaveStateMachine state = SlaveStateMachine.getInstance();

	/**
	 * gets the jar file from disk
	 * 
	 * @author ajay subramanya
	 * @param path
	 *            the s3 path of the input file
	 * @return the local s3 file which is download'ed from s3 and stored as
	 *         <jar_name>.jar
	 */
	public static File jarFile(String path) {
		return FileUtils.getFile(new File(JAR_DIR + StringUtils.substringAfterLast(path, "/")));
	}

	/**
	 * sets up the files on the local for the map task to do its thing
	 * 
	 * @author ajay subramanya
	 * @param task
	 *            the task object sent by the MRAppMaster
	 * @return the local temporary file location
	 * @throws IOException
	 */
	public static String setup(Task task) {
		String localTemp = task.getTaskType() + "_" + task.getTaskId();
		try {
			FileUtils.forceMkdir(new File(localTemp));
		} catch (IOException e) {
			logger.error("Exception occoured while creating local temporary directory \n" + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.EXCEPTION_CREATING_TEMP_DIR_MAP
			        : ERR_CODE.EXCEPTION_CREATING_TEMP_DIR_REDUCE);
		}
		return localTemp;
	}

	/**
	 * 
	 * @author ajay subramanya
	 * @param types
	 *            the types of params we need
	 * @return a class array of the params supplied
	 */
	public static Class<?>[] getParams(Class<?>[] types) {
		Class<?>[] prms = new Class[types.length];
		for (int i = 0; i < types.length; i++) {
			prms[i] = types[i];
		}
		return prms;
	}

	/**
	 * cleans the current directory by first uploading the data to s3 and then
	 * deleting the temporary file and also the input file
	 * 
	 * @author ajay subramanya
	 * 
	 * @param task
	 *            the task object
	 * @param localTemp
	 *            the local directory to store temporary map or reducer output
	 * @throws IOException
	 */
	public static void clean(Task task, String localTemp) {
		if (state.getErrCode() == null) S3IO.uploadDir(task.getOutputPath(), localTemp);
		try {
			FileUtils.deleteDirectory(new File(localTemp));
			FileUtils.deleteDirectory(task.getTaskType() == TaskType.MAP_TASK
			        ? new File(MAP_INPUT_DIR + task.getTaskId()) : new File(REDUCE_INPUT_DIR + task.getTaskId()));
		} catch (IOException e) {
			logger.error("Exception occoured while deleting input files and local temporary directory \n"
			        + e.getLocalizedMessage());
			state.setErrCode(state.getTsk() == TaskType.MAP_TASK ? ERR_CODE.EXCEPTION_DELETING_TEMP_DIR_MAP
			        : ERR_CODE.EXCEPTION_DELETING_TEMP_DIR_REDUCE);
		}

	}

}
