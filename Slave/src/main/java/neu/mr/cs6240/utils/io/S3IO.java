package neu.mr.cs6240.utils.io;

import static neu.mr.cs6240.utils.constants.CONSTS.JAR_DIR;
import static neu.mr.cs6240.utils.constants.CONSTS.MAP_INPUT_DIR;
import static neu.mr.cs6240.utils.constants.CONSTS.REDUCE_INPUT_DIR;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskType;;

/**
 * class to hold methods that read and write to S3s
 *
 * @author ajay subramanya
 *
 */
public class S3IO {

	private static S3 s3 = new S3();
	final static Logger logger = Logger.getLogger(S3IO.class);

	public static void downloadFiles(final Task task) {
		// Start downloading jar
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					getJarFile(task.getJarPath());
				} catch (Exception e) {
					logger.error("Exception while downloading jar " + e.getMessage());
				}
			}
		});
		t1.start();

		// Start downloading input files
		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					if (task.getTaskType() == TaskType.MAP_TASK) getMapInput(task);
					if (task.getTaskType() == TaskType.REDUCE_TASK) getReduceInput(task);
				} catch (Exception e) {
					logger.error("Exception while downloading input files" + e.getMessage());
				}
			}
		});
		t2.start();

		try {
			t1.join();
			t2.join();
		} catch (InterruptedException e1) {
			logger.error("Exception in download thread " + e1.getLocalizedMessage());
		}
	}

	/**
	 * used to get the files from s3 and place in local
	 *
	 * @author ajay subramanya
	 * @param inputPath
	 *            the s3 path where the input files are
	 */
	public static void getMapInput(Task task) {
		String mapIpDir = MAP_INPUT_DIR + task.getTaskId();
		mkdir(mapIpDir);
		List<String> input = s3.splitS3Path(task.getInputPath());
		s3.readS3CopyLocal(input.get(0), input.get(1), mapIpDir + "/" + inputFile(task));
	}

	/**
	 * 
	 * @param task
	 * @return the input file for the task
	 */
	private static String inputFile(Task task) {
		return StringUtils.substringAfterLast(task.getInputPath(), "/");
	}

	/**
	 * used to get the files from s3 and place in local
	 *
	 * @author ajay subramanya
	 * @param inputPath
	 *            the s3 path where the input files are
	 */
	public static void getReduceInput(Task task) {
		logger.info("getting input files for key :  " + task.getReduceTaskKey());
		List<String> input = s3.splitS3Path(task.getInputPath());
		List<String> keys = s3.listObjsInBucket(input.get(0), input.get(1));
		mkdir(REDUCE_INPUT_DIR + task.getTaskId());
		for (String key : keys) {
			if (contains(task.getReduceTaskKey(), key)) {
				s3.readS3CopyLocal(input.get(0), key, getLocalDir(task.getTaskId(), key));
			}
		}
	}

	/**
	 * 
	 * @param taskID
	 *            the id of the task assigned by the master
	 * @param key
	 *            the s3 key
	 * @return the local directory in which the input files for either mapper or
	 *         reducer phase would be stored
	 */
	private static String getLocalDir(Integer taskID, String key) {
		return REDUCE_INPUT_DIR + taskID + "/" + StringUtils.substringAfterLast(key, "/");
	}

	/**
	 * checks if the key starts with the task key
	 * 
	 * @param taskKey
	 * @param key
	 * @return
	 */
	private static boolean contains(String taskKey, String key) {
		return StringUtils.startsWith(StringUtils.substringAfterLast(key, "/"), taskKey + "__");
	}

	/**
	 * used to download the jar from s3 on to local
	 *
	 * @author ajay subramanya
	 * @param jarPath
	 *            the s3 path of the jar
	 */
	public static void getJarFile(String jarPath) {
		if (exists(JAR_DIR, jarPath)) return;
		if (logger.isDebugEnabled()) logger.debug("copying jar from  " + jarPath + " to local");
		List<String> jar = s3.splitS3Path(jarPath);
		mkdir(JAR_DIR);
		s3.readS3CopyLocal(jar.get(0), jar.get(1), JAR_DIR + StringUtils.substringAfterLast(jarPath, "/"));
	}

	/**
	 * checks if a file exists or not
	 *
	 * @param path
	 *            the file to check
	 * @return true if file exists , false otherwise
	 */
	private static boolean exists(String dir, String path) {
		return new File(dir + StringUtils.substringAfterLast(path, "/")).exists();
	}

	/**
	 * to create a new directory
	 *
	 * @author ajay subramanya
	 * @param path
	 */
	private static void mkdir(String path) {
		try {
			FileUtils.forceMkdir(new File(path));
		} catch (IOException e) {
			logger.error("Exception while creating directory for input files" + e.getMessage());
		}
	}

	/**
	 * used to write results to temporary location on s3
	 *
	 * @author ajay subramanya
	 * @param s3Temp
	 *            the temporary location on s3 where we write our intermediate
	 *            results
	 * @param localTemp
	 *            the temporary directory on local
	 */
	public static void uploadDir(String s3Temp, String localTemp) {
		logger.info("writting temp dir " + localTemp + "  to s3 " + s3Temp);
		// List<String> temp = s3.splitS3Path(s3Temp);
		String bucketName = s3.getBucketName(s3Temp);
		String prefix = s3.getPrefixName(s3Temp, bucketName);
		s3.uploadDirectory(bucketName, prefix, new File(localTemp));
	}
}
