package neu.mr.cs6240.utils.job;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.mapred.Job;

public class JobValidation {

	private static String log4jConfPath = "./MapReduce-API/log4j.properties";
	private final static Logger logger = Logger.getLogger(JobValidation.class);
	private static Job job;

	/**
	 * @author ajay subramanya
	 * @param jobObj
	 *            the job which needs to be sent to the MRAppMaster, but needs
	 *            validation before sending
	 * @return true if the fields in the job pass the validations , false
	 *         otherwise
	 */
	public static boolean valid(Job jobObj) {
		PropertyConfigurator.configure(log4jConfPath);
		job = jobObj;
		if (!isAllSet()) return false;
		if (!validPath(job.getInputPath())) return false;
		if (!validPath(job.getJarPath())) return false;
		if (isOutputPathPresent()) return false;
		return true;
	}

	/**
	 * checks if all the required parameters that are needed to run the job are
	 * submitted by the user
	 *
	 * @author ajay subramanya
	 * @return true if all the parameters are set, false otherwise
	 */
	public static boolean isAllSet() {
		if (!StringUtils.isNotEmpty(job.getInputPath())) {
			logger.error("input path is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getOutputPath())) {
			logger.error("output path is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getJarByClass())) {
			logger.error("jar by class is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getJarPath())) {
			logger.error("s3 jar path is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getMapOutputKeyClass())) {
			logger.error("map output key class is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getMapOutputValueClass())) {
			logger.error("map output value class is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getMapperClass())) {
			logger.error("mapper class is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getReducerClass())) {
			logger.error("reducer class is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getOutputKeyClass())) {
			logger.error("output key class is not set");
			return false;
		}
		if (!StringUtils.isNotEmpty(job.getOutputValueClass())) {
			logger.error("output value class is not set");
			return false;
		}

		return true;
	}

	/**
	 * @author ajay subramanya
	 * @return true if the file is not present, false otherwise. Mainly used to
	 *         check if the use specified output file exists
	 */
	public static boolean isOutputPathPresent() {
		S3 s3 = new S3();
		List<String> split = s3.splitS3Path(job.getOutputPath());
		List<String> objs = s3.listObjsInBucket(split.get(0), null);
		for (String s : objs) {
			if (s.startsWith(split.get(1))) {
				logger.error("output path " + job.getOutputPath() + " seems to exist in your bucket, please delete");
				return true;
			}
		}
		return false;
	}

	/**
	 * @author ajay subramanya
	 * @return true if the input path is valid and exists on s3, false otherwise
	 */
	public static boolean validPath(String path) {
		S3 s3 = new S3();
		List<String> split = s3.splitS3Path(path);
		if (split.size() != 2) {
			logger.error("[invalid path : " + path + " ] s3 path must be of the format <s3://bucket_name/key_name>");
			return false;
		}

		if (!s3.isValidFile(split.get(0), getKey(split.get(1)))) {
			logger.error("path : " + path + " does not exist on s3, please check your bucket ");
			return false;
		}
		return true;
	}

	/**
	 * @author ajay subramanya
	 * @param key
	 *            the key which may or may not end with a '/'
	 * @return if key ends with a '/' it is return as is else is appended with a
	 *         '/'
	 */
	public static String getKey(String key) {
		return StringUtils.endsWith(key, "/") || StringUtils.endsWith(key, ".jar") ? key : key + "/";
	}
}
