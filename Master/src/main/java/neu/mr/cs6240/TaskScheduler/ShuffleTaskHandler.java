package neu.mr.cs6240.TaskScheduler;

import static neu.mr.cs6240.mapred.Customizations.mapOutPattern;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;
import neu.mr.cs6240.StateMachine.TaskStateMachine;
import neu.mr.cs6240.StateMachine.TaskTracker;
import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.sharedobjects.JobState;

/**
 * @author smitha
 * @info Singleton class to handle shuffle phase by the master.
 *       <ul>
 *       <li>Brief Intro to shuffle phase implementation</li>
 *       <li>Start off by inspecting all fileNames in the s3 Map Temp Output.
 *       </li>
 *       <li>Each file is of the format KEYNAME__MAPTASK<NUM>. Double underscore
 *       is need when KEY is composite in which case Key values will be
 *       separated by _. <br>
 *       It holds Data for that key from a Map task. This key file gone through
 *       the combine phase after completion of Map Phase by our Slave program.
 *       Slave program takes care of combining all the values for a key to the
 *       list and writes it to a file <br>
 *       Our main assumption here is number of unique keys will be very limited
 *       as compared to values. May not work well in practice in real world. Ex:
 *       If we are getting wordcount of huge corpus like wikipedia then we will
 *       have million of unique keys.<br>
 *       Split to get the KEYNAME. And put to ShuffleKey HashMap<KEYNAME,
 *       NUM_OF_FILES_IT_OCCURED> <br>
 *       </li>
 *       <li>Once the complete folder is read now we have a list of Keys <br>
 *       Sort the keys in ascending order <br>
 *       Begin forming Reduce tasks. Once done start scheduling reduce tasks.
 *       </li>
 *       <li>MRAppMaster does not need to download any of the files. By
 *       inspecting the metadata of the files it assigns key and num of files
 *       those keys found and our Slave program downloads all files beginning
 *       with the key name and can validate with the num of files sent by Master
 *       </li>
 *
 *
 */
public class ShuffleTaskHandler {
	private static final String MAIN_MAPRED_PATH = "neu.mr.cs6240.mapred.";

	private static final ShuffleTaskHandler instance = new ShuffleTaskHandler();

	// no class can extend it
	private ShuffleTaskHandler() {
	}

	// Runtime initialization
	public static ShuffleTaskHandler getInstance() {
		return instance;
	}

	private static final int KEY_INDEX = 0;

	final Logger logger = Logger.getLogger(ShuffleTaskHandler.class);
	TaskStateMachine taskSM = TaskStateMachine.getInstance();
	SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();
	S3 s3Obj = new S3();

	/**
	 * Start the shuffle phase
	 */
	public void beginShufflePhase() {
		if (slaveSM.getJobServerJobState() != JobState.MAP_RUNNING) {
			logger.error("Invalid Job State to begin Shuffle... State Machine Corrupted.. Terminating");
			slaveSM.onError();
			return;
		}

		taskSM.setShuffleTaskTracker(new TaskTracker());

		String mapTempOutput = taskSM.getMapTaskOpPath();
		slaveSM.updateJobStatusInformSubmitter(JobState.SHUFFLE_RUNNING);

		List<String> validFiles = getFilesMapOutput(mapTempOutput);
		if (validFiles.isEmpty()) {
			logger.error("No files to process by Shuffle phase... Terminating");
			slaveSM.onError();
			return;
		}

		if (processInputExtractKeys(validFiles)) {
			ReduceTaskHandler.getInstance().onShuffleComplete();
		}
	}

	/**
	 * Populates the taskSM shuffleKeys by reading the keys from file Name.
	 * Currently composite keys are not supported.
	 *
	 * @param validFiles
	 * @return boolean
	 */
	private boolean processInputExtractKeys(List<String> validFiles) {
		String clsName = MAIN_MAPRED_PATH + slaveSM.getJobServerJob().getMapOutputKeyClass();
		try {
			taskSM.setShuffleKeys(new HashMap<Object, Integer>());
			for (String fileName : validFiles) {
				String key = extractKeyName(fileName);

				if (key != null) {
					Object obj = extractKeyObject(key, clsName);
					if (logger.isDebugEnabled()) {
						logger.debug("Key Name Extracted " + key + "from " + fileName);
					}

					if (!taskSM.getShuffleKeys().containsKey(obj)) {
						taskSM.getShuffleKeys().put(obj, 1);
					} else {
						taskSM.getShuffleKeys().put(obj, taskSM.getShuffleKeys().get(obj) + 1);
					}
				} else {
					logger.warn("Could not extract the key for " + fileName);
				}
			}
			taskSM.printAllShuffleKeys();
		} catch (Exception e) {
			logger.error("Mapper Output Key Class Not Found in shuffle phase... " + clsName + " Terminating", e);
			slaveSM.onError();
			return false;
		}
		return true;
	}

	/**
	 * Get a primitive type object out of String Key and String constructor
	 *
	 * @param key
	 * @param type
	 * @return
	 */
	public Object extractKeyObject(String key, String className)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {

		Class<?> clazz = Class.forName(className);
		// Handles base classes
		Constructor<?> constructor = clazz.getConstructor(String.class);
		return constructor.newInstance(key);

	}

	/**
	 * Extract the Key name from the file.<br>
	 * Ex : KEY__MAPTASK<NUM>
	 *
	 * @param fileName
	 * @return
	 */
	private String extractKeyName(String fileName) {
		try {
			if (fileName.contains(mapOutPattern)) {
				return fileName.split(mapOutPattern)[KEY_INDEX];
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.error("Could not extract key for fileName " + fileName);
		}
		return null;
	}

	/**
	 * Get all files from MapOutput folder from s3
	 *
	 * @param mapTempOutput
	 * @return
	 */
	private List<String> getFilesMapOutput(String mapTempOutput) {
		List<String> validFiles = new ArrayList<>();
		String bucketName = s3Obj.getBucketName(mapTempOutput);
		String folderName = s3Obj.getPrefixName(mapTempOutput, bucketName);

		logger.info("mapTempOutput " + mapTempOutput + " bucketName " + bucketName + " folderName " + folderName);
		try {
			List<String> lstFiles = s3Obj.listObjsInBucket(bucketName, folderName);

			String validPrefixPath = folderName + "/";

			for (String file : lstFiles) {
				if (logger.isDebugEnabled()) {
					logger.debug("Checking File " + file);
				}
				if (file.startsWith(validPrefixPath) && !(file.equals(validPrefixPath))) {
					validFiles.add(StringUtils.substring(file, validPrefixPath.length()));
				}
			}
		} catch (Exception e) {
			logger.error("Could not read MapTempOutput from s3 for shuffling... Terminating", e);
			slaveSM.onError();
		}
		return validFiles;
	}

}
