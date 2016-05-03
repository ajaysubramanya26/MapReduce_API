package neu.mr.cs6240.TaskScheduler;

import static neu.mr.cs6240.Constants.NetworkCC.MAP_TMP_OP_START_PATH;
import static neu.mr.cs6240.Constants.NetworkCC.S3_START_PATH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;
import neu.mr.cs6240.StateMachine.TaskStateMachine;
import neu.mr.cs6240.aws.FileNameSizeObj;
import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.mapred.Job;
import neu.mr.cs6240.sharedobjects.JobState;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskType;

/**
 * Receives a job. <br>
 * Splits the job into different map tasks by reading input path.<br>
 * Setup tracker to track all the jobs <br>
 * Begins executing each task by send first set of tasks in the Queue to active
 * slaves.
 *
 * @author smitha
 *
 */
public class MapTaskHandler {

	private static final MapTaskHandler instance = new MapTaskHandler();

	// no class can extend it
	private MapTaskHandler() {
	}

	// Runtime initialization
	public static MapTaskHandler getInstance() {
		return instance;
	}

	final Logger logger = Logger.getLogger(MapTaskHandler.class);
	SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();
	TaskStateMachine taskSM = TaskStateMachine.getInstance();
	S3 s3Obj = new S3();

	/**
	 * Prepare the input split when a Job is received <br>
	 * Create a list of Map tasks<br>
	 * Get the status of the slaves to check if they are still connected. <br>
	 * Assign the first set of map task to available slaves.
	 *
	 * @param job
	 * @return
	 */
	public boolean onReceiveJobMsg(Job job) {
		logger.info("Begin processing Job :" + job.toString());

		// get input split
		List<FileNameSizeObj> inputFiles = splitInput(job.getInputPath());
		if (inputFiles == null) {
			logger.error("Input split could not be created");
			return false;
		}

		// create map tasks
		if (taskSM.getMapTasks() == null) {
			taskSM.setMapTasks(createMapTask(job, inputFiles));
		} else {
			logger.error("Map tasks already present... State Machine Corrupted");
			return false;
		}

		taskSM.setUpLogFolder(job.getOutputPath(), job.getJobName());
		// schedule first set of map tasks
		taskSM.setUpMapTaskTracker();
		taskSM.scheduleTask(taskSM.getMapTasks(), taskSM.getMapTaskTracker());
		slaveSM.updateJobStatusInformSubmitter(JobState.MAP_RUNNING);
		return true;
	}

	/**
	 * Get the input files in list from s3 Currently each file is considered as
	 * a single split
	 *
	 * @throws IOException
	 */
	private List<FileNameSizeObj> splitInput(String inputPath) {

		List<FileNameSizeObj> files = null;
		if (inputPath.startsWith(S3_START_PATH)) {
			String bucketName = s3Obj.getBucketName(inputPath);
			String folderName = s3Obj.getPrefixName(inputPath, bucketName);
			logger.info("BucketName :" + bucketName + " InputFolderName :" + folderName);

			files = s3Obj.getS3FileObjects(bucketName, folderName);
			Collections.sort(files);
		}

		return files;
	}

	/**
	 * Create Map Tasks. <br>
	 * Also takes care of deleting and creating MapTemp Output folder.
	 *
	 * @param job
	 * @param inputFiles
	 * @return
	 */
	public List<Task> createMapTask(Job job, List<FileNameSizeObj> inputFiles) {
		List<Task> lstMapTask = Collections.synchronizedList(new ArrayList<Task>());
		AtomicInteger mapTaskId = new AtomicInteger(0);

		String bucketName = s3Obj.getBucketName(job.getInputPath());
		String mapOpFolderName = MAP_TMP_OP_START_PATH + job.getJobName();

		// Create a temp folder to save map outputs. Also deletes if that
		// previously exist.
		// s3Obj.deleteMultipleFiles(bucketName,
		// s3Obj.listObjsInBucket(bucketName, mapOpFolderName));
		for (String file : s3Obj.listObjsInBucket(bucketName, mapOpFolderName)) {
			s3Obj.deleteFile(bucketName, file);
		}

		String mapTempOp = S3_START_PATH + bucketName + "/" + MAP_TMP_OP_START_PATH + job.getJobName();

		taskSM.setMapTaskOpPath(mapTempOp);

		// Check if it exists then delete and create new
		for (FileNameSizeObj obj : inputFiles) {
			Task taskObj = new Task(mapTaskId.incrementAndGet(), TaskType.MAP_TASK);

			taskObj.setInputPath(job.getInputPath() + "/" + obj.getName());
			taskObj.setOutputPath(mapTempOp);
			taskObj.setJarByClass(job.getJarByClass());
			taskObj.setClassName(job.getMapperClass());

			taskObj.setOutputKeyClass(job.getMapOutputKeyClass());
			taskObj.setOutputValueClass(job.getMapOutputValueClass());

			taskObj.setJarPath(job.getJarPath());

			lstMapTask.add(taskObj);
		}

		// create Map Output Bucket
		s3Obj.createFolder(bucketName, mapOpFolderName);
		return lstMapTask;
	}
}
