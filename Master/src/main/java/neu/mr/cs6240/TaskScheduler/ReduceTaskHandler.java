package neu.mr.cs6240.TaskScheduler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;
import neu.mr.cs6240.StateMachine.TaskStateMachine;
import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.mapred.Job;
import neu.mr.cs6240.sharedobjects.JobState;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskType;

/**
 * @info Singleton class to handle all reduce task creation and scheduling.
 *       <ul>
 *       <li>After shuffle phase (i.e. all keys are collected) sort the keys and
 *       create Reduce Task for each key.</li>
 *       <li>Each Reduce task contain extra fields reduceTaskKey and
 *       numOfFilesWithKey. reduceTaskKey is the key associated with this
 *       reducer. numOfFilesWithKey is used for validation when slave program
 *       downloads the files and extracts files with keys.</li>
 *       <li>Submit the Reduce task for active slaves and track the tasks. Once
 *       complete assign the next task if any. Slaves will write the part files
 *       using the task_id given to them</li>
 *       <li>Once all tasks are assigned wait for TaskResults and once done
 *       write SUCCESS file to output path and send UPLOAD logs Task to all
 *       slaves, on reply send FINISHED job and close all the slaves and
 *       shutdown itself. status</li>
 *       <li>On error in any of the above steps terminate</li>
 *
 *
 * @author smitha
 *
 */
public class ReduceTaskHandler {
	private static final ReduceTaskHandler instance = new ReduceTaskHandler();

	// no class can extend it
	private ReduceTaskHandler() {
	}

	// Runtime initialization
	public static ReduceTaskHandler getInstance() {
		return instance;
	}

	final Logger logger = Logger.getLogger(ReduceTaskHandler.class);

	TaskStateMachine taskSM = TaskStateMachine.getInstance();
	SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();
	S3 s3Obj = new S3();

	/**
	 * Create Reduce tasks and Reduce task tracker<br>
	 * Begin scheduling tasks to active slaves
	 *
	 */
	public void onShuffleComplete() {
		if (slaveSM.getJobServerJobState() != JobState.SHUFFLE_RUNNING) {
			logger.error("Invalid Job State to begin Reduce phase... State Machine Corrupted.. Terminating");
			slaveSM.onError();
			return;
		}

		// create reduce tasks
		if (taskSM.getReduceTasks() == null) {
			taskSM.setReduceTasks(createReduceTasks(slaveSM.getJobServerJob()));
		} else {
			logger.error("Reduce tasks already present... State Machine Corrupted.. Terminating");
			slaveSM.onError();
			return;
		}

		taskSM.getShuffleTaskTracker().setEndTime(Calendar.getInstance().getTimeInMillis());
		logger.info(
				"Time taken for Shuffle to Complete " + taskSM.totalTimeTask(taskSM.getShuffleTaskTracker()) + " secs");

		logger.info("Begin scheduling Reduce Tasks");
		// schedule first set of reduce tasks
		taskSM.setUpReduceTaskTracker();
		taskSM.scheduleTask(taskSM.getReduceTasks(), taskSM.getReduceTaskTracker());
		slaveSM.updateJobStatusInformSubmitter(JobState.REDUCE_RUNNING);
	}

	/**
	 * Creates a list of Reduce tasks. <br>
	 * One Reduce task per key found in shuffle phase. <br>
	 * Also creates the Output folder in to store the final Output.
	 *
	 * @param job
	 *
	 * @return List of Reduce Task Objects
	 */
	public List<Task> createReduceTasks(Job job) {

		SortedSet<Object> keys = new TreeSet<Object>(taskSM.getShuffleKeys().keySet());
		logger.info("Sorted keys " + keys);

		List<Task> lstReduceTask = Collections.synchronizedList(new ArrayList<Task>());
		AtomicInteger mapTaskId = new AtomicInteger(0);

		String bucketName = s3Obj.getBucketName(job.getOutputPath());
		String opFolderName = s3Obj.getPrefixName(job.getJobName(), bucketName);

		// create Reduce Output Bucket
		s3Obj.createFolder(bucketName, opFolderName);

		taskSM.setReduceTaskOpPath(job.getOutputPath());

		// Check if it exists then delete and create new
		for (Object obj : keys) {
			Task taskObj = new Task(mapTaskId.incrementAndGet(), TaskType.REDUCE_TASK);

			taskObj.setInputPath(taskSM.getMapTaskOpPath());
			taskObj.setOutputPath(job.getOutputPath());
			taskObj.setJarByClass(job.getJarByClass());
			taskObj.setClassName(job.getReducerClass());

			taskObj.setOutputKeyClass(job.getOutputKeyClass());
			taskObj.setOutputValueClass(job.getOutputValueClass());

			taskObj.setJarPath(job.getJarPath());

			taskObj.setReduceTaskKey(obj.toString());
			taskObj.setNumOfFilesWithKey(taskSM.getShuffleKeys().get(obj));

			lstReduceTask.add(taskObj);
		}

		return lstReduceTask;
	}

	/**
	 * Used to write _SUCCESS to the output
	 *
	 * @param res
	 */
	public void writeResult(String res) {
		writeResFile(res);
		String opPath = slaveSM.getJobServerJob().getOutputPath();
		String bucketName = s3Obj.getBucketName(opPath);
		String opFolderName = s3Obj.getPrefixName(opPath, bucketName);
		File tmpSuccessFile = new File(res);
		s3Obj.uploadFile(bucketName, opFolderName + "/" + res, tmpSuccessFile);
		tmpSuccessFile.delete();
	}

	/**
	 * This method writes the file to S3
	 *
	 * @param res
	 */
	private void writeResFile(String res) {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(res)))) {
			bw.write("");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates a list of Log tasks. <br>
	 * One Reduce task is complete log tasks are sent to slaves to upload it's
	 * log<br>
	 * Also creates the Log Output folder in format Log_<jobname>_date <br>
	 *
	 * @param job
	 *
	 * @return List of Reduce Task Objects
	 */
	public List<Task> createLogTasks(Job job) {

		List<Task> lstReduceTask = Collections.synchronizedList(new ArrayList<Task>());

		// Check if it exists then delete and create new
		for (String slave : slaveSM.getSlaveIpToSlaveId().keySet()) {
			Task taskObj = new Task(slaveSM.getSlaveId(slave), TaskType.UPLOAD_LOG);
			taskObj.setOutputPath(taskSM.getLogTaskOpPath());
			lstReduceTask.add(taskObj);
		}

		return lstReduceTask;
	}

	/**
	 * Called when Reduce completes and begin tasks for uploading log
	 */
	public void onReduceComplete() {
		if (slaveSM.getJobServerJobState() != JobState.REDUCE_RUNNING) {
			logger.error("Invalid Job State to begin Log upload phase... State Machine Corrupted.. Terminating");
			slaveSM.onError();
			return;
		}

		// create log tasks
		if (taskSM.getLogTasks() == null) {
			taskSM.setLogTasks(createLogTasks(slaveSM.getJobServerJob()));
		} else {
			logger.error("Log tasks already present... State Machine Corrupted.. Terminating");
			slaveSM.onError();
			return;
		}

		logger.info("Begin Log Uploading Tasks");
		// schedule first set of log tasks
		taskSM.setUpLogTaskTracker();
		slaveSM.updateJobStatusInformSubmitter(JobState.UPLOADING_LOGS);
		taskSM.scheduleTask(taskSM.getLogTasks(), taskSM.getLogTaskTracker());

	}
}
