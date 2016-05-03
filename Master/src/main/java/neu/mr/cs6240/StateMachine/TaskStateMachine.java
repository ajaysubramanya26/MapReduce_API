package neu.mr.cs6240.StateMachine;

import static neu.mr.cs6240.Constants.NetworkCC.LOG_START_PATH;
import static neu.mr.cs6240.Constants.NetworkCC.S3_START_PATH;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskResult;

/**
 * Singleton class to maintain the statemachine for tasks.<br>
 * It maintains task objects, output folders, task trackers for each of the
 * task(Map, Shuffle, Reduce, Upload logs)
 *
 *
 * @author smitha
 *
 */
public class TaskStateMachine {
	private static final int TIME_1000_MS = 1000;

	private static final TaskStateMachine instance = new TaskStateMachine();

	final Logger logger = Logger.getLogger(TaskStateMachine.class);

	// no class can extend it
	private TaskStateMachine() {
		setMapTasks(null);
		setReduceTasks(null);
		mapTaskOpPath = null;
		shuffleTaskTracker = null;
	}

	// Runtime initialization
	public static TaskStateMachine getInstance() {
		return instance;
	}

	// Synchronized list of Map tasks
	private List<Task> mapTasks;

	// Synchronized map of to track Map task
	private Map<Integer, TaskTracker> mapTaskTracker;

	// Temp output path where mapper output is stored
	private String mapTaskOpPath;

	// map of Shuffle keys. Key is of type MapOutputKeyClass
	// value is number of files the Key found
	// NOTE : We need the Key type to be MapOutputKeyClass so it can sort
	private HashMap<Object, Integer> shuffleKeys;

	// Tracking time for shuffle to start and finish
	// Rest of the fields TaskObj, TaskState, slaveId not used as this is done
	// by master
	TaskTracker shuffleTaskTracker;

	// Synchronized list of Reduce tasks
	List<Task> reduceTasks;

	// Output path where reducer output is stored
	// Master will write _SUCCESS on completion
	private String reduceTaskOpPath;

	// Synchronized Map of to track Reduce task
	private Map<Integer, TaskTracker> reduceTaskTracker;

	// Synchronized list of Log tasks
	List<Task> logTasks;

	// Output path where slaves log and master output is stored
	private String logTaskOpPath;

	// Synchronized Map of to track log task
	private Map<Integer, TaskTracker> logTaskTracker;

	/**
	 * Getters and Setters
	 */
	public List<Task> getMapTasks() {
		return mapTasks;
	}

	public synchronized void setMapTasks(List<Task> mapTasks) {
		this.mapTasks = mapTasks;
	}

	public List<Task> getReduceTasks() {
		return reduceTasks;
	}

	public synchronized void setReduceTasks(List<Task> reduceTasks) {
		this.reduceTasks = reduceTasks;
	}

	public String getMapTaskOpPath() {
		return mapTaskOpPath;
	}

	public void setMapTaskOpPath(String mapTaskOpPath) {
		this.mapTaskOpPath = mapTaskOpPath;
	}

	public Map<Integer, TaskTracker> getMapTaskTracker() {
		return mapTaskTracker;
	}

	public synchronized void setMapTaskTracker(Map<Integer, TaskTracker> mapTaskTracker) {
		this.mapTaskTracker = mapTaskTracker;
	}

	public HashMap<Object, Integer> getShuffleKeys() {
		return shuffleKeys;
	}

	public void setShuffleKeys(HashMap<Object, Integer> shuffleKeys) {
		this.shuffleKeys = shuffleKeys;
	}

	public TaskTracker getShuffleTaskTracker() {
		return shuffleTaskTracker;
	}

	public synchronized void setShuffleTaskTracker(TaskTracker shuffleTaskTracker) {
		this.shuffleTaskTracker = shuffleTaskTracker;
	}

	public String getReduceTaskOpPath() {
		return reduceTaskOpPath;
	}

	public void setReduceTaskOpPath(String reduceTaskOpPath) {
		this.reduceTaskOpPath = reduceTaskOpPath;
	}

	public Map<Integer, TaskTracker> getReduceTaskTracker() {
		return reduceTaskTracker;
	}

	public synchronized void setReduceTaskTracker(Map<Integer, TaskTracker> reduceTaskTracker) {
		this.reduceTaskTracker = reduceTaskTracker;
	}

	public List<Task> getLogTasks() {
		return logTasks;
	}

	public void setLogTasks(List<Task> logTasks) {
		this.logTasks = logTasks;
	}

	public String getLogTaskOpPath() {
		return logTaskOpPath;
	}

	public void setLogTaskOpPath(String logTaskOpPath) {
		this.logTaskOpPath = logTaskOpPath;
	}

	public Map<Integer, TaskTracker> getLogTaskTracker() {
		return logTaskTracker;
	}

	public synchronized void setLogTaskTracker(Map<Integer, TaskTracker> logTaskTracker) {
		this.logTaskTracker = logTaskTracker;
	}

	/**
	 * Schedule the first set of Tasks to available slaves
	 */
	public synchronized void scheduleTask(List<Task> taskLst, Map<Integer, TaskTracker> tracker) {

		SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();

		if (slaveSM.getNumOfSlavesExpected() != slaveSM.getNumOfSlavesConnected()) {
			logger.warn("NumOfSlavesExpected " + slaveSM.getNumOfSlavesExpected() + "NumOfSlavesConnected "
					+ slaveSM.getNumOfSlavesConnected());
		}

		if (checkEmpty(taskLst, slaveSM.getSlaveIpToSlaveId())) {
			slaveSM.onError();
			return;
		}

		for (String slave : slaveSM.getSlaveIpToSlaveId().keySet()) {
			// check if slave is active
			SlaveHeartBeatInfo slaveInfo = slaveSM.getSlaveIpToSlaveId().get(slave);
			if (slaveInfo.isActive() && !taskLst.isEmpty()) {
				submitTask(slaveSM, slave, slaveInfo, taskLst, tracker);
			} else {
				logger.warn("Slave " + slaveInfo + " Inactive No tasks to submitted");
			}
		}
	}

	/**
	 * Submits the MapTask from the head of the List to slave Channel.<br>
	 * Creates an entry in mapTaskTracker
	 *
	 * @param slaveSM
	 * @param slave
	 * @param slaveInfo
	 * @param taskLst
	 * @param tracker
	 */
	private void submitTask(SlaveServerStateMachine slaveSM, String slave, SlaveHeartBeatInfo slaveInfo,
			List<Task> taskLst, Map<Integer, TaskTracker> tracker) {
		if (taskLst == null || tracker == null) {
			logger.error("Received Null for taskLst or tracker... Terminating");
			slaveSM.onError();
			return;
		}

		Task task = taskLst.get(0); // get the head of Queue
		tracker.put(task.getTaskId(), new TaskTracker(slaveInfo.getSlaveId(), task));
		logger.info("Submitting task " + task.getTaskId() + " taskType " + task.getTaskType() + " to slave"
				+ slaveInfo.getSlaveId());
		slaveSM.getSlaveChannels().get(slave).writeAndFlush(task);
		tracker.get(task.getTaskId()).setCurTaskState(TaskState.RUNNING);
		taskLst.remove(0); // remove the head after assigning
	}

	/**
	 * Submit the next task in the Queue
	 *
	 * @param slave
	 * @param taskLst
	 * @param tracker
	 */
	public synchronized void submitNextTask(String slave, List<Task> taskLst, Map<Integer, TaskTracker> tracker) {
		SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();
		SlaveHeartBeatInfo slaveInfo = slaveSM.getSlaveIpToSlaveId().get(slave);

		// check if this slave already present in SlaveSM
		if (!taskLst.isEmpty() && slaveInfo != null && slaveInfo.isActive()) {
			submitTask(slaveSM, slave, slaveInfo, taskLst, tracker);
		}
	}

	/**
	 * Update the TaskTracker entry when the TaskResult is returned for the Task
	 * from slave
	 *
	 * @param res
	 * @param tracker
	 */
	public synchronized void updateTaskTracker(TaskResult res, Map<Integer, TaskTracker> tracker) {
		tracker.get(res.getTask_id()).setEndTime(Calendar.getInstance().getTimeInMillis());
		if (res.getErrorCode() == ERR_CODE.MAP_SUCCESS || res.getErrorCode() == ERR_CODE.REDUCE_SUCCESS
				|| res.getErrorCode() == ERR_CODE.LOG_SUCCESS) {
			tracker.get(res.getTask_id()).setCurTaskState(TaskState.FINISHED);
		} else {
			tracker.get(res.getTask_id()).setCurTaskState(TaskState.ERROR);
		}
	}

	/**
	 * Checks if all tasks are submitted and TaskState is Finished
	 *
	 * @param taskLst
	 * @param tracker
	 *
	 * @return
	 */
	public synchronized boolean allTasksFinished(List<Task> taskLst, Map<Integer, TaskTracker> tracker) {
		if (taskLst.isEmpty() == true) {
			boolean res = true;
			for (Integer taskid : tracker.keySet()) {
				if (tracker.get(taskid).getCurTaskState() == TaskState.FINISHED) {
					res &= true;
				} else {
					res &= false;
				}
			}
			return res;
		}
		return false;
	}

	/**
	 * Total time in Seconds for Tasks are Submitted and TaskResult Finished is
	 * returned <br>
	 * NOTE : Need to call method allTasksFinished() before to check if all
	 * tasks finished.
	 *
	 * @param tracker
	 * @param taskLst
	 *
	 * @return Time in Seconds(Ignoring Decimal)
	 */
	public synchronized Long totalTimeTasksFinished(List<Task> taskLst, Map<Integer, TaskTracker> tracker) {
		long totalTime = 0L;
		if (taskLst.isEmpty() == true) {
			for (Integer taskid : tracker.keySet()) {
				totalTime += tracker.get(taskid).getEndTime() - tracker.get(taskid).getStartTime();
			}
		}
		return (totalTime / TIME_1000_MS);
	}

	/**
	 * Returns totalTime in Seconds for a completion NOTE : Used for jobs such
	 * as shuffle where only start and end times are maintained
	 *
	 * @param tracker
	 * @return
	 */
	public Long totalTimeTask(TaskTracker tracker) {
		return (tracker.getEndTime() - tracker.getStartTime()) / TIME_1000_MS;
	}

	/**
	 * Prints Key and Value in shuffleKeys DataStructure
	 */
	public void printAllShuffleKeys() {
		if (shuffleKeys != null) {
			logger.info("ShuffleKeys " + shuffleKeys);
		} else {
			logger.warn("shuffleKeys Null");
		}
	}

	/**
	 * Setup for tracking Map tasks
	 */
	public void setUpMapTaskTracker() {
		if (mapTaskTracker != null) {
			logger.error("Map task tracker already present");
			return;
		}
		mapTaskTracker = Collections.synchronizedMap(new HashMap<Integer, TaskTracker>());
		SlaveServerStateMachine.getInstance().checkSlavesActive();
	}

	/**
	 * Setup for tracking Reduce tasks
	 */
	public void setUpReduceTaskTracker() {
		if (reduceTaskTracker != null) {
			logger.error("Reduce task tracker already present");
			return;
		}
		reduceTaskTracker = Collections.synchronizedMap(new HashMap<Integer, TaskTracker>());
		SlaveServerStateMachine.getInstance().checkSlavesActive();
	}

	/**
	 * Setup for tracking Log tasks
	 */
	public void setUpLogTaskTracker() {
		if (logTaskTracker != null) {
			logger.error("Log task tracker already present");
			return;
		}
		logTaskTracker = Collections.synchronizedMap(new HashMap<Integer, TaskTracker>());
		SlaveServerStateMachine.getInstance().checkSlavesActive();
	}

	/**
	 * Checks if tasks and slaveInfo are empty return true else false
	 *
	 * @param tasks
	 * @param slaveInfo
	 * @return
	 */
	private boolean checkEmpty(List<Task> tasks, HashMap<String, SlaveHeartBeatInfo> slaveInfo) {
		if (slaveInfo.keySet().isEmpty()) {
			logger.error("No Slaves found to submit Tasks");
			return true;
		}

		if (tasks.isEmpty()) {
			logger.error("No tasks found to submit");
			return true;
		}
		return false;
	}

	/**
	 * Setup log folder on s3 as soon job is submitted
	 *
	 * @param jobs3OpPath
	 * @param jobName
	 */
	public void setUpLogFolder(String jobs3OpPath, String jobName) {
		S3 s3Obj = new S3();
		String bucketName = s3Obj.getBucketName(jobs3OpPath);
		Calendar cal = Calendar.getInstance();
		String dateOfLog = cal.get(Calendar.MONTH) + "_" + cal.get(Calendar.DAY_OF_MONTH) + "_" + cal.get(Calendar.HOUR)
				+ "_" + cal.get(Calendar.MINUTE);
		String logOpFolderName = LOG_START_PATH + jobName + "_" + dateOfLog;
		setLogTaskOpPath(S3_START_PATH + bucketName + "/" + logOpFolderName);
	}

}
