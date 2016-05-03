package neu.mr.cs6240.TaskScheduler;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import io.netty.channel.Channel;
import neu.mr.cs6240.StateMachine.SlaveServerStateMachine;
import neu.mr.cs6240.StateMachine.TaskStateMachine;
import neu.mr.cs6240.StateMachine.TaskTracker;
import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.JobState;
import neu.mr.cs6240.sharedobjects.Task;
import neu.mr.cs6240.sharedobjects.TaskResult;
import neu.mr.cs6240.sharedobjects.TaskType;

/**
 * Singleton class to handle all the TaskResults and update the task
 * statemachine<br>
 * On completion of all tasks in a that group it moves to next state in job
 * statemachine.
 *
 * @author smitha
 *
 */
public class TaskResultHandler {

	private static final TaskResultHandler instance = new TaskResultHandler();

	// no class can extend it
	private TaskResultHandler() {
	}

	// Runtime initialization
	public static TaskResultHandler getInstance() {
		return instance;
	}

	final Logger logger = Logger.getLogger(TaskResultHandler.class);

	SlaveServerStateMachine slaveSM = SlaveServerStateMachine.getInstance();
	TaskStateMachine taskSM = TaskStateMachine.getInstance();

	/**
	 * Handles the TaskResult from the slave and updates it task state machine
	 * and submits next job if any or moves to next state in the job state
	 * machine
	 *
	 * @param ch
	 * @param res
	 */
	public void onReceiveTaskResult(Channel ch, TaskResult res) {

		slaveSM.updateActiveStatusOnWrite(ch.remoteAddress().toString());
		// checks the type of TaskResult and verify with the current JobState
		// Machine
		if (res.getTaskType() == TaskType.MAP_TASK && slaveSM.getJobServerJobState() == JobState.MAP_RUNNING) {
			boolean isMapTaskComplete = handleTaskResult(ch, res, taskSM.getMapTasks(), taskSM.getMapTaskTracker(),
					"Map");

			/**
			 * Start shuffle phase <br>
			 * If there is no pending task begins shuffle and sort phase by
			 * examining the Map Temp Output path to obtain keys. When last
			 * MapTask Completes and result is returned begins with actual
			 * shuffle phase <br>
			 */
			if (isMapTaskComplete) {
				logger.info("Begin Shuffle phase");
				ShuffleTaskHandler.getInstance().beginShufflePhase();
			}
		} else if (res.getTaskType() == TaskType.REDUCE_TASK
				&& slaveSM.getJobServerJobState() == JobState.REDUCE_RUNNING) {
			boolean isReduceTaskComplete = handleTaskResult(ch, res, taskSM.getReduceTasks(),
					taskSM.getReduceTaskTracker(), "Reduce");
			// Write _SUCESS and Start log uploading phase
			if (isReduceTaskComplete) {
				logger.info("End Reduce phase");
				ReduceTaskHandler.getInstance().writeResult("_SUCCESS");
				// Send UPLOAD_LOG command to slave
				ReduceTaskHandler.getInstance().onReduceComplete();
			}
		} else if (res.getTaskType() == TaskType.UPLOAD_LOG
				&& slaveSM.getJobServerJobState() == JobState.UPLOADING_LOGS) {
			boolean isLogTaskComplete = handleTaskResult(ch, res, taskSM.getLogTasks(), taskSM.getLogTaskTracker(),
					"Log");
			// upload master log the master log and then call onSuccess which
			// sends finished status to job submitter
			if (isLogTaskComplete) {
				slaveSM.onSuccess();
			}

		}

	}

	/**
	 *
	 * Handles the TaskResult for a Task. <br>
	 * Updates the TaskSM with the endTime and Finished or Error status <br>
	 * On Error program informs the job submitter and will terminate. <br>
	 * When there are still pending Tasks it submits next Task to the slave.
	 * Returns true when all tasks for a job state are in Finished state.
	 *
	 *
	 * @param ch
	 * @param res
	 * @param taskLst
	 * @param tracker
	 * @param caller
	 * @return boolean
	 */
	private boolean handleTaskResult(Channel ch, TaskResult res, List<Task> taskLst, Map<Integer, TaskTracker> tracker,
			String caller) {
		boolean isTaskFinished = false;
		// update the askTracker
		taskSM.updateTaskTracker(res, tracker);

		if (res.getErrorCode() == ERR_CODE.MAP_SUCCESS || res.getErrorCode() == ERR_CODE.REDUCE_SUCCESS
				|| res.getErrorCode() == ERR_CODE.LOG_SUCCESS) {
			// submit the nextJob if any
			if (!taskLst.isEmpty()) {
				taskSM.submitNextTask(ch.remoteAddress().toString(), taskLst, tracker);
			} else {
				// check if all the Tasks Returned Result
				if (taskSM.allTasksFinished(taskLst, tracker)) {
					logger.info("All " + caller + "Tasks finished successfully " + "TotalTime "
							+ taskSM.totalTimeTasksFinished(taskLst, tracker) + " secs");
					isTaskFinished = true;
				}
			}
		} else {
			// currently no retry mechanism for the map fail/reducer fail
			// task implemented
			logger.error(caller + " Task failed: " + res.toString() + " Terminating");
			slaveSM.onError();
		}
		return isTaskFinished;
	}

}
