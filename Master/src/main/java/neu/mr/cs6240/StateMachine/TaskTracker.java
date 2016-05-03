package neu.mr.cs6240.StateMachine;

import java.util.Calendar;

import neu.mr.cs6240.sharedobjects.Task;

/**
 * Class maintains all the necessary info of Task assignment to slave and time
 * taken and current Task state.
 * <ul>
 * <li>Integer slaveId</li>
 * <li>Task taskObj</li>
 * <li>Long startTime</li>
 * <li>Long endTime</li>
 * <li>TaskState curTaskState</li>
 * </ul>
 *
 * @author smitha
 *
 */
public class TaskTracker {
	Integer slaveId;
	Task taskObj;
	Long startTime;
	Long endTime;
	TaskState curTaskState;

	/**
	 * Used for Map and Reduce task tracking
	 *
	 * @param slaveId
	 * @param taskObj
	 */
	public TaskTracker(Integer slaveId, Task taskObj) {
		this.slaveId = slaveId;
		this.taskObj = taskObj;
		this.startTime = Calendar.getInstance().getTimeInMillis();
		this.curTaskState = TaskState.WAITING;
	}

	/**
	 * Used only when time tracking is required
	 */
	public TaskTracker() {
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	public Long getStartTime() {
		return startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public TaskState getCurTaskState() {
		return curTaskState;
	}

	public void setCurTaskState(TaskState curTaskState) {
		this.curTaskState = curTaskState;
	}

}
