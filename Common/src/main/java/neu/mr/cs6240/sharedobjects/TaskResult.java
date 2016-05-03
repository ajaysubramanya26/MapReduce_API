package neu.mr.cs6240.sharedobjects;

import java.io.Serializable;

/**
 * TaskResult object is used to by Slaves to communicate to MRAppMaster about
 * task result.
 *
 * @author smitha
 *
 */
public class TaskResult implements Serializable {

	/**
	 * default id
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * If value is true then task completed successfully. If value is false task
	 * failed and error code is updated.
	 */

	private Integer taskid;
	private TaskType taskType;
	private ERR_CODE errorCode;

	public Integer getTask_id() {
		return taskid;
	}

	public void setTask_id(Integer task_id) {
		this.taskid = task_id;
	}

	public TaskType getTaskType() {
		return taskType;
	}

	public void setTaskType(TaskType task_type) {
		this.taskType = task_type;
	}

	public ERR_CODE getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(ERR_CODE errorCode) {
		this.errorCode = errorCode;
	}

	public TaskResult(Integer task_id, TaskType task_type, ERR_CODE errorCode) {
		this.taskid = task_id;
		this.taskType = task_type;
		this.errorCode = errorCode;
	}

	@Override
	public String toString() {
		return "TaskResult [taskid=" + taskid + ", taskType=" + taskType + ", errorCode=" + errorCode + "]";
	}

}
