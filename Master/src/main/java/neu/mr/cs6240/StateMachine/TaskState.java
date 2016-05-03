package neu.mr.cs6240.StateMachine;

import java.io.Serializable;

/**
 * This enum maintains current status of each task.
 *
 * @author smitha
 *
 */
public enum TaskState implements Serializable {

		WAITING(0), RUNNING(1), FINISHED(2), ERROR(3);

	private int code;

	private TaskState(int value) {
		this.code = value;
	}

	public int val() {
		return code;
	}

}
