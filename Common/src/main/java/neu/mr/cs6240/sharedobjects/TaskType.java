package neu.mr.cs6240.sharedobjects;

import java.io.Serializable;

/**
 * This enum maintains the type of task assigned to slaves.
 *
 * @author smitha
 *
 */
public enum TaskType implements Serializable {
		MAP_TASK(0), REDUCE_TASK(1), UPLOAD_LOG(2);

	private int code;

	private TaskType(int value) {
		this.code = value;
	}

	public int val() {
		return code;
	}
}
