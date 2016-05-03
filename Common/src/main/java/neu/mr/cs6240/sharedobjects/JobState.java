package neu.mr.cs6240.sharedobjects;

import java.io.Serializable;

/**
 * Different job states in the lifecycle of job received to executed to finished
 *
 * @author smitha
 *
 */
public enum JobState implements Serializable {
		NO_JOB(0),
		RECEIVED(1),
		SUBMITTED(2),
		MAP_RUNNING(3),
		SHUFFLE_RUNNING(4),
		REDUCE_RUNNING(5),
		UPLOADING_LOGS(6),
		FINISHED(7),
		ERROR(8);

	private int code;

	private JobState(int value) {
		this.code = value;
	}

	public int val() {
		return code;
	}

	public static JobState fromValue(JobState value) throws IllegalArgumentException {
		try {
			return JobState.values()[value.val()];
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Unknown enum value :" + value);
		}
	}

}
