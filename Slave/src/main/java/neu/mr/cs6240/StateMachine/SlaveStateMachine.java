package neu.mr.cs6240.StateMachine;

import neu.mr.cs6240.sharedobjects.ERR_CODE;
import neu.mr.cs6240.sharedobjects.TaskType;

/**
 * Singleton class to maintain all state variables for a slave
 *
 * @author smitha
 *
 */
public class SlaveStateMachine {
	private static final SlaveStateMachine instance = new SlaveStateMachine();

	// no class can extend it
	private SlaveStateMachine() {
	}

	// Runtime initialization
	public static SlaveStateMachine getInstance() {
		return instance;
	}

	private ERR_CODE errCode;
	private String masterIp;
	private TaskType tsk;

	public ERR_CODE getErrCode() {
		return errCode;
	}

	public void setErrCode(ERR_CODE errCode) {
		this.errCode = errCode;
	}

	public String getMasterIp() {
		return masterIp;
	}

	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}

	public TaskType getTsk() {
		return tsk;
	}

	public void setTsk(TaskType tsk) {
		this.tsk = tsk;
	}

}
