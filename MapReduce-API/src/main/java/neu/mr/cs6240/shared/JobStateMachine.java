package neu.mr.cs6240.shared;

import neu.mr.cs6240.sharedobjects.JobState;

/**
 * Singleton to share state of the job between the network and the API
 * 
 * @author smitha bangalore
 * @author ajay subramanya
 */
public class JobStateMachine {
	private static final JobStateMachine instance = new JobStateMachine();

	// no class can extend it
	private JobStateMachine() {
	}

	// Runtime initialization
	public static JobStateMachine getInstance() {
		return instance;
	}

	/**
	 * the state at which the job is , takes one of
	 * {@link neu.mr.cs6240.sharedobjects.JobState}
	 */
	private JobState jobState;

	/**
	 * 
	 * @return {@link neu.mr.cs6240.sharedobjects.JobState}
	 */
	public JobState getJobState() {
		return jobState;
	}

	/**
	 * 
	 * @param jobState
	 *            {@link neu.mr.cs6240.sharedobjects.JobState}
	 */
	public void setJobState(JobState jobState) {
		this.jobState = jobState;
	}

}
