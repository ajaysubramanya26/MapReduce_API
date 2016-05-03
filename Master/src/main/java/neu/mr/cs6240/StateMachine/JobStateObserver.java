package neu.mr.cs6240.StateMachine;

import org.apache.log4j.Logger;

import neu.mr.cs6240.mapred.Job;
import neu.mr.cs6240.sharedobjects.JobState;

/**
 * Singleton object for maintaining JobState and Job.<br>
 * Shared b/w JobServer and SlaveServer<br>
 * http://crunchify.com/thread-safe-and-a-fast-singleton-implementation-in-java/
 * Assumption : Only one job can run at a time
 *
 * @author smitha
 *
 */
public class JobStateObserver {

	private static final JobStateObserver instance = new JobStateObserver();

	final Logger logger = Logger.getLogger(JobStateObserver.class);

	private JobState jobState;
	private Job job;

	// no class can extend it
	private JobStateObserver() {
		this.jobState = JobState.NO_JOB;
	}

	// Runtime initialization
	public static JobStateObserver getInstance() {
		return instance;
	}

	public synchronized void setJobState(JobState js) {
		this.jobState = js;
	}

	public JobState getJobState() {
		return this.jobState;
	}

	public synchronized void setJob(Job job) {
		this.job = job;
	}

	public Job getJob() {
		return this.job;
	}

	@Override
	public String toString() {
		return "[jobState=" + jobState + "]";
	}

}
