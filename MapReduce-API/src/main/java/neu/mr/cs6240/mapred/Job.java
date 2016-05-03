package neu.mr.cs6240.mapred;

import java.io.Serializable;

import neu.mr.cs6240.shared.JobStateMachine;
import neu.mr.cs6240.sharedobjects.JobState;
import neu.mr.cs6240.utils.job.JobValidation;
import neu.mr.cs6240.utils.network.Client;

/**
 * validates the supplied arguments and submits the job to MRAppMaster for
 * processing.
 *
 * Below are the required fields
 *
 * <ul>
 * <li>String inputPath</li>
 * <li>String outputPath</li>
 * <li>String jarByClass</li>
 * <li>String jarPath</li>
 * <li>String mapperClass</li>
 * <li>String reducerClass</li>
 * <li>String mapOutputKeyClass</li>
 * <li>String mapOutputValueClass</li>
 * <li>String outputKeyClass</li>
 * <li>String outputValueClass</li>
 * </ul>
 *
 * @author ajay subramanya
 */
public class Job implements Serializable {

	private static final long serialVersionUID = -7841833861684132089L;
	private String jobName;

	private String inputPath;
	private String outputPath;

	private String jarPath;

	private String jarByClass;

	private String mapperClass;
	private String reducerClass;

	private String mapOutputKeyClass;
	private String mapOutputValueClass;

	private String outputKeyClass;
	private String outputValueClass;

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getJarByClass() {
		return jarByClass;
	}

	public void setJarByClass(String jarByClass) {
		this.jarByClass = jarByClass;
	}

	public String getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(String mapperClass) {
		this.mapperClass = mapperClass;
	}

	public String getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(String reducerClass) {
		this.reducerClass = reducerClass;
	}

	public String getMapOutputKeyClass() {
		return mapOutputKeyClass;
	}

	public void setMapOutputKeyClass(String mapOutputKeyClass) {
		this.mapOutputKeyClass = mapOutputKeyClass;
	}

	public String getMapOutputValueClass() {
		return mapOutputValueClass;
	}

	public void setMapOutputValueClass(String mapOutputValueClass) {
		this.mapOutputValueClass = mapOutputValueClass;
	}

	public String getOutputKeyClass() {
		return outputKeyClass;
	}

	public void setOutputKeyClass(String outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	public String getOutputValueClass() {
		return outputValueClass;
	}

	public void setOutputValueClass(String outputValueClass) {
		this.outputValueClass = outputValueClass;
	}

	public String getJarPath() {
		return jarPath;
	}

	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}

	/**
	 * validates the supplied arguments and submits the job to MRAppMaster for
	 * processing.
	 *
	 * Below are the required fields
	 *
	 * <ul>
	 * <li>String inputPath</li>
	 * <li>String outputPath</li>
	 * <li>String jarByClass</li>
	 * <li>String jarPath</li>
	 * <li>String mapperClass</li>
	 * <li>String reducerClass</li>
	 * <li>String mapOutputKeyClass</li>
	 * <li>String mapOutputValueClass</li>
	 * <li>String outputKeyClass</li>
	 * <li>String outputValueClass</li>
	 * </ul>
	 *
	 * @author ajay subramanya
	 */
	public void submit() {
		if (!JobValidation.valid(this)) System.exit(0);
		Client network = new Client(this);
		network.sendJob();
	}

	/**
	 * must be called only after submit job is called. Waits for a job to
	 * complete and notifies once done.
	 *
	 * @author ajay subramanya
	 * @return true when the job is complete - either success or failure
	 */
	public boolean waitForCompletion() {
		JobStateMachine state = JobStateMachine.getInstance();
		while (true) {
			if (state.getJobState() == JobState.ERROR || state.getJobState() == JobState.FINISHED) {
				break;
			}
			continue;
		}

		if (state.getJobState() == JobState.ERROR) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public String toString() {
		return "Job [jobName=" + jobName + ", inputPath=" + inputPath + ", outputPath=" + outputPath + ", jarPath="
		        + jarPath + ", jarByClass=" + jarByClass + ", mapperClass=" + mapperClass + ", reducerClass="
		        + reducerClass + ", mapOutputKeyClass=" + mapOutputKeyClass + ", mapOutputValueClass="
		        + mapOutputValueClass + ", outputKeyClass=" + outputKeyClass + ", outputValueClass=" + outputValueClass
		        + "]";
	}

}