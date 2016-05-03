package neu.mr.cs6240.sharedobjects;

import java.io.Serializable;

/**
 * Task object which is passed to slave to execute a particular task.
 *
 * @author smitha
 *
 */
public class Task implements Serializable {
	/**
	 * default id
	 */
	private static final long serialVersionUID = 1L;

	private Integer taskId; // id associated with each task
	private TaskType taskType; // type of the task

	private String jarByClass; // JarName
	private String jarPath; // s3 jar path
	private String className;
	private String inputPath; // currently s3 input path
	private String outputPath; // currently s3 Output path

	private String outputKeyClass; // based on task type it will be for Mapper
									// or Reducer
	private String outputValueClass;// based on task type it will be for Mapper
									// or Reducer

	// used during reduce task phase only
	private String reduceTaskKey; // holds the key associated with the reduce
									// phase
	private Integer numOfFilesWithKey; // holds the number of files associated
										// with that key. Mainly used by reducer
										// for validation

	public Task(Integer taskId, TaskType taskType) {
		this.taskId = taskId;
		this.taskType = taskType;
	}

	/**
	 * Return task id
	 *
	 * @return : Integer
	 */
	public Integer getTaskId() {
		return taskId;
	}

	/**
	 * Set task id for a Map or Reduce phase
	 *
	 * @param taskId
	 */
	public void setTaskId(Integer taskId) {
		this.taskId = taskId;
	}

	/**
	 * Returns taskType
	 *
	 * @return : TaskType
	 */
	public TaskType getTaskType() {
		return taskType;
	}

	/**
	 * Set taskType (Mapper/Reducer/Log)s
	 *
	 * @param taskType
	 */
	public void setTaskType(TaskType taskType) {
		this.taskType = taskType;
	}

	/**
	 * Get the jar Main class name associated with the task
	 *
	 * @return
	 */
	public String getJarByClass() {
		return jarByClass;
	}

	/**
	 * Set the jar Main class name associated with the task
	 *
	 * @param jarByClass
	 *
	 */
	public void setJarByClass(String jarByClass) {
		this.jarByClass = jarByClass;
	}

	/**
	 *
	 * @return the mapper or reducer class name in the format
	 *         com.some.thing.class
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * Set the Mapper.class in Map phase and Reduce.class in Reduce phase
	 *
	 * @param className
	 */
	public void setClassName(String className) {
		this.className = className;
	}

	/**
	 * Get the input path to read the data. For Map phase user input will be
	 * given. For Reduce phase Mapper output will be given.
	 *
	 * @return
	 */
	public String getInputPath() {
		return inputPath;
	}

	/**
	 * Set the input path to read the data. For Map phase user input will be
	 * given. For Reduce phase Mapper output will be given.
	 *
	 * @return
	 */
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	/**
	 * Get the output path where data has to stored. For Map phase temp output
	 * will be given. For Reduce phase user output will be given.
	 *
	 * @return
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * Set the output path where data has to stored. For Map phase temp output
	 * will be set. For Reduce phase user output will be set.
	 *
	 * @return
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * Gets the Output Key Class. For Map Task MapOutputKeyClass is given. For
	 * Reduce Task OutputKeyClass is given
	 *
	 * @return
	 */
	public String getOutputKeyClass() {
		return outputKeyClass;
	}

	/**
	 * Sets the Output Key Class. For Map Task MapOutputKeyClass is set. For
	 * Reduce Task OutputKeyClass is set.
	 *
	 * @return
	 */
	public void setOutputKeyClass(String outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}

	/**
	 * Gets the Output Value Class. For Map Task MapOutputValClass is given. For
	 * Reduce Task OutputValClass is given
	 *
	 * @return
	 */
	public String getOutputValueClass() {
		return outputValueClass;
	}

	/**
	 * Sets the Output Value Class. For Map Task MapOutputValClass is set. For
	 * Reduce Task OutputValClass is set.
	 *
	 * @return
	 */
	public void setOutputValueClass(String outputValueClass) {
		this.outputValueClass = outputValueClass;
	}

	/**
	 * Gets jar path to download from.
	 *
	 * @return
	 */
	public String getJarPath() {
		return jarPath;
	}

	/**
	 * Sets the jar path from where it can be downloaded.
	 *
	 * @return
	 */
	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}

	/**
	 * Returns the key corresponding to the reduce task
	 *
	 * @return
	 */
	public String getReduceTaskKey() {
		return reduceTaskKey;
	}

	/**
	 * Sets the key corresponding to the reduce task
	 *
	 * @return
	 */
	public void setReduceTaskKey(String reduceTaskKey) {
		this.reduceTaskKey = reduceTaskKey;
	}

	/**
	 * Returns number of files found with key associated with this reduce task
	 *
	 * @return
	 */
	public Integer getNumOfFilesWithKey() {
		return numOfFilesWithKey;
	}

	/**
	 * Sets number of files found with key associated with this reduce task
	 *
	 * @return
	 */
	public void setNumOfFilesWithKey(Integer numOfFilesWithKey) {
		this.numOfFilesWithKey = numOfFilesWithKey;
	}

	@Override
	public String toString() {
		if (reduceTaskKey == null) {
			return "Task [taskId=" + taskId + ", taskType=" + taskType + ", jarByClass=" + jarByClass + ", jarPath="
					+ jarPath + ", className=" + className + ", inputPath=" + inputPath + ", outputPath=" + outputPath
					+ ", outputKeyClass=" + outputKeyClass + ", outputValueClass=" + outputValueClass + "]";
		} else {
			return "Task [taskId=" + taskId + ", taskType=" + taskType + ", jarByClass=" + jarByClass + ", jarPath="
					+ jarPath + ", className=" + className + ", inputPath=" + inputPath + ", outputPath=" + outputPath
					+ ", outputKeyClass=" + outputKeyClass + ", outputValueClass=" + outputValueClass
					+ ", reduceTaskKey=" + reduceTaskKey + ", numOfFilesWithKey=" + numOfFilesWithKey + "]";
		}
	}
}
