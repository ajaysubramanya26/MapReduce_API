package neu.mr.cs6240.StateMachine;

import java.io.File;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.log4j.Logger;

import io.netty.channel.Channel;
import neu.mr.cs6240.aws.S3;
import neu.mr.cs6240.mapred.Job;
import neu.mr.cs6240.sharedobjects.JobState;

/**
 * Maintains the heart beat info for each slave
 *
 * @author smitha
 *
 */
class SlaveHeartBeatInfo {

	public SlaveHeartBeatInfo(int slaveId, long lastHeartBeatRcvTime, boolean isActive) {
		this.slaveId = slaveId;
		this.lstHeartBeatRcvTime = lastHeartBeatRcvTime;
		this.isActive = isActive;
	}

	public int getSlaveId() {
		return slaveId;
	}

	public long getLastHeartBeatRcvTime() {
		return lstHeartBeatRcvTime;
	}

	public void setLastHeartBeatRcvTime(long lastHeartBeatRcvTime) {
		this.lstHeartBeatRcvTime = lastHeartBeatRcvTime;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	private int slaveId;
	private long lstHeartBeatRcvTime;
	private boolean isActive;

	@Override
	public String toString() {
		return "[slaveId=" + slaveId + ", lstHeartBeatRcvTime=" + lstHeartBeatRcvTime + ", isActive=" + isActive + "]";
	}

}

/**
 * Singleton class to maintain all state variable for a Server
 *
 * @author smitha
 *
 */
public class SlaveServerStateMachine {
	private static final int TIME_120000_MS = 120000;

	private static final SlaveServerStateMachine instance = new SlaveServerStateMachine();

	final Logger logger = Logger.getLogger(SlaveServerStateMachine.class);

	// no class can extend it
	private SlaveServerStateMachine() {
		masterSlaveServerChannel = null;
		masterJobServerChannel = null;
		jobSubmitterChannel = null;
		slaveChannels = new HashMap<>();
		slaveIpToSlaveId = new HashMap<>();
		numOfSlavesConnected = 0;
		numOfSlavesExpected = 0;
		slaveId = 0;
	}

	// Runtime initialization
	public static SlaveServerStateMachine getInstance() {
		return instance;
	}

	/**
	 * Holds the master slave server channel
	 */
	private Channel masterSlaveServerChannel;

	/**
	 * Holds the master job server channel
	 */
	private Channel masterJobServerChannel;

	/**
	 * Holds Channel for job submitter
	 */
	private Channel jobSubmitterChannel;

	/**
	 * mapping of remote address to Slave Channel
	 */
	private HashMap<String, Channel> slaveChannels;

	/**
	 * mapping for Slave IP address to SlaveID
	 */
	private HashMap<String, SlaveHeartBeatInfo> slaveIpToSlaveId;

	private volatile Integer numOfSlavesConnected; // number of slaves currently
	// connected
	private volatile Integer numOfSlavesExpected; // total number of slaves
													// expected to
	// connect
	private volatile Integer slaveId; // Slave ids will be always increasing

	public Channel getMasterSlaveServerChannel() {
		return masterSlaveServerChannel;
	}

	/**
	 * Used to termination of the Slave Server Channel
	 *
	 * @param ch
	 */
	public void setMasterSlaveServerChannel(Channel ch) {
		this.masterSlaveServerChannel = ch;
	}

	public Channel getMasterJobServerChannel() {
		return masterJobServerChannel;
	}

	/**
	 * Used for termination of the Job Server Channel
	 *
	 * @param ch
	 */
	public void setMasterJobServerChannel(Channel ch) {
		this.masterJobServerChannel = ch;
	}

	/**
	 * Actual clients that connected to master
	 *
	 * @return
	 */
	public Integer getNumOfSlavesConnected() {
		return numOfSlavesConnected;
	}

	public void setNumOfSlavesConnected(Integer numOfSlavesConnected) {
		this.numOfSlavesConnected = numOfSlavesConnected;
	}

	/**
	 * Adds the slave channel when slave sends SLAVE_READY_MSG msg
	 *
	 * @param ch
	 */
	public synchronized void addSlaveChannel(Channel ch) {
		slaveChannels.put(ch.remoteAddress().toString(), ch);
		numOfSlavesConnected++;
		slaveId++;
		slaveIpToSlaveId.put(ch.remoteAddress().toString(),
				new SlaveHeartBeatInfo(slaveId, Calendar.getInstance().getTimeInMillis(), true));
	}

	/**
	 * Removes the slave channel on disconnect
	 *
	 * @param ch
	 */
	public synchronized void removeSlaveChannel(Channel ch) {
		slaveChannels.remove(ch.remoteAddress().toString());
		numOfSlavesConnected--;
		slaveIpToSlaveId.remove(ch.remoteAddress().toString());
	}

	public synchronized void addJobSubmitterChannel(Channel ch) {
		jobSubmitterChannel = ch;
	}

	public Integer getNumOfSlavesExpected() {
		return numOfSlavesExpected;
	}

	public synchronized void setNumOfSlavesExpected(Integer numOfSlavesExpected) {
		this.numOfSlavesExpected = numOfSlavesExpected;
	}

	/**
	 * Function to print the HashMap slaveIpToSlaveId
	 */
	public void printSlaveIptoIds() {
		for (String ip : slaveIpToSlaveId.keySet()) {
			logger.info(ip + " " + slaveIpToSlaveId.get(ip).toString());
		}
	}

	@Override
	public String toString() {
		return "SlaveServerStateMachine [slaveIpToSlaveId=" + slaveIpToSlaveId + ", numOfSlavesConnected="
				+ numOfSlavesConnected + ", numOfSlavesExpected=" + numOfSlavesExpected + "]";
	}

	public Channel getJobSubmitterChannel() {
		return jobSubmitterChannel;
	}

	public HashMap<String, Channel> getSlaveChannels() {
		return slaveChannels;
	}

	public HashMap<String, SlaveHeartBeatInfo> getSlaveIpToSlaveId() {
		return slaveIpToSlaveId;
	}

	/**
	 * Update time the heart beat is received from slave channel
	 *
	 * @param ch
	 */
	public synchronized void updateHeartBeat(Channel ch) {
		String slaveAddr = ch.remoteAddress().toString();
		if (slaveIpToSlaveId.get(slaveAddr) != null) {
			slaveIpToSlaveId.get(slaveAddr).setLastHeartBeatRcvTime(Calendar.getInstance().getTimeInMillis());
			slaveIpToSlaveId.get(slaveAddr).setActive(true);
		}
	}

	/**
	 * Checks if all slaves are still connected by checking the last heart beat
	 * time it received.
	 */
	public synchronized void checkSlavesActive() {
		Long curTime = Calendar.getInstance().getTimeInMillis();

		for (String slaveIp : slaveIpToSlaveId.keySet()) {
			Long elapsedTime = curTime - slaveIpToSlaveId.get(slaveIp).getLastHeartBeatRcvTime();
			// slave not responded with heart beat for more than 2 mins
			if (elapsedTime > TIME_120000_MS) {
				slaveIpToSlaveId.get(slaveIp).setActive(false);
			}
		}
	}

	/**
	 * On error send error status to job submmitter <br>
	 * Close all slave channels<br>
	 * Terminate itself
	 */
	public synchronized void onError() {

		logger.info("In onError");
		// Remove final Output Folder from s3 if created
		deleteFolder(TaskStateMachine.getInstance().getReduceTaskOpPath());
		uploadMasterLog(TaskStateMachine.getInstance().getLogTaskOpPath());
		updateJobStatusInformSubmitter(JobState.ERROR);
		terminateSlavesAndMaster();
	}

	/**
	 * On success send Finished status to job submmitter <br>
	 * Close all slave channels<br>
	 * Upload the master log <br>
	 * Terminate itself
	 */
	public synchronized void onSuccess() {

		uploadMasterLog(TaskStateMachine.getInstance().getLogTaskOpPath());
		updateJobStatusInformSubmitter(JobState.FINISHED);
		terminateSlavesAndMaster();

	}

	/**
	 * Upload the master log to s3
	 *
	 * @param s3path
	 */
	private void uploadMasterLog(String s3path) {
		S3 s3Obj = new S3();
		String logbucketName = s3Obj.getBucketName(s3path);
		String logFolderName = s3Obj.getPrefixName(s3path, logbucketName);
		s3Obj.uploadFile(logbucketName, logFolderName + "/" + "Master.log", new File("./log/Master.log"));
	}

	/**
	 * This method terminates slaves and master
	 */
	private void terminateSlavesAndMaster() {

		// Terminate all slaves and close all channels
		for (String slave : slaveChannels.keySet()) {
			getSlaveChannels().get(slave).close();
		}

		// Terminate itself(jobSubmitterChannel)
		if (jobSubmitterChannel != null) {
			jobSubmitterChannel.close();
		} else {
			logger.fatal("Could not close jobSubmitterChannel channel");
		}

		// Terminate itself(master slave server)
		if (masterSlaveServerChannel != null) {
			masterSlaveServerChannel.close();
		} else {
			logger.fatal("Could not close slave server channel");
		}

		// Terminate itself(master job server)
		if (masterJobServerChannel != null) {
			masterJobServerChannel.close();
		} else {
			logger.fatal("Could not close job server channel");
		}

	}

	/**
	 * Sets the JobState and notifies Job Submitter regarding the change
	 *
	 * @param js
	 */
	public synchronized void updateJobStatusInformSubmitter(JobState js) {

		JobStateObserver observer = JobStateObserver.getInstance();
		observer.setJobState(js);

		jobSubmitterChannel.writeAndFlush(js);
	}

	/**
	 * Returns the slaveId associated with the slave Ip
	 *
	 * @param slave
	 * @return
	 */
	public Integer getSlaveId(String slave) {
		return slaveIpToSlaveId.get(slave).getSlaveId();
	}

	/**
	 * Set activity status of a slave to true when a TaskResult is received
	 * indicating it still active and set the heart time.
	 *
	 * @param slaveIp
	 */
	public void updateActiveStatusOnWrite(String slaveIp) {
		slaveIpToSlaveId.get(slaveIp).setActive(true);
		slaveIpToSlaveId.get(slaveIp).setLastHeartBeatRcvTime(Calendar.getInstance().getTimeInMillis());
	}

	/**
	 * Deletes any intermediate folders
	 *
	 * @param ouputPath
	 */
	private void deleteFolder(String ouputPath) {
		S3 s3Obj = new S3();
		if (ouputPath != null) {
			String opBucketName = s3Obj.getBucketName(ouputPath);
			String opFolderName = s3Obj.getPrefixName(ouputPath, opBucketName);
			if (s3Obj.isValidFile(opBucketName, opFolderName)) {
				s3Obj.deleteFile(opBucketName, opFolderName);
			}
		}
	}

	/**
	 * Function to set the job state
	 */
	public void setObserverJobState(JobState js) {
		JobStateObserver.getInstance().setJobState(js);
	}

	/**
	 * Function to get the job state
	 */
	public JobState getJobServerJobState() {
		return JobStateObserver.getInstance().getJobState();
	}

	/**
	 * Function to get the job
	 */
	public Job getJobServerJob() {
		return JobStateObserver.getInstance().getJob();
	}

}
