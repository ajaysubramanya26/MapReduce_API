package neu.mr.cs6240.Constants;

/**
 * Constants for all network communication
 *
 * @author smitha
 *
 */
public class NetworkCC {
	public static final int JOB_RECEIVE_PORT = Integer.parseInt(System.getProperty("port", "8991"));
	public static final int CHANNEL_BUF_SIZE = 128000000;
	public static final int SLAVE_CONN_PORT = Integer.parseInt(System.getProperty("port", "8992"));
	public static final String JOB_READY_MSG = "JobReady";
	public static final String SLAVE_READY_MSG = "SlaveReady";
	public static final String SLAVE_HEARTBEAT_MSG = "SlaveHB";
	public static final String JOBSUBMITTER_STATUS_MSG = "JobStatus";
	public final static String MASTER_IP_FILE = "MasterIp";

	public static final String S3_START_PATH = "s3://";
	public static final String LOG_START_PATH = "Log_";
	public static final String MAP_TMP_OP_START_PATH = "MapOp_";
}
