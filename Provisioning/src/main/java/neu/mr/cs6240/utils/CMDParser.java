package neu.mr.cs6240.utils;

import org.apache.log4j.Logger;

/**
 * This is a POJO class for command line arguments
 * @author prasadmemane
 *
 */
public class CMDParser {
	
	private final static Logger logger = Logger.getLogger(CMDParser.class);
	
	private static final int EC2_DETAILS = 0;
	private static final int MASTER_SCRIPT = 1;
	private static final int SLAVE_SCRIPT = 2;
	private static final int NUM_NODES = 3;
	private static final int S3_BUCKET = 4;
	
	private String[] args;
	
	private String ec2Details;
	private String masterScript;
	private String slaveScript;
	private String numNodes;
	private String s3Bucket;
	
	public CMDParser(String[] args) {
		if(args.length != 5) {
			logger.error("Invalid number of arguments in provisioning: " + args);
			System.exit(1);
		}
			
		this.args = args;
		this.ec2Details = args[EC2_DETAILS];
		this.masterScript = args[MASTER_SCRIPT];
		this.slaveScript = args[SLAVE_SCRIPT];
		this.numNodes = args[NUM_NODES];
		this.s3Bucket = args[S3_BUCKET];
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public String getEc2Details() {
		return ec2Details;
	}

	public void setEc2Details(String ec2Details) {
		this.ec2Details = ec2Details;
	}

	public String getMasterScript() {
		return masterScript;
	}

	public void setMasterScript(String masterScript) {
		this.masterScript = masterScript;
	}

	public String getSlaveScript() {
		return slaveScript;
	}

	public void setSlaveScript(String slaveScript) {
		this.slaveScript = slaveScript;
	}

	public String getNumNodes() {
		return numNodes;
	}

	public void setNumNodes(String numNodes) {
		this.numNodes = numNodes;
	}

	public String getS3Bucket() {
		return s3Bucket;
	}

	public void setS3Bucket(String s3Bucket) {
		this.s3Bucket = s3Bucket;
	}
	
}
