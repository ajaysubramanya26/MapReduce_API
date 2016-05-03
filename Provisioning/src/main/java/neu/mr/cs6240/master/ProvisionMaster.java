package neu.mr.cs6240.master;

import java.io.BufferedWriter;
import static neu.mr.cs6240.Constants.NetworkCC.MASTER_IP_FILE;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import neu.mr.cs6240.aws.EC2;
import neu.mr.cs6240.utils.Utils;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.InstanceState;

/**
 * This is a class for running the master instance
 * @author prasad memane
 * @author ajay subramanya
 * @author swapnil mahajan
 */
public class ProvisionMaster {
	
	private final static Logger logger = Logger.getLogger(ProvisionMaster.class);	

	private EC2 ec2;
	private String serverScript;
	private String ip;
	private Integer slaves;
	private String instance;
	private static AmazonEC2Client ec2Client = new AmazonEC2Client();
	
	/**
	 * default constructor
	 *
	 * @param credentials
	 *            the file which contains all the ec2 details
	 * @param script
	 *            the boot-up run script
	 */
	public ProvisionMaster(String ec2Details, String script, String nodes) {
		this.serverScript = script;
		this.slaves = Integer.parseInt(nodes) - 1;
		this.ec2 = new EC2(ec2Details);
		this.ec2.setMinCount(1);
		this.ec2.setMaxCount(1);
	}
	
	/**
	 * starts the master instance and polls until the machine is running,
	 * returns once the machine is running
	 *
	 * @throws IOException
	 */
	public void start() {
		logger.info("starting master");
		try {
			ec2Client.setEndpoint(ec2.getRegion());
			RunInstancesRequest runInstancesRequest = ec2
					.bootstrap(Utils.getUserDataScript(this.serverScript, this.slaves.toString()));
			RunInstancesResult runInstancesResult = ec2Client.runInstances(runInstancesRequest);

			this.instance = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();
			DescribeInstancesRequest descReq = new DescribeInstancesRequest().withInstanceIds(this.instance);

			logger.info("master instance id : " + this.instance);
			logger.info("polling to check master state");
			while (true) {
				DescribeInstancesResult res = ec2Client.describeInstances(descReq);
				String state = res.getReservations().get(0).getInstances().get(0).getState().getName();
				if (state.equalsIgnoreCase(InstanceState.RUNNING.name())) {
					this.ip = res.getReservations().get(0).getInstances().get(0).getPublicIpAddress();
					logger.info("master started at IP : " + this.ip);
					return;
				}
				Utils.sleep(10);
			}

		} catch (AmazonServiceException ase) {
			Utils.logServiceError(ase);
		} catch (AmazonClientException ace) {
			Utils.logClientError(ace);
		} catch (IOException ioe) {
			logger.error("IOException while bootstraping the master", ioe);
		}
	}
	
	public void writeMasterIP() {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(MASTER_IP_FILE)));
			bw.write(this.ip);
			bw.flush();
			bw.close();			
		} catch(IOException ioe) {
			logger.error("IOException while writing the MasterIp file", ioe);
			System.exit(1);
		}
		
	}
	

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public Integer getSlaves() {
		return slaves;
	}

	public void setSlaves(Integer slaves) {
		this.slaves = slaves;
	}
	
}
