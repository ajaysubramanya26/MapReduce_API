package neu.mr.cs6240.slave;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import neu.mr.cs6240.aws.EC2;
import neu.mr.cs6240.utils.Utils;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticmapreduce.model.InstanceState;

/**
 * abstraction to run the slave instance
 * 
 * @author ajay subramanya
 * @author prasad memane
 * @author swapnil mahajan
 */
public class ProvisionSlave {
	
	private final static Logger logger = Logger.getLogger(ProvisionSlave.class);
	
	private String clientScript;
	private EC2 ec2;
	private String masterIp;
	private List<String> instances;
	private static AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
	private static AmazonEC2Client amazonEC2Client = new AmazonEC2Client(credentials);
	
	/**
	 * default constructor
	 * 
	 * @param credentials
	 *            the file which contains all the ec2 details
	 * @param script
	 *            the bootup run script
	 */
	public ProvisionSlave(String ec2Details, String script, String nodes, String masterIp) {
		this.ec2 = new EC2(ec2Details);
		this.ec2.setMinCount(1);
		this.ec2.setMaxCount(Integer.parseInt(nodes) - 1);
		this.clientScript = script;
		this.masterIp = masterIp;
		this.instances = new ArrayList<>();
	}
	
	/**
	 * starts the slaves, number and other params set using constructor
	 * 
	 * @throws IOException
	 */
	public void start() {
		logger.info("Starting slaves");
		try {
			String scriptArgs = this.masterIp;
			RunInstancesRequest runInstancesRequest = ec2.bootstrap(Utils.getUserDataScript(clientScript, scriptArgs));

			amazonEC2Client.setEndpoint(ec2.getRegion());
			RunInstancesResult runInstancesResult = amazonEC2Client.runInstances(runInstancesRequest);

			List<Instance> instanceIds = runInstancesResult.getReservation().getInstances();
			List<DescribeInstancesRequest> descReq = new ArrayList<>();
			for (Instance i : instanceIds) {
				descReq.add(new DescribeInstancesRequest().withInstanceIds(i.getInstanceId()));
			}

			logger.info("polling to check slaves state");

			while (true) {
				List<Instance> resInstances = new ArrayList<>();
				for (DescribeInstancesRequest dir : descReq) {
					DescribeInstancesResult res = amazonEC2Client.describeInstances(dir);
					resInstances.addAll(res.getReservations().get(0).getInstances());
				}

				List<String> completed = new ArrayList<>();
				for (Instance i : resInstances) {
					logger.info("instance " + i.getInstanceId() + " is in " + i.getState().getName());
					if (i.getState().getName().equalsIgnoreCase(InstanceState.RUNNING.name())) {
						completed.add(i.getInstanceId());
						logger.info("client with public ip " + i.getPublicIpAddress() + " is now running");
					}
				}

				if (completed.size() == this.ec2.getMaxCount()) {
					logger.info("all clients now bootstrapped and running");
					this.instances.addAll(completed);
					return;
				}

				Utils.sleep(10);
			}

		} catch (AmazonServiceException ase) {
			Utils.logServiceError(ase);
		} catch (AmazonClientException ace) {
			Utils.logClientError(ace);
		} catch (IOException ioe) {
			logger.error("IOException while bootstraping the slaves", ioe);
		}
		
	}

	public String getMasterIp() {
		return masterIp;
	}

	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}

	public List<String> getInstances() {
		return instances;
	}

	public void setInstances(List<String> instances) {
		this.instances = instances;
	}
	
}
