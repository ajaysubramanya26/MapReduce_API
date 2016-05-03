package neu.mr.cs6240.provisioning;

import java.io.File;

import static neu.mr.cs6240.Constants.NetworkCC.MASTER_IP_FILE;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import neu.mr.cs6240.aws.EC2;
import neu.mr.cs6240.master.ProvisionMaster;
import neu.mr.cs6240.slave.ProvisionSlave;
import neu.mr.cs6240.userProcess.UserProcess;
import neu.mr.cs6240.utils.CMDParser;
import neu.mr.cs6240.utils.Utils;

import org.apache.log4j.PropertyConfigurator;

/**
 * Initialises the master and slave
 * 
 * @author ajay subramanya
 * @author prasad memane
 * @author swapnil mahajan
 */
public class Provisioning {
	
	private static String log4jConfPath = "./Provisioning/log4jProvisioning.properties";
	
	public static void main(String[] args) {
		CMDParser cmd = new CMDParser(args);
		
		//Initialize the master and slaves and the setup the connection between them
		List<String> instances = new ArrayList<>();
		PropertyConfigurator.configure(log4jConfPath);
		ProvisionMaster master = new ProvisionMaster(cmd.getEc2Details(), cmd.getMasterScript(), cmd.getNumNodes());
		master.start();		
		instances.add(master.getInstance());
		ProvisionSlave slave = new ProvisionSlave(cmd.getEc2Details(), cmd.getSlaveScript(), cmd.getNumNodes(), master.getIp());
		slave.start();
		instances.addAll(slave.getInstances());
		
		//Write the IP of the master to a file
		master.writeMasterIP();
		
		Utils.sleep(30);
		executeUserJar();
		
		//terminate the ec2 machines
		EC2 ec2 = new EC2(cmd.getEc2Details());
		ec2.terminateAllEC2(instances);
	}
	
	/**
	 * Execute the users mapreduce program on the ec2 machines spawned
	 */
	public static void executeUserJar() {
		Scanner reader = new Scanner(System.in);
		System.out.println("Enter the command to run your mapreduce program: ");
		UserProcess up = new UserProcess();
		up.runProcess(reader.nextLine());
		reader.close();
	}
}
