package neu.mr.cs6240.MRAppMaster;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import neu.mr.cs6240.JobReceiver.JobServer;

/**
 *
 * @author smitha
 * @info
 * 		<ul>
 *       <li>Main class of the Master</li>
 *       <li>MRAppMaster opens a SlaveServer socket and waits for Slaves to
 *       connect(ec2 slave nodes or local).<br>
 *       MRAppMaster will start the JobServer socket to listen to incoming job
 *       requests. Then MRAppMaster waits for a Job submitter receive a job.
 *       </li>
 *       <li>Once Slaves connect each slave gets now added to MRAppMaster's
 *       Slave StateMachine indicating its ready to accept Tasks. <br>
 *       Each slave will send a heart-beat to master and slave alive status is
 *       updated in this state machine. <br>
 *       </li>
 *       <li>Then MRAppMaster waits for Job to be submitted by Job submitter.
 *       Job submitter will connect to master node using Job server. <br>
 *       (this is due to the channel for this has to maintained separately. Not
 *       to be mixed with slave node channels.)
 *       <ul>
 *       <li>Once it receives a Job object in JobServer, MRAppMaster will be
 *       polling JobStateObserver class. Once job is ready then it creates Task
 *       objects. <br>
 *       To create Task object it reads input and creates input splits.
 *       (Currently we are assuming each File itself is a single split.). Each
 *       task object is uniquely identified by as task_id. And a Task TaskResult
 *       Machine will be created.</li>
 *       <li>It creates a Queue of Map Tasks. Once a task is assigned to slave
 *       it will be removed and moved to another queue which indicates tasks in
 *       progress.</li>
 *       <li>Once slave returns TaskResult object indicating success or failure
 *       and error code of the task then it will be removed and Map Task state
 *       machine for this task_id will be updated as finished.<br>
 *       On failure currently no retry mechanism is implemented. Just error is
 *       returned to the user using job submitter channel.</li>
 *       </ul>
 *       </li>
 *       <li>Once all map tasks are completed then shuffle/sort phase is started
 *       by getting the key files from s3. <br>
 *       Partitions are computed and i.e. the output of this Queue of Reduce
 *       tasks will be created. And a Reduce state machine will be created with
 *       reduce task_id.</li>
 *       <li>Based on the number of reduce tasks, master will begin assigning by
 *       highest key:List[value] size to slaves as they take longer time to
 *       complete. <br>
 *       Each Slave returns TaskResult object which indicates success or failure
 *       and error codes in case of failure.</li>
 *       <li>Once all reduce tasks are completed successfully, master sends
 *       another Task object to upload slaves log to s3. And waits for
 *       TaskResult object to be returned from slaves.</li>
 *       <li>Once done writes its log to s3 log path and _SUCCESS to s3 output
 *       path and returns success to job submitter.</li>
 *       </ul>
 *
 */

public class MRAppMaster {

	public static void main(String[] args) throws Exception {

		String log4jConfPath = "./log4jM.properties";
		System.setProperty("logfile.name", "./log/Master.log");
		PropertyConfigurator.configure(log4jConfPath);

		final Logger logger = Logger.getLogger(MRAppMaster.class);
		logger.info("In MRAppMaster");

		final String[] argsFinal = args;

		// Start SlaveServer
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					SlaveServer.startTaskServer(argsFinal);
				} catch (InterruptedException ie) {
					// end of the both threads
					logger.debug("In main thread interruptted by " + Thread.currentThread().getName());
				} catch (Exception e) {
					logger.fatal("Error in Starting SlaveServer", e);
				}
			}
		});
		t1.start();

		// Start JobServer
		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					JobServer.startJobServer(argsFinal);
				} catch (InterruptedException ie) {
					// end of the both threads
					logger.debug("In main thread interrupted by " + Thread.currentThread().getName());
				} catch (Exception e) {
					logger.fatal("Error in Starting Job Server", e);
				}
			}
		});
		t2.start();

		t1.join();
		t2.join();

		logger.info("Shutting down MRAppMaster");
		System.exit(0);
	}
}
