package neu.mr.cs6240.userProcess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

/**
 * This is a class for spawning a separate JVM to run the user mapreduce code
 * @author prasad memane
 * @author swapnil mahajan
 * @author ajay subramnaya
 */
public class UserProcess {
	
	private final static Logger logger = Logger.getLogger(UserProcess.class);

	/**
	 * This method runs in the users command, mapreduce code in a separate process
	 * @param command
	 */
	public void runProcess(String command) {
		try {
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
			printLines("User's MapReduce code: " + "[INFO]:", p.getInputStream());
			printLines("User's MapReduce code: " + "[ERROR]:", p.getErrorStream());
			System.out.println(command + " exitValue() " + p.exitValue());
		} catch(IOException ioe) {
			logger.error("IOException while executing the user mapreduce code: ", ioe);
			System.exit(1);
		} catch(InterruptedException ie) {
			logger.error("InterruptedException while executing the user mapreduce code: ", ie);
			System.exit(1);
		}
	}

	/**
	 * This method prints out the error or info thrown by the users mapreduce code
	 * @param name
	 * @param ins
	 */
	private static void printLines(String name, InputStream ins) {
		try {
			String line = null;
			BufferedReader in = new BufferedReader(new InputStreamReader(ins));
			while ((line = in.readLine()) != null) {
				System.out.println(name + " " + line);
			}
		} catch(IOException ioe) {
			logger.error("IOException while printing input or error stream for user mapreduce code: ", ioe);
		}
	}
	
}
