package neu.mr.cs6240.pseudo_cloud;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 01/29/2016
 * @info Assignment 3
 * NOTE : This is retained to get the idea of what a3 actual was.
 *
 * A2 - Distribution. As data sizes will increase the single machine version of your program
 	will not scale. Develop a version of A1 using the Hadoop Map Reduce API.
	Fine print: (0) Group assignment, two students.
	(1) Provide code that can run in pseudo-distributed mode as well as on EMR.
	(3) Produce a graph that plots the average ticket price for each month for each airline.
	Use R. No other output is required. (3) Include a script that executes everything and produces
	the graph. For example, if you use the Unix make command, you should have two targets pseudo
 	and cloud such that typing make pseudo will create a HDFS file system, start hadoop,
  	run your job, get the output, and produce the graph. Typing  make pseudo will run your
  	code on EMR. (4) Only plot airlines with flights in 2015, limit yourself to the 10 airlines
  	with the most flights overall. (5) Information on how to setup AWS is here.
  	(6) Write a one page report that documents your implementation and that describes your
  	results. The report should be automatically constructed as part of running the project to
  	include the plot. (Hint: use LaTeX or Markdown) (7) Submit a tar.gz file which unpacks
  	into a directory name "LastName1_LastName2_A2". That directory should contain a README
  	file that explains how to build and run your code. Make sure that the code is portable.
  	Document what it requires.
  	(8) The reference solution builds off A1, adding 154 lines of Java code and 15 lines of
  	R code.
 *
 * Driver class -
 * Configure and submit MapReduce job
 * */

import neu.mr.cs6240.mapred.Job;

public class AvgPricePerMonthDriver {

	public int run(String[] args) throws Exception {

		Job job = new Job();

		job.setInputPath(args[1]);
		job.setOutputPath(args[2]);

		job.setJobName("A3_Redux");
		job.setJarByClass("neu.mr.cs6240.pseudo_cloud.AvgPricePerMonthDriver");
		job.setJarPath(args[0]);
		job.setMapperClass("neu.mr.cs6240.pseudo_cloud.AirlineMapper");
		job.setMapOutputKeyClass("CustomString");
		job.setMapOutputValueClass("CustomString");
		job.setOutputKeyClass("CustomString");
		job.setOutputValueClass("CustomString");

		setReducer(args, job);
		job.submit();

		return (job.waitForCompletion() ? 0 : 1);
	}

	private void setReducer(String[] args, Job job) {
		switch (args[3]) {
		case "e":
			job.setReducerClass("neu.mr.cs6240.pseudo_cloud.MeanPerMonthReducer");
			break;
		case "d":
			job.setReducerClass("neu.mr.cs6240.pseudo_cloud.MedianPerMonthReducer");
			break;
		case "f":
			job.setReducerClass("neu.mr.cs6240.pseudo_cloud.FastMedianPerMonthReducer");
			break;
		default:
			System.err.println("Invalid calculate option");
			break;
		}
	}
}
