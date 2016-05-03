package neu.mr.cs6240;

import neu.mr.cs6240.mapred.Job;

public class WordCountDriver {

	public static int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = new Job();
		job.setJobName("wordCount");
		job.setJarPath(args[0]);
		job.setJarByClass("neu.mr.cs6240.WordCountDriver");
		job.setMapperClass("neu.mr.cs6240.WordCountMapper");
		job.setReducerClass("neu.mr.cs6240.WordCountReducer");
		job.setMapOutputKeyClass("CustomString");
		job.setMapOutputValueClass("CustomInteger");
		job.setOutputKeyClass("CustomString");
		job.setOutputValueClass("CustomInteger");
		job.setInputPath(args[1]);
		job.setOutputPath(args[2]);
		job.submit();
		return (job.waitForCompletion() ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		run(args);
	}
}
