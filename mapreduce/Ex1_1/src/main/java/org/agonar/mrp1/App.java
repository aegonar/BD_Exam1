package org.agonar.mrp1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class App {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: CountJob <input path> <output path>");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(org.agonar.mrp1.App.class);
		job.setJobName("Count Words");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(org.agonar.mrp1.mrp1Mapper.class);
		job.setReducerClass(org.agonar.mrp1.mrp1Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
