package com.across.execute;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.across.mapper.JoinDataSetsMapper;
import com.across.reducer.EUFilterOutReducer;

/**
 * This classs is the main execution point. It runs the mapper and reduce code.
 * we need to set the configuration files from here.
 * 
 * Following details should be placed here 1. Placing the files to the
 * distributed cache. 2. Setting the mapper and reducer names 3. Setting the
 * mapper input and output file formats 4. Setting the reducer input and the
 * output file formats 5. Setting the output folder path etc..
 * 
 * 
 * Important : there should be three parameters 
 *             input directory , output directory and  input directory of the dCache file
 *             else it will result into error
 * 
 * @author pawank
 *
 */

public class JoinAndFilterDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new JoinAndFilterDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf(
					"Three parameters are required- <input dir>  <output dir>  <distributed cache file path>\n");
			return -1;
		}

		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("Map-side join with text lookup file in DCache");
		DistributedCache.addCacheFile(new URI(args[2]), conf);

		DistributedCache.addLocalFiles(conf,
				"/home/pawank/workspace/spring-boot/33across_solution/src/main/resources/EUCountries.txt");

		job.setJarByClass(JoinAndFilterDriver.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(JoinDataSetsMapper.class);
		job.setReducerClass(EUFilterOutReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
