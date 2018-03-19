package com.across.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.across.mapper.JoinDataSetsMapper;
import com.across.reducer.EUFilterOutReducer;

public class JoinAndFilterDriverTest {
	
	public MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		EUFilterOutReducer euFilterOutReducer = new EUFilterOutReducer();
		JoinDataSetsMapper dataSetsMapper = new JoinDataSetsMapper();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(dataSetsMapper, euFilterOutReducer);
		
		Configuration conf = new Configuration();
		mapReduceDriver.setConfiguration(conf);
		DistributedCache.addLocalFiles(conf,"/home/pawank/workspace/spring-boot/33across_solution/src/main/resources/EUCountries.txt");
		DistributedCache.addLocalFiles(conf,"/home/pawank/workspace/spring-boot/33across_solution/src/test/resources/ccds1.csv");
	}
	
	@Test
	public void testMapReduce(){
		mapReduceDriver.withInput(new LongWritable(), new Text(
		        "1\tUS\t223\t \t "));
		Text t = new Text("1,US,Mozilla/5.0 (playstation 444),223, ,");
		List<Text> values = new ArrayList<Text>();
		values.add(t);
		mapReduceDriver.withOutput(NullWritable.get(), new Text("1,US,\"Mozilla/5.0 (playstation 444)\",223, , ") );
		mapReduceDriver.runTest();
	}
}

