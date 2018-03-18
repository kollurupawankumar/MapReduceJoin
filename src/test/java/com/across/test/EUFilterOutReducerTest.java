package com.across.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.across.reducer.EUFilterOutReducer;

public class EUFilterOutReducerTest {
	
	public ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
	
	
	@Before
	public void setUp() {
		EUFilterOutReducer euFilterOutReducer = new EUFilterOutReducer();
		reduceDriver = ReduceDriver.newReduceDriver(euFilterOutReducer);
		
		Configuration conf = new Configuration();
		reduceDriver.setConfiguration(conf);
		DistributedCache.addLocalFiles(conf,"/home/pawank/workspace/spring-boot/33across_solution/src/main/resources/EUCountries.txt");
		
	}
	
	@Test
	public void testReducerPositive(){
		Text t = new Text("1,US,Mozilla/5.0 (playstation 444),223, ,");
		List<Text> values = new ArrayList<Text>();
		values.add(t);
		reduceDriver.withInput(new Text("US"), values);
		reduceDriver.withOutput(NullWritable.get(), new Text("1,US,Mozilla/5.0 (playstation 444),223, ,") );
		reduceDriver.runTest();
		
	}
	
	@Test
	public void testReducerNegative(){
		Text t = new Text("1,BE,Mozilla/5.0 (playstation 444),223, ,");
		List<Text> values = new ArrayList<Text>();
		values.add(t);
		reduceDriver.withInput(new Text("BE"), values);
	//	reduceDriver.withOutput(null);
		reduceDriver.runTest();
		
	}

}
