package com.across.test;

import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.across.mapper.JoinDataSetsMapper;

/**
 * This tests the mapper
 * 
 * @author pawank
 *
 */
public class JoinDataSetsMapperTest {

	public MapDriver<LongWritable, Text, Text, Text> mapDriver;

	@Before
	public void setUp() {
		JoinDataSetsMapper dataSetsMapper = new JoinDataSetsMapper();
		mapDriver = MapDriver.newMapDriver(dataSetsMapper);

	}
	
	
	@Test
	  public void testMapper() throws URISyntaxException {
		Configuration conf = new Configuration();
		mapDriver.setConfiguration(conf);
		DistributedCache.addLocalFiles(conf,"/home/pawank/workspace/spring-boot/33across_solution/src/test/resources/ccds1.csv");
		
		//if give  input  is \tUS\t223\t \t 
	    mapDriver.withInput(new LongWritable(), new Text(
	        "1\tUS\t223\t \t "));
	    
	    //output should be (("US" ,"1,US,Mozilla/5.0 (playstation 444),223, ,")  )
	    mapDriver.withOutput(new Text("US"), new Text("1,US,\"Mozilla/5.0 (playstation 444)\",223, , "));
	    mapDriver.runTest();
	  }

}
