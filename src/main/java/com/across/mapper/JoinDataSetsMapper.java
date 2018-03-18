package com.across.mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * In this class the map side join is implemented smaller file is considered as
 * ccds1.csv (This file will be cached) the file ccds2.tsv file will be joined
 * with the cached file ccds1.csv
 * 
 * Example of mapper functionality
 * 
 * Mapper input file sample 1\tUS\t223\t \t
 * Distributed cache sample 1,Mozilla/5.0 (playstation 444)
 * 
 *  Mapper Output : (Text, Text)  -> ("US" ,"1,US,Mozilla/5.0 (playstation 444),223, ,")  
 * 
 * @author pawank
 *
 */
public class JoinDataSetsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Map<String, String> idUaCache = new HashMap<String, String>();
	private BufferedReader br;
	private String uaDetails;
	private Text output;

	/**
	 * Method loads the distributed cache
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Path[] cachedFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path path : cachedFiles) {
			if (path.getName().toString().equalsIgnoreCase("ccds1.csv")) {
				loadidUaCache(path, context);
			}
		}
	}

	/**
	 * private method loads the cache
	 * 
	 * @throws Exception
	 */
	private void loadidUaCache(Path filePath, Context context) throws IOException {
		String strLineRead = "";
		try {
			br = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = br.readLine()) != null) {
				String idUaArray[] = strLineRead.split(",");
				idUaCache.put(idUaArray[0].trim(), idUaArray[1].trim());
			}
		} catch (Exception e) {
			throw new IOException("Error reading the ccds1.csv");
		} finally {
			if (br != null) {
				br.close();

			}
		}
	}

	/**
	 * Map method joins the country data with UA data
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		if (value.toString().length() > 0){
			String countryDataSet[] = value.toString().split("\\t");
			uaDetails = idUaCache.get(countryDataSet[0]);
			output =  new Text(countryDataSet[0]+","+countryDataSet[1]+","
								+ uaDetails + ","+countryDataSet[2]+","+countryDataSet[3]+","
								+ countryDataSet[4]);
			context.write(new Text(countryDataSet[1]), output);
		}
	}

}
