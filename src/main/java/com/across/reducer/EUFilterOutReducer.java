package com.across.reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This reducer class is used to filter out the european union countries 
 * 
 * so get the list of the eu contries from the list and filter them out.
 * check if the key is EU country or not 
 *    if the key is not a EU country then write it to a file else do not write it to a file
 * @author pawank
 *
 */
public class EUFilterOutReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	private static List<String> europeanUnionList = new ArrayList<String>();
	private BufferedReader br;
	
	/**
	 * The setup method is used to load the European countries List 
	 */
	@Override
	protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Path[] cachedFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path path : cachedFiles) {
			if (path.getName().toString().equalsIgnoreCase("EUCountries.txt")) {
				loadfilterList(path, context);
			}
		}
	}
	
	/**
	 * Load the EU  list based the file path
	 * @param filePath
	 * @param context
	 * @throws IOException
	 */
	private void loadfilterList(Path filePath, Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException {
		String strLineRead = "";
		try {
			br = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = br.readLine()) != null) {
				String idUaArray[] = strLineRead.split(",");
				europeanUnionList.add(idUaArray[0].trim());
			}
		} catch (Exception e) {
			throw new IOException("Error reading the EUCountries.txt");
		} finally {
			if (br != null) {
				br.close();

			}
		}
		
	}

	/**
	 * The reduce method is used to filter out the EU contries from the main list
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		if (!europeanUnionList.contains(key.toString())){
			Iterator<Text> valuesIt = values.iterator();
			while (valuesIt.hasNext()) {
				context.write(NullWritable.get(), valuesIt.next());
				
			}
		}
	}

}
