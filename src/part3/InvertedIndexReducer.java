package part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

public class InvertedIndexReducer extends MapReduceBase
	implements Reducer<ITTKey, ITTValue, ITTKey, ITTValue> {
	
	private Group counters;
	
	@Override
	public void configure(JobConf conf) {
	    try {
	    	JobClient client = new JobClient(conf);
	    	RunningJob parentJob = 
	    			client.getJob(JobID.forName( conf.get("mapred.job.id") ));
	    	counters = parentJob.getCounters().getGroup("file");	
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    
	}
	
	
	public void reduce(ITTKey key, Iterator<ITTValue> values,
		OutputCollector<ITTKey, ITTValue> output, Reporter reporter) throws IOException {	
		
		
	
		
		ArrayList<Long> arr = new ArrayList<Long>();
		
		while (values.hasNext()) {
			ITTValue tmp = values.next();
			arr.addAll(tmp.getOffset());
		}
		Collections.sort(arr);
		Double   totalWords     = new Double(counters.getCounter(key.getFile()));
		Double	 offsetLength   = new Double(arr.size());
		ITTValue offset 	    = new ITTValue(arr, offsetLength / totalWords);
		
		
		output.collect(key, offset);
	}
}
