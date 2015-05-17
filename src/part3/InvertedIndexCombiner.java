package part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexCombiner extends MapReduceBase
	implements Reducer<ITTKey, ITTValue, ITTKey, ITTValue> {
		
	public void reduce(ITTKey key, Iterator<ITTValue> values,
		OutputCollector<ITTKey, ITTValue> output, Reporter reporter) throws IOException {	
		
		// TODO : Calculate (key, <Word1, Word2 ... >) 
		//		  to (key, Word1 + Word2 + ...)
		
		ArrayList<Long> arr = new ArrayList<Long>();
		
		while (values.hasNext()) {
			ITTValue tmp = values.next();
			arr.addAll(tmp.getOffset());
		}
		
		ITTValue offset = new ITTValue(arr, (double)0.0);
		
		
		output.collect(key, offset);
	}
}

