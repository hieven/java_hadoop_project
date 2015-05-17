package part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndexMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, ITTKey, ITTValue> {

	public void map(LongWritable key, Text value, OutputCollector<ITTKey, ITTValue> output, 
		Reporter reporter) throws IOException{
			
			// File name
			FileSplit fileSplit	   = (FileSplit)reporter.getInputSplit();
	      	String    filename     = fileSplit.getPath().getName();
			
	      	// Regular Expression
	      	String  inputStr   = value.toString();
		    String  patternStr = "[A-Za-z]+";
		    Pattern pattern    = Pattern.compile(patternStr);
		    Matcher matcher    = pattern.matcher(inputStr);
		    
		    
		    
		    while (matcher.find()) {
		    	Long tmp = key.get() + (long)matcher.start();
		    	ArrayList<Long> arr = new ArrayList<Long>();
		    	arr.add(tmp);
		    	
		    	ITTValue  offset       = new ITTValue (arr, (Double)0.0);
		    	ITTKey wordEntry    = new ITTKey(filename, matcher.group());
		    	reporter.incrCounter("file", filename, 1);
		    	output.collect(wordEntry, offset);
		    }
	}
}
