package part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BuildTableMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, TableKey, TableValue> {

	public void map(LongWritable key, Text value, OutputCollector<TableKey, TableValue> output, 
		Reporter reporter) throws IOException{
			
			
			String[] strArray	   = value.toString().split("[\\s]+");
			String   word    	   = strArray[0];
			String   fileName 	   = strArray[1];
			Double   termFreq 	   = Double.parseDouble(strArray[2]);
			ArrayList<Long> offset = new ArrayList<Long>();
			
			
			String  inputStr   = strArray[3];
			String  patternStr = "(\\d+)";
		    Pattern pattern    = Pattern.compile(patternStr);
		    Matcher matcher    = pattern.matcher(inputStr);
		    while (matcher.find()) {
		    	offset.add(Long.parseLong(matcher.group().trim()));
		    }
		    			
			TermMember termMember = new TermMember(fileName, termFreq, offset);
		    ArrayList<TermMember> termMembers = new ArrayList<TermMember>();
		    termMembers.add(termMember);
		    
			TableKey   tKey   = new TableKey(word);
			TableValue tValue = new TableValue(0, termMembers);
			
			output.collect(tKey, tValue);
	}
}
