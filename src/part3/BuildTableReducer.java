package part3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BuildTableReducer extends MapReduceBase
	implements Reducer<TableKey, TableValue, TableKey, TableValue> {
		
	public void reduce(TableKey key, Iterator<TableValue> values,
		OutputCollector<TableKey, TableValue> output, Reporter reporter) throws IOException {	
		
		ArrayList<TermMember> arr = new ArrayList<TermMember>();
		
		while (values.hasNext()) {
			TableValue tmp = values.next();
			arr.addAll(tmp.getTermMembers());
		}
		
		Collections.sort(arr, new Comparator<TermMember>(){
	        @Override
	        public int compare(TermMember t1, TermMember t2){
	            return  t1.getFileName().compareTo(t2.getFileName());
	        }
	    });
		TableValue tValue = new TableValue(arr.size(), arr);
		
		
		output.collect(key, tValue);
	}
}





