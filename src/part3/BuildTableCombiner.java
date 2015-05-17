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

public class BuildTableCombiner extends MapReduceBase
	implements Reducer<TableKey, TableValue, TableKey, TableValue> {
		
	public void reduce(TableKey key, Iterator<TableValue> values,
		OutputCollector<TableKey, TableValue> output, Reporter reporter) throws IOException {	
		
		ArrayList<TermMember> arr = new ArrayList<TermMember>();
		
		while (values.hasNext()) {
			TableValue tmp = values.next();
			arr.addAll(tmp.getTermMembers());
		}
		
		TableValue tValue = new TableValue(0, arr);
		
		
		output.collect(key, tValue);
	}
}

