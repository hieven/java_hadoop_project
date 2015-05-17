package part2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalculateAverageReducer extends MapReduceBase
	implements Reducer<Text, SumCountPair, Text, DoubleWritable> {
		
	public void reduce(Text key, Iterator<SumCountPair> values,
		OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		
		// TODO : Calculate (key, <SumCountPair1, SumCountPair2 ... >) to
		// 		  (key, average)
		
		int sum = 0, count = 0;
		while(values.hasNext()){
			SumCountPair pair = values.next();
			sum += pair.getSum();
			count += pair.getCount();
		}
		
		double avg = (double)sum/count;
		output.collect(key, new DoubleWritable(avg));
	}
}
