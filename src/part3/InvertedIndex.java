package part3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class InvertedIndex {
	public static void main(String[] args) throws Exception {
		InvertedIndex wc = new InvertedIndex();
		//wc.run(args[0], args[1]);
		wc.run2(args[1], args[2]);
		
	}
	
	public void run (String inputPath, String outputPath) throws IOException {
		/* 
		 * You can lookup usage of these api from this website. =)
		 * http://tool.oschina.net/uploads/apidocs/hadoop/index.html?overview-summary.html
	   	 */
		 
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("Inverted Index");
		
		conf.setMapperClass(InvertedIndexMapper.class);
		conf.setReducerClass(InvertedIndexReducer.class);
		conf.setCombinerClass(InvertedIndexCombiner.class);
		
		/*
		 * If your mapper output key-value pair is different from final 
		 * output key-value pair, please remember to setup these two APIs.
		 */ 

		
		conf.setMapOutputKeyClass(ITTKey.class);
		conf.setMapOutputValueClass(ITTValue.class);
		
		conf.setOutputKeyClass(ITTKey.class);
		conf.setOutputValueClass(ITTValue.class);
		
		
		// TODO setup your partitioner class
		conf.setPartitionerClass(InvertedIndexPartitioner.class);
		
	
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(3);
		conf.setNumReduceTasks(2);

		JobClient.runJob(conf);
	}
	
	public void run2(String inputPath, String outputPath) throws IOException {
		 
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("Build Table");
		
		conf.setMapperClass  (BuildTableMapper.class);
		conf.setReducerClass (BuildTableReducer.class);
		conf.setCombinerClass(BuildTableCombiner.class);
		
		/*
		 * If your mapper output key-value pair is different from final 
		 * output key-value pair, please remember to setup these two APIs.
		 */ 

		
		conf.setMapOutputKeyClass(TableKey.class);
		conf.setMapOutputValueClass(TableValue.class);
		
		conf.setOutputKeyClass(TableKey.class);
		conf.setOutputValueClass(TableValue.class);
		
		
		// TODO setup your partitioner class
		conf.setPartitionerClass(BuildTablePartitioner.class);
		
	
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(3);
		conf.setNumReduceTasks(2);

		JobClient.runJob(conf);
	}
}
