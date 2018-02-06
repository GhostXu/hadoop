package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		job.setJar("wordCount.jar");
		/**
		 * 1.指定mapper和reducer类
		 * 2.指定map输出key类型
		 * 3.指定map输出value类型
		 * 4.指定全局输出key类型
		 * 5.指定全局输出value类型
		 * 6.指定inputFormat类型
		 * 7.指定outputFormat类型
		 * 8.指定待分析的文件位置
		 * 9.指定输出文件的位置
		 */
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("/wordCount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordCount/output"));
		
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);
	}
}
