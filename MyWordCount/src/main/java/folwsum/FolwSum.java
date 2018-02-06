package folwsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FolwSum {
	
	/**
	 * mapper
	 * @param args
	 */
	public static class FolwSumMapper extends Mapper<LongWritable, Text, Text, FolwBean>{
		Text k = new Text();
		FolwBean v = new FolwBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] values = StringUtils.split(str,"\t");
			
			String phoneNum = values[1];
			int upFlow = Integer.parseInt(values[values.length-3]);
			int downFlow = Integer.parseInt(values[values.length-2]);
			k.set(phoneNum);
			v.set(upFlow,downFlow);
			
			context.write(k, v);
		}
	}
	
	/**
	 * reducer
	 * @param args
	 */
	public static class FolwSumReducer extends Reducer<Text, FolwBean, FolwBean, Text>{
		FolwBean v = new FolwBean();
		@Override
		protected void reduce(Text key, Iterable<FolwBean> value,
				Context context) throws IOException, InterruptedException {
			int upFlow = 0;
			int downFlow = 0;
			
			for(FolwBean f : value) {
				upFlow += f.getUpFolw();
				downFlow += f.getDownFolw();
			}
			v.set(upFlow, downFlow);
			context.write(v, key);
		}
	}
	
	public void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		//job.setJar("flowsum.jar");
		job.setJarByClass(FolwSum.class);
		/**
		 * 1.ָ��mapper��reducer��
		 * 2.ָ��map���key����
		 * 3.ָ��map���value����
		 * 4.ָ��ȫ�����key����
		 * 5.ָ��ȫ�����value����
		 * 6.ָ��inputFormat����
		 * 7.ָ��outputFormat����
		 * 8.ָ�����������ļ�λ��
		 * 9.ָ������ļ���λ��
		 */
		job.setMapperClass(FolwSumMapper.class);
		job.setReducerClass(FolwSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FolwBean.class);
		
		job.setOutputKeyClass(FolwBean.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("E://flowsum/input"));
		FileOutputFormat.setOutputPath(job, new Path("E://flowsum/output"));
		
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);
	}
}
