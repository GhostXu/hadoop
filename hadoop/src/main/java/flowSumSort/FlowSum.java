package flowSumSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSum {
    public static class FlowSumSortMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
        FlowBean v = new FlowBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            String phoneNum = values[0];
            long upFlow = Long.parseLong(values[1]);
            long downFlow = Long.parseLong(values[2]);
            k.set(phoneNum);
            v.set(upFlow, downFlow);

            context.write(k, v);
        }
    }

    public static class FlowSumSortReducer extends Reducer<Text, FlowBean, FlowBean, Text> {
        FlowBean k = new FlowBean();
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values,Context context)
                throws IOException, InterruptedException {
            long upFlow = 0;
            long downFlow = 0;
            for(FlowBean v : values){
                upFlow += v.getUpFlow();
                downFlow += v.getDownFlow();

            }
            k.set(upFlow, downFlow);
            context.write(k, key);
        }
    }

    public FlowSum() {

    }

    public void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        /**
         * 1.
         */
        job.setJar("flowSumSort.jar");
        job.setMapperClass(FlowSumSortMapper.class);
        job.setReducerClass(FlowSumSortReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("/flowsumsort/output"));
        FileInputFormat.setInputPaths(job, new Path("/flowsumsort/input"));

        boolean flag = job.waitForCompletion(true);
        System.exit(flag==true?0:1);
    }
}
