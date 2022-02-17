package hadoop.mr.wordCount;

import hadoop.mr.wordCount.WCMapper;
import hadoop.mr.wordCount.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //需要有输入路径和输出路径
        if(args.length != 2) {
            System.err.println("Usage: WCDriver <input path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WCDriver.class);

        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCReducer.class);
        job.setReducerClass(WCReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        //设置作业的输入和输出
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //启动作业
        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone?0:1);
    }
}
