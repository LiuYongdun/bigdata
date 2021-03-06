package hadoop.mr.scoreAverage;

import hadoop.common.tuple.Tuple2;
import hadoop.common.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class AvgDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //需要有输入路径和输出路径
        if(args.length != 2) {
            System.err.println("Usage: AvgDriver <input path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "score average");
        job.setJarByClass(AvgDriver.class);

        job.setMapperClass(AvgMapper.class);
//        job.setCombinerClass(AvgCombiner.class);
//        job.setReducerClass(AvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple3.class);

//        job.setOutputKeyClass(Text.class);
//        job.setOutputKeyClass(FloatWritable.class);

        //设置作业的输入和输出
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //启动作业
        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone?0:1);
    }
}
