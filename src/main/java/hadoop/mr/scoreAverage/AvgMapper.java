package hadoop.mr.scoreAverage;

import hadoop.common.tuple.Tuple2;
import hadoop.common.tuple.Tuple3;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgMapper extends Mapper<LongWritable, Text, Text, Tuple3> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(" +");
        context.write(new Text(split[0]), new Tuple3(Integer.parseInt(split[1]), 1));
    }
}
