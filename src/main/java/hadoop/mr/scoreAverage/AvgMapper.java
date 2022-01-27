package hadoop.mr.scoreAverage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scala.Tuple2;

import java.io.IOException;

public class AvgMapper extends Mapper<IntWritable, Text, Text, Tuple2<Integer, Integer>> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("");
        if(split.length==2){
            context.write(new Text(split[0]), new Tuple2<>(Integer.parseInt(split[1]), 1));
        }
    }
}
