package hadoop.mr.scoreAverage;

import hadoop.common.tuple.Tuple2;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class AvgReducer extends Reducer<Text, Tuple2<Integer,Integer>,Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<Tuple2<Integer, Integer>> values, Context context) throws IOException, InterruptedException {

        Tuple2<Integer, Integer> resultTuple = StreamSupport.stream(values.spliterator(), false)
                .reduce((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
                .orElseGet(() -> new Tuple2<>(0,0));

        if(resultTuple._2!=0){
            context.write(key,new FloatWritable(resultTuple._1*1.0f/resultTuple._2));
        }
    }
}
