package hadoop.mr.scoreAverage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import scala.Tuple2;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class AvgCombiner extends Reducer<Text, Tuple2<Integer,Integer>, Text, Tuple2<Integer,Integer>> {
    @Override
    protected void reduce(Text key, Iterable<Tuple2<Integer, Integer>> values, Context context) throws IOException, InterruptedException {

        Tuple2<Integer, Integer> resultTuple = StreamSupport.stream(values.spliterator(), false)
                .reduce((tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
                .orElseGet(() -> new Tuple2<>(0,0));

        context.write(key,resultTuple);
    }
}
