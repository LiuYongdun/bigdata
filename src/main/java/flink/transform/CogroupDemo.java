package flink.transform;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CogroupDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("hadoop01", 9000);

        DataStreamSource<String> source2 = env.socketTextStream("hadoop01", 8000);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndValue1 = source1.map(each -> {
            String[] splits = each.split(" ");
            return Tuple2.of(splits[0], Integer.parseInt(splits[1]));
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndValue2 = source2.map(each -> {
            String[] splits = each.split(" ");
            return Tuple2.of(splits[0], Integer.parseInt(splits[1]));
        });

        wordAndValue1.coGroup(wordAndValue2)
                .where(each->each.f0).equalTo(each->each.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2000)))
                .apply(new MyCogroupFunc()).print();

        env.execute();
    }
}

class MyCogroupFunc extends RichCoGroupFunction<Tuple2<String, Integer>,
        Tuple2<String, Integer>, Tuple4<String, String,Integer,Integer>>{

    @Override
    public void coGroup(Iterable<Tuple2<String, Integer>> first,
                        Iterable<Tuple2<String, Integer>> second,
                        Collector<Tuple4<String, String, Integer, Integer>> out) {

        int sum1=0;
        String key1=null;
        for (Tuple2<String, Integer> tuple2 : first) {
            sum1+=tuple2.f1;
            if(null==key1)
                key1=tuple2.f0;
        }


        int sum2=0;
        String key2=null;
        for (Tuple2<String, Integer> tuple2 : second) {
            sum2+=tuple2.f1;
            if(null==key2)
                key2=tuple2.f0;
        }

        out.collect(Tuple4.of(key1,key2,sum1,sum2));

    }
}
