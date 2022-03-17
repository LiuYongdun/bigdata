package flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterativeStreamDemo {

    /*
    迭代数据流测试
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("hadoop01", 9000);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCounts = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] splits = value.split(" ");
                return Tuple2.of(splits[0],Integer.parseInt(splits[1]));
            }
        });

        IterativeStream<Tuple2<String, Integer>> iterativeStream = wordAndCounts.iterate();
        SingleOutputStreamOperator<Tuple2<String, Integer>> iterationBody = iterativeStream.map(each -> {
            each.setField(each.f1 - 1, 1);
            return each;
        });

        iterativeStream.closeWith(iterationBody.filter(each->each.f1>0));

        SingleOutputStreamOperator<Tuple2<String, Integer>> lessThanZero = iterationBody.filter(each -> each.f1 <= 0);

        lessThanZero.print();

        env.execute();
    }
}
