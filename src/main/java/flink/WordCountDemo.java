package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class WordCountDemo {
    /*
    计算wc, 2s开窗
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sentences = env.socketTextStream("hadoop01", 9000);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOnes = sentences.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                Arrays.stream(value.split(" ")).forEach(each->out.collect(Tuple2.of(each,1)));
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCounts = wordAndOnes.keyBy(each -> each.f0)
                .window(TumblingProcessingTimeWindows.of(Time.of(2, TimeUnit.SECONDS)))
                .sum(1);
        wordAndCounts.print();
        env.execute("word count");
    }
}
