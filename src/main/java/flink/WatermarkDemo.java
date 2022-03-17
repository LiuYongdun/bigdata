package flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.ArrayList;

public class WatermarkDemo {
    public static void main(String[] args) throws InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        ArrayList<Tuple3<String, Integer, Long>> users = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(100);
            users.add(Tuple3.of("tom",i*10%5,System.currentTimeMillis()));
        }

        DataStreamSource<Tuple3<String, Integer, Long>> source = env.fromCollection(users);
        source.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner((event, timestamp) -> event.f2));
    }
}
