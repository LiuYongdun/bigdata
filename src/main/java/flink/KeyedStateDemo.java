package flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;

public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<String, Integer>> words = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            words.add(Tuple2.of(String.format("word_%s",1),i));
        }

        DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(words);
        source.keyBy(each->each.f0).flatMap(new TwoAvgFlatMap()).print();
        env.execute();

    }
}

class TwoAvgFlatMap extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>{

    private ValueState<Tuple2<Integer,Integer>> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //state descriptor获取引用
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor = new ValueStateDescriptor<>(
                "sumState",
                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})
        );
        sumState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws IOException {
        Tuple2<Integer, Integer> stateValue = sumState.value();
        if(null==stateValue)
            stateValue=Tuple2.of(0,0);

        stateValue.f0+=1;
        stateValue.f1+=value.f1;
        sumState.update(stateValue);

        if (sumState.value().f0==2){
            out.collect(Tuple2.of(value.f0,sumState.value().f1/sumState.value().f0));
            sumState.clear();
        }
    }
}

