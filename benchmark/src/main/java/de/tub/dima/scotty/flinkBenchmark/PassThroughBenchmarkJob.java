package de.tub.dima.scotty.flinkBenchmark;

import de.tub.dima.scotty.core.windowType.*;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.functions.sink.*;
import org.apache.flink.streaming.api.windowing.time.*;
import org.apache.flink.streaming.api.windowing.windows.*;

import java.util.*;

public class PassThroughBenchmarkJob {
    public PassThroughBenchmarkJob(List<Window> assigners, StreamExecutionEnvironment env, long runtime, int throughput, List<Tuple2<Long, Long>> gaps) {
        Map<String, String> configMap = new HashMap<>();
        ParameterTool parameters = ParameterTool.fromMap(configMap);

        env.getConfig().setGlobalJobParameters(parameters);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.setMaxParallelism(1);

        DataStream<Tuple4<String, Integer, Long, Long>> messageStream = env
                .addSource(new LoadGeneratorSource(runtime, throughput, gaps));

        messageStream.flatMap(new ThroughputLogger<>(200, throughput));

        final SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> timestampsAndWatermarks = messageStream
                .assignTimestampsAndWatermarks(new BenchmarkJob.TimestampsAndWatermarks());

        timestampsAndWatermarks
                .keyBy(0)
                .addSink(new DiscardingSink<>());

        try {
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
