package cn.mykine.userbehavior.bean.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static ParameterTool parameterTool;

    public static <T> DataStream<T> createKafkaStream(String[] args, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {

        parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointPath = parameterTool.getRequired("checkpoint.path");
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

        //从Kafka中读取数据
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);

        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);

    }


}
