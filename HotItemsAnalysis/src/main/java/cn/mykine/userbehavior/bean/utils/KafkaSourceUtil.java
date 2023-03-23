package cn.mykine.userbehavior.bean.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceUtil {

    public static FlinkKafkaConsumer<String> getKafkaSource(String servers, String offset, String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", servers);
        props.setProperty("auto.offset.reset", offset);
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        return  kafkaSource;
    }
}
