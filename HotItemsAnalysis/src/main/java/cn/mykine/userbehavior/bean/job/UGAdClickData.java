package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.utils.FlinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * ug业务-接入用户点击广告行为数据
 * */
@Slf4j
public class UGAdClickData {

    public static void main(String[] args) {

        try {
            //获取env和source-kafka,从与jar包同目录下的配置文件conf.properties读取参数
            DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args, SimpleStringSchema.class);

            //tansformation
            kafkaStream.print();

            //sink

            //exec
            FlinkUtils.env.execute();
        } catch (Exception e) {
            log.error("UGAdClickData run error",e);
            e.printStackTrace();
        }
    }
}
