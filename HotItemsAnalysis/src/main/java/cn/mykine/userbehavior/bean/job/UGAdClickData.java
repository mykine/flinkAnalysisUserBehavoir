package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.pojo.AdClickData;
import cn.mykine.userbehavior.bean.utils.FlinkUtils;
import cn.mykine.userbehavior.bean.utils.MyKafkaStringDeserializationSchema;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * ug业务-接入用户点击广告行为数据
 * */
@Slf4j
public class UGAdClickData {

    public static void main(String[] args) {

        try {
            //获取env和source-kafka,从与jar包同目录下的配置文件conf.properties读取参数
            DataStream<Tuple2<String, String>> kafkaStream = FlinkUtils.createKafkaStreamV2(args, MyKafkaStringDeserializationSchema.class);

            //tansformation
            //-先将数据转化为bean
            DataStream<AdClickData> mapList = kafkaStream.map(new MapFunction<Tuple2<String, String>, AdClickData>() {
                @Override
                public AdClickData map(Tuple2<String, String> value) throws Exception {
                    AdClickData adClickData = JSON.parseObject(value.f1, AdClickData.class);
                    adClickData.setId(value.f0);
                    return adClickData;
                }
            });

            //sink
            mapList.print();

            //exec
            FlinkUtils.env.execute();
        } catch (Exception e) {
            log.error("UGAdClickData run error",e);
            e.printStackTrace();
        }
    }
}
