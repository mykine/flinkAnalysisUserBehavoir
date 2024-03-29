package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.pojo.AdClickData;
import cn.mykine.userbehavior.bean.udf.FilterIosAdBlackList;
import cn.mykine.userbehavior.bean.udf.MyEsSinkFunction;
import cn.mykine.userbehavior.bean.utils.FlinkUtils;
import cn.mykine.userbehavior.bean.utils.MyKafkaStringDeserializationSchema;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;


import java.util.ArrayList;

/**
 * ug业务-接入用户点击广告行为数据-使用flink自带的es操作Api
 * */
@Slf4j
public class UGAdClickData {

    public static void main(String[] args) {

        try {
//            System.out.println("tag="+bulkProcessor.tag);
            //获取env和source-kafka,从与jar包同目录下的配置文件conf.properties读取参数
            DataStream<Tuple2<String, String>> kafkaStream = FlinkUtils.createKafkaStreamV2(args,
                    MyKafkaStringDeserializationSchema.class);

            //tansformation
            //-先将数据转化为bean
            DataStream<AdClickData> mapList = kafkaStream
                    .map(new MapFunction<Tuple2<String, String>, AdClickData>() {
                        @Override
                        public AdClickData map(Tuple2<String, String> value) throws Exception {
                            AdClickData adClickData = JSON.parseObject(value.f1, AdClickData.class);
                            String id = value.f0 + "_" + adClickData.getPlatform() + "_" + adClickData.getClickId();
                            adClickData.setId(id);
                            return adClickData;
                        }
                    }).setParallelism(3);

            /**
             * 风控处理-同一个用户一天内点同一个广告达到一定次数进入黑名单，不算入有效的广告用户数据
             * 模拟对苹果用户投放时的情景
             * */
            KeyedStream<AdClickData, Tuple2<String, String>> adClickDataTuple2KeyedStream =
                    mapList.keyBy(new IosBlackListKeySelector());//分组

            DataStream<AdClickData> processList = adClickDataTuple2KeyedStream
                    .process(new FilterIosAdBlackList(5));//处理刷点击量的用户数据


            //sink-写入到es
            processList.print();

//            // 定义es的连接配置
            ArrayList<HttpHost> httpHosts = new ArrayList<>();
            httpHosts.add(new HttpHost("192.168.10.135", 9200));
            httpHosts.add(new HttpHost("192.168.10.136", 9200));
            httpHosts.add(new HttpHost("192.168.10.137", 9200));

            ElasticsearchSink.Builder<AdClickData> adClickDataBuilder = new ElasticsearchSink.Builder<>(
                    httpHosts,
                    new MyEsSinkFunction()
            );
            adClickDataBuilder.setBulkFlushMaxActions(5000);//每5000条数据提交一次
            adClickDataBuilder.setBulkFlushInterval(1000);//每1s条提交一次
            adClickDataBuilder.setBulkFlushMaxSizeMb(1);//内存到达1M时刷新
            processList.addSink(adClickDataBuilder.build())
                    .setParallelism(3)//并行度与es主分片数目一致，提高写入性能
            ;

            //exec
            FlinkUtils.env.execute("adClick");

        } catch (Exception e) {
            log.error("UGAdClickData run error",e);
            e.printStackTrace();
        }
    }

    public static class IosBlackListKeySelector implements KeySelector<AdClickData, Tuple2<String,String>>{

        @Override
        public Tuple2<String, String> getKey(AdClickData adClickData) throws Exception {
            return new Tuple2<>(adClickData.getIosDeviceid(),adClickData.getAdName());
        }

    }

}
