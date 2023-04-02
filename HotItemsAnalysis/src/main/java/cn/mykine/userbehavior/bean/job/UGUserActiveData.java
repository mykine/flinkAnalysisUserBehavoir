package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.pojo.LoginUserData;
import cn.mykine.userbehavior.bean.udf.MyEsSinkUserActiveFunction;
import cn.mykine.userbehavior.bean.utils.FlinkUtils;
import cn.mykine.userbehavior.bean.utils.MyKafkaStringDeserializationSchema;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.ArrayList;

/**
 * ug业务-用户激活监测
 * */
@Slf4j
public class UGUserActiveData {

    public static void main(String[] args) {

        try {
//            System.out.println("tag="+bulkProcessor.tag);
            //获取env和source-kafka,从与jar包同目录下的配置文件conf2.properties读取参数
            DataStream<Tuple2<String, String>> kafkaStream = FlinkUtils.createKafkaStreamV2(args,
                    MyKafkaStringDeserializationSchema.class);

            //tansformation
            //-先将数据转化为bean
            DataStream<LoginUserData> userList = kafkaStream
                    .map(new MapFunction<Tuple2<String, String>, LoginUserData>() {
                        @Override
                        public LoginUserData map(Tuple2<String, String> value) throws Exception {
                            LoginUserData userActiveData = JSON.parseObject(value.f1, LoginUserData.class);
                            String id = value.f0 + "_" + userActiveData.getId();
                            userActiveData.setId(id);
                            return userActiveData;
                        }
                    })
                    .setParallelism(3);
            //过滤出新用户
            DataStream<LoginUserData> newUserList = userList
                    .filter(new FilterFunction<LoginUserData>() {
                        @Override
                        public boolean filter(LoginUserData loginUserData) throws Exception {
                            return loginUserData.getIsNew()!=null && loginUserData.getIsNew()==1;
                        }
                    }).setParallelism(3);

            //查询es是否广告投放转化的用户，进一步过滤，匹配成功附加广告投放相关的字段值


            //sink-写入到es
            newUserList.print();

//            // 定义es的连接配置
            ArrayList<HttpHost> httpHosts = new ArrayList<>();
            httpHosts.add(new HttpHost("192.168.10.135", 9200));
            httpHosts.add(new HttpHost("192.168.10.136", 9200));
            httpHosts.add(new HttpHost("192.168.10.137", 9200));

            ElasticsearchSink.Builder<LoginUserData> adClickDataBuilder = new ElasticsearchSink.Builder<>(
                    httpHosts,
                    new MyEsSinkUserActiveFunction()
            );
            adClickDataBuilder.setBulkFlushMaxActions(5000);//每5000条数据提交一次
            adClickDataBuilder.setBulkFlushInterval(1000);//每1s条提交一次
            adClickDataBuilder.setBulkFlushMaxSizeMb(1);//内存到达1M时刷新
            newUserList.addSink(adClickDataBuilder.build())
                    .setParallelism(3)//并行度与es主分片数目一致，提高写入性能
            ;

            //exec
            FlinkUtils.env.execute("adClick");

        } catch (Exception e) {
            log.error("UGAdClickData run error",e);
            e.printStackTrace();
        }
    }
}
