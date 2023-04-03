package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.pojo.LoginUserData;
import cn.mykine.userbehavior.bean.udf.MyEsSinkUserActiveFunction;
import cn.mykine.userbehavior.bean.utils.FlinkUtils;
import cn.mykine.userbehavior.bean.utils.MyKafkaStringDeserializationSchema;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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
            //过滤出有效的新用户数据
            DataStream<LoginUserData> newUserList = userList
                    .filter(new FilterFunction<LoginUserData>() {
                        @Override
                        public boolean filter(LoginUserData loginUserData) throws Exception {
                            if(
                                    StringUtils.isBlank(loginUserData.getIosDeviceid())
                                            &&StringUtils.isBlank(loginUserData.getImei())
                                            &&StringUtils.isBlank(loginUserData.getOaid())
                                            &&StringUtils.isBlank(loginUserData.getAndroidId())
                            ){
                                System.out.println("无效的用户数据:"+JSON.toJSONString(loginUserData));
                                log.info("无效的用户数据:{}",loginUserData);
                                return false;
                            }

                            return loginUserData.getIsNew()!=null && loginUserData.getIsNew()==1;
                        }
                    }).setParallelism(3);

            //sink-查询匹配转化数据并写入到es
            newUserList.print();
            newUserList.addSink(new MyEsSinkUserActiveFunction()).setParallelism(3);
            //exec
            FlinkUtils.env.execute("newUserActive");

        } catch (Exception e) {
            log.error("UGAdClickData run error",e);
            e.printStackTrace();
        }
    }
}
