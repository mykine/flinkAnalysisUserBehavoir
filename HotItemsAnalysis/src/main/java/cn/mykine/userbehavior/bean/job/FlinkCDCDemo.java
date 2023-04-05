package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.pojo.TestUser1;
import cn.mykine.userbehavior.bean.udf.MyDebeziumDeserializationSchema;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * 通过flink-cdc连接mysql配置，获取binlog日志，并通过自定义binlog反序列工具化类格式化数据成JSONObject
 * */
@Slf4j
public class FlinkCDCDemo {

    public static void main(String[] args) throws Exception {
        //env-创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:/myEnv/devData/flinkCheckpoint");


        Properties prop = new Properties();
        prop.setProperty("useUnicode","true");
        prop.setProperty("characterEncoding","UTF-8");
        prop.setProperty("serverTimezone","UTC");
        prop.setProperty("useSSL","false");
        MySqlSource<JSONObject> mySqlSource = MySqlSource.<JSONObject>builder()
                .hostname("192.168.10.98")
                .port(3306)
                .databaseList("marketing") // set captured database
                .tableList("marketing.test_user1") // set captured table
                .username("root")
                .password("jyIsTpYmq7%Z")
                .startupOptions(StartupOptions.initial())
                //自定义binlog数据反序列工具化类
                .deserializer(new MyDebeziumDeserializationSchema())
                .jdbcProperties(prop)
                .build();


//        env.fromSource(mySqlSource,
//                        WatermarkStrategy.noWatermarks(),
//                        "marketingDB"
//            )
//            .setParallelism(3)
//            .print()
//            .setParallelism(1); // 单个并行度任务保证顺序输出操作
        //读取cdc数据源中mysql-binlog数据并转换成JSONObject类型的数据流
        DataStreamSource<JSONObject> marketingDataStream = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "marketingDB");
        //转换成指定bean
        DataStream<TestUser1> testUser1DataStream = marketingDataStream.map(new MapFunction<JSONObject, TestUser1>() {
            @Override
            public TestUser1 map(JSONObject value) throws Exception {
                TestUser1 tu = new TestUser1();
                tu.setId((Integer) value.get("id"));
                tu.setRuleName(value.get("rule_name").toString());
                tu.setUidsBitmap(byteBufferToByteArray((ByteBuffer) value.get("uids_bitmap")));

                // 反序列化本次拿到的规则的bitmap
                Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
                bitmap.deserialize(ByteBuffer.wrap(tu.getUidsBitmap()));

                // 判断 10002,10003,30003 用户是否在其中
                boolean res10002 = bitmap.contains(10002);
                boolean res10003 = bitmap.contains(10003);
                boolean res30003 = bitmap.contains(30003);

                log.info("规则id：{},规则名称：{}, 用户：{}, 存在于规则人群否: {}", tu.getId(), tu.getRuleName(), 10002, res10002);
                log.info("规则id：{},规则名称：{}, 用户：{}, 存在于规则人群否: {}", tu.getId(), tu.getRuleName(), 10003, res10003);
                log.info("规则id：{},规则名称：{}, 用户：{}, 存在于规则人群否: {}", tu.getId(), tu.getRuleName(), 30003, res30003);

                return tu;

            }
        }).setParallelism(3);

        //打印
        testUser1DataStream.print().setParallelism(1);

        env.execute("flink-cdc-mysql-ruleChange");


    }

    /**
     * byteBuffer 转 byte数组
     * @param buffer
     * @return
     */
    public static byte[] byteBufferToByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes, 0, bytes.length);
        return bytes;
    }


}
