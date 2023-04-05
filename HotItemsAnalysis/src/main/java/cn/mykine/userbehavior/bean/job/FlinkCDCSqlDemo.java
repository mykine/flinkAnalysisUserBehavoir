package cn.mykine.userbehavior.bean.job;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;

/**
 * FlinkCDC结合FlinkSql，创建MySQL CDC table，实现监听mysql-binlog数据并转化数据为flink.table数据
 * */
public class FlinkCDCSqlDemo {

    public static void main(String[] args) throws Exception {
        //env-创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///E:/myEnv/devData/flinkCheckpoint");


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //创建cdc连接表，用于读取mysql的规则表的binlog
        tenv.executeSql("CREATE TABLE cdc_test_user1 (   " +
                "      id Integer  PRIMARY KEY NOT ENFORCED,    " +
                "      rule_name String,    " +
                "      uids_bitmap BINARY                  " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = '192.168.10.98'   ,         " +
                "     'port' = '3306'          ,         " +
                "      'jdbc.properties.useUnicode' = 'true'          ,         " +
                "      'jdbc.properties.characterEncoding' = 'UTF-8'          ,         " +
                "      'jdbc.properties.serverTimezone' = 'UTC'          ,         " +
                "      'jdbc.properties.useSSL' = 'false'          ,         " +
                "     'username' = 'root'      ,         " +
                "     'password' = 'jyIsTpYmq7%Z'      ,         " +
                "     'database-name' = 'marketing',          " +
                "     'table-name' = 'test_user1'     " +
                ")");
        //读取数据
        Table table = tenv.sqlQuery("select id,rule_name,uids_bitmap from cdc_test_user1");
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);
        //处理数据
        SingleOutputStreamOperator<String> processDataStream = rowDataStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row,
                                       ProcessFunction<Row, String>.Context context,
                                       Collector<String> collector
            ) throws Exception {
                if (row.getKind().equals(RowKind.INSERT)) {
                    //新增的数据
                    Integer ruleId = row.<Integer>getFieldAs("id");
                    String ruleName = row.<String>getFieldAs("rule_name");
                    byte[] uidsBitmap = row.<byte[]>getFieldAs("uids_bitmap");
                    // 反序列化本次拿到的规则的bitmap
                    Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
                    bitmap.deserialize(ByteBuffer.wrap(uidsBitmap));

                    // 判断 10002,10003,30003 用户是否在其中
                    boolean res10002 = bitmap.contains(10002);
                    boolean res10003 = bitmap.contains(10003);
                    boolean res30003 = bitmap.contains(30003);

                    collector.collect(String.format("规则id：%d,规则名称：%s, 用户：10002, 存在于规则人群否: %s ", ruleId, ruleName, res10002));
                    collector.collect(String.format("规则id：%d,规则名称：%s, 用户：10003, 存在于规则人群否: %s ", ruleId, ruleName, res10003));
                    collector.collect(String.format("规则id：%d,规则名称：%s,用户：30003 , 存在于规则人群否: %s ", ruleId, ruleName, res30003));
                }

            }
        });
       //输出
        processDataStream.print("cdc data res");
        //执行
        env.execute();

    }




}
