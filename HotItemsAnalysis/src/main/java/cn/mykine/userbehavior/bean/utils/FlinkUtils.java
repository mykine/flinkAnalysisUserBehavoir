package cn.mykine.userbehavior.bean.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class FlinkUtils {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static ParameterTool parameterTool;

    private static boolean hasConfigCheckpointing = false;


    private static void initConfigCheckPoint(String[] args)  {
       try {
           if(!hasConfigCheckpointing){
               log.info("initConfigCheckPoint,args:{}",args);
               parameterTool = ParameterTool.fromPropertiesFile(args[0]);

               long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
               String checkpointPath = parameterTool.getRequired("checkpoint.path");
               env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
               env.setStateBackend(new FsStateBackend(checkpointPath));
               //        env.setStateBackend(new RocksDBStateBackend(checkpointPath, true));
               env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

               hasConfigCheckpointing = true;
           }
       }catch (Exception e){
           log.error("initConfigCheckPoint error, args:{}",args,e);
       }
    }

    public static <T> DataStream<T> createKafkaStream(String[] args, Class<? extends DeserializationSchema<T>> deserializer) throws Exception {

//        parameterTool = ParameterTool.fromPropertiesFile(args[0]);
//
//
//        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
//        String checkpointPath = parameterTool.getRequired("checkpoint.path");
//        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
//
////        env.setStateBackend(new RocksDBStateBackend(checkpointPath, true));
//
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        initConfigCheckPoint(args);
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

        //从Kafka中读取数据

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);

        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);

    }


    /**
     * 可以读取Kafka消费数据中的topic、partition、offset
     * @param args
     * @param deserializer
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> DataStream<T> createKafkaStreamV2(String[] args, Class<? extends KafkaDeserializationSchema<T>> deserializer) {

//        parameterTool = ParameterTool.fromPropertiesFile(args[0]);
//
//        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
//        String checkpointPath = parameterTool.getRequired("checkpoint.path");
//        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend(checkpointPath));
////        env.setStateBackend(new RocksDBStateBackend(checkpointPath, true));
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        initConfigCheckPoint(args);
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

        //从Kafka中读取数据
        FlinkKafkaConsumer<T> kafkaConsumer = null;
        try {
            kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializer.newInstance(), properties);
        } catch (Exception e) {
            log.error("createKafkaStreamV2 error ,args:{}",args,e);
            return null;
        }

        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer)
                .setParallelism(3)//并行度与Kafka分区数保持一致，提高性能
                ;

    }

    /**
     * 通过flinkcdc监听mysql-binlo数据到flinkSQl中的表中
     */
    public static  DataStream<Row> createMysqlRuleDataStreamByCDC(String[] args)  {

        initConfigCheckPoint(args);

        String hostname = parameterTool.getRequired("mysql.hostname");
        String port = parameterTool.getRequired("mysql.port");
        String username = parameterTool.getRequired("mysql.username");
        String password = parameterTool.getRequired("mysql.password");
        String databaseName = parameterTool.getRequired("mysql.database-name");
        String tableName = parameterTool.getRequired("mysql.table-name");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //创建cdc连接表，用于读取mysql的规则表的binlog
        tenv.executeSql("CREATE TABLE cdc_test_user1 (   " +
                "      id Integer  PRIMARY KEY NOT ENFORCED,    " +
                "      rule_name String,    " +
                "      uids_bitmap BINARY                  " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = '"+hostname+"'   ,         " +
                "     'port' = '"+port+"'          ,         " +
                "      'jdbc.properties.useUnicode' = 'true'          ,         " +
                "      'jdbc.properties.characterEncoding' = 'UTF-8'          ,         " +
                "      'jdbc.properties.serverTimezone' = 'UTC'          ,         " +
                "      'jdbc.properties.useSSL' = 'false'          ,         " +
                "     'username' = '"+username+"'      ,         " +
                "     'password' = '"+password+"'      ,         " +
                "     'database-name' = '"+databaseName+"',          " +
                "     'table-name' = '"+tableName+"'     " +
                ")");
        //读取数据
        Table table = tenv.sqlQuery("select id,rule_name,uids_bitmap from cdc_test_user1");
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);
        return rowDataStream;
    }

}
