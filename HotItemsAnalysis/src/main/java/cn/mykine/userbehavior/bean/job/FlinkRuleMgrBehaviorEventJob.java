package cn.mykine.userbehavior.bean.job;

import cn.mykine.userbehavior.bean.pojo.*;
import cn.mykine.userbehavior.bean.utils.FlinkUtils;
import cn.mykine.userbehavior.bean.utils.MyKafkaStringDeserializationSchema;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 处理规则和用户行为两种数据流
 * */
@Slf4j
public class FlinkRuleMgrBehaviorEventJob {
    public static void main(String[] args) throws Exception {
        //来自规则管理平台mysql-binlog的流
        DataStream<Row> ruleDataStream = FlinkUtils.createMysqlRuleDataStreamByCDC(args);
        //提取出规则信息和对应的人群bitmap,转换成自定义的bean
        DataStream<RuleEventData> ruleChangeDS = ruleDataStream.map(new MapFunction<Row, RuleEventData>() {
            @Override
            public RuleEventData map(Row row) throws Exception {
                log.info("ruleDataStream-->map ,row:{}",row);
                RuleEventData data = new RuleEventData();
                if (row.getKind().equals(RowKind.INSERT)) {
                    data.setChangeType(DataChangeTypeEnum.INSERT);
                } else if (row.getKind().equals(RowKind.DELETE)) {
                    data.setChangeType(DataChangeTypeEnum.DELETE);
                } else {
                    //-u、+u就是update
                    data.setChangeType(DataChangeTypeEnum.UPDATE);
                }
                Integer ruleId = row.<Integer>getFieldAs("id");
                String ruleName = row.<String>getFieldAs("rule_name");
                byte[] uidsBitmapBytes = row.<byte[]>getFieldAs("uids_bitmap");
                // 反序列化本次拿到的规则的bitmap
                Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
                bitmap.deserialize(ByteBuffer.wrap(uidsBitmapBytes));
                MarketingRule rule = new MarketingRule(ruleId, ruleName, bitmap);
                data.setMarketingRule(rule);
                return data;
            }
        }).setParallelism(1);

        //广播状态类型描述器
        MapStateDescriptor<String,Roaring64Bitmap>  broadcastStateDesc
                = new MapStateDescriptor<>("rule_info", String.class, Roaring64Bitmap.class);
        //将规则数据流转为广播流,方便下游的算子操作所在的所有并行子任务都能接收到广播流中数据的变更,相当于数据共享了
        BroadcastStream<RuleEventData> ruleBroadcastDS = ruleChangeDS.broadcast(broadcastStateDesc);


        //来自kafka的实时用户行为事件流
        DataStream<Tuple2<String, String>> kafkaStream = FlinkUtils.createKafkaStreamV2(args,
                MyKafkaStringDeserializationSchema.class);
        //-先将数据转化为bean
        DataStream<UserEventData> userEventDS = kafkaStream
                .map(new MapFunction<Tuple2<String, String>, UserEventData>() {
                    @Override
                    public UserEventData map(Tuple2<String, String> value) throws Exception {
                        log.info("kafkaStream-->map ,value:{}",value);
                        UserEventData userEventData = JSON.parseObject(value.f1, UserEventData.class);
                        return userEventData;
                    }
                }).setParallelism(3);

        //将行为事件流对用户id分组操作实现进入不同分区并行操作数据，并与规则变更广播流进行合流操作
        SingleOutputStreamOperator<String> processResDS = userEventDS.keyBy(UserEventData::getUid)
                .connect(ruleBroadcastDS)
                .process(new KeyedBroadcastProcessFunction<Long, UserEventData, RuleEventData, String>() {

                    /**
                     * 处理主流-用户行为数据的方法
                     * */
                    @Override
                    public void processElement(UserEventData userEventData,
                                               KeyedBroadcastProcessFunction<Long, UserEventData, RuleEventData, String>.ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        log.info("userEventDS connect ruleBroadcastDS ==>processElement,userEventData:{}",userEventData);
                        //获取合流后广播流中的广播状态,只读功能，状态数据描述要与广播流的一致
                        ReadOnlyBroadcastState<String, Roaring64Bitmap> broadcastState
                                = ctx.getBroadcastState(broadcastStateDesc);

                        //每来一个用户行为数据就遍历广播状态中的所有规则，看用户是否命中其中的某些规则
                        for (Map.Entry<String, Roaring64Bitmap> entry :
                                broadcastState.immutableEntries()) {
                            String ruleName = entry.getKey();
                            Roaring64Bitmap uidsBitmap = entry.getValue();
                            String res = userEventData.getUid() + "这个用户"
                                    + (uidsBitmap.contains(userEventData.getUid()) ? "在" : "不在")
                                    + "规则:" + ruleName + "范围内";
                            out.collect(res);
                        }

                    }

                    /**
                     * 处理广播流-规则变更数据的方法
                     * */
                    @Override
                    public void processBroadcastElement(RuleEventData ruleEventData,
                                                        KeyedBroadcastProcessFunction<Long, UserEventData, RuleEventData, String>.Context ctx,
                                                        Collector<String> out) throws Exception {
                        log.info("userEventDS connect ruleBroadcastDS ==>processBroadcastElement,ruleEventData:{}",ruleEventData);
                        //每来一个规则变更数据就获取广播流一个广播状态对象,并存储所需要的数据到广播状态,给其他地方使用状态值
                        BroadcastState<String, Roaring64Bitmap> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
                        if (ruleEventData.getChangeType().equals(DataChangeTypeEnum.DELETE)) {
                            //删除操作
                            broadcastState.remove(ruleEventData.getMarketingRule().getRuleName());
                        } else {
                            //新增或修改操作
                            broadcastState.put(ruleEventData.getMarketingRule().getRuleName(),
                                    ruleEventData.getMarketingRule().getUidsBitmap());
                        }
                    }
                }).setParallelism(1);

        //sink
        processResDS.print("打印").setParallelism(1);
        FlinkUtils.env.execute("FlinkRuleMgrBehaviorEventJob");

    }
}
