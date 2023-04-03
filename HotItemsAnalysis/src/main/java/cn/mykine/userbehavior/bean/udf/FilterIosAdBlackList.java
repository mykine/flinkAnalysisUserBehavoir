package cn.mykine.userbehavior.bean.udf;

import cn.mykine.userbehavior.bean.pojo.AdClickData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 风控-对用户一天内过多点击同一个广告的行为进行黑名单处理
 * */
public class FilterIosAdBlackList extends KeyedProcessFunction<Tuple2<String,String>, AdClickData, AdClickData> {
    //点击次数限制
    private Integer countLimit;
    //状态-存储用户点击次数
    ValueState<Long> stateCountClick;
    //状态-存储用户是否记入黑名单
    ValueState<Boolean> stateIsBlack;

    public FilterIosAdBlackList(Integer countLimit) {
        this.countLimit = countLimit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //对状态初始化,这里不赋初始值，到第一次使用时再赋初始值
        stateCountClick = getRuntimeContext()
                .getState(new ValueStateDescriptor<Long>("ad-count",Long.class));
        stateIsBlack = getRuntimeContext()
                .getState(new ValueStateDescriptor<Boolean>("is-black",Boolean.class));
    }

    @Override
    public void processElement(AdClickData value,
                               KeyedProcessFunction<Tuple2<String,String>, AdClickData, AdClickData>.Context ctx,
                               Collector<AdClickData> out
    ) throws Exception {
        //第一次使用时(即第一条数据来了时)，对状态进行赋初始值，并注册个定时器(即闹钟)用于凌晨重置状态
        if(stateCountClick.value()==null){
            stateCountClick.update(0L);
            //计算出下一天（东八区时区）凌晨毫秒时间戳
            Long ts = (ctx.timerService().currentProcessingTime() / 86400000 + 1) * 86400000 - 8*60*60*1000;
            ctx.timerService().registerProcessingTimeTimer(ts);
        }
        if(stateIsBlack.value()==null){
            stateIsBlack.update(false);
        }

        //先判断用户是否已经进入黑名单
        if(stateIsBlack.value()){
            System.out.println("用户已经在黑名单，数据无效，直接跳过! iosDeviceId:"+value.getIosDeviceid());
            return;//并直接返回
        }
        //判断点击数是否达到限制
        Long clickCount = stateCountClick.value();
        stateCountClick.update(clickCount+1);
        if(clickCount>=countLimit){
            //更新黑名单状态，并直接返回
            stateIsBlack.update(true);
            return;
        }
        //风控校验通过，数据正常流转
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<String, String>, AdClickData, AdClickData>.OnTimerContext ctx, Collector<AdClickData> out) throws Exception {
        //闹钟处理-清空状态
        stateCountClick.clear();
        stateIsBlack.clear();
    }
}
