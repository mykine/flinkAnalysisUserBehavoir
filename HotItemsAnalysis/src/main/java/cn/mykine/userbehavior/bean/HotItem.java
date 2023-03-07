package cn.mykine.userbehavior.bean;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * 统计最近1小时内热门topN商品，每隔5分钟刷新一次
 * */
public class HotItem {


    public static void main(String[] args) throws Exception {

        //1.source操作
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置使用事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //生成水印(或叫水位线)的周期,单位毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.135:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

//        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));

        DataStream<String> inputStream = env.readTextFile("C:\\codeDemo\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        //2.转换计算
        //2.1 转换成数据对象并过滤出需要的数据
        DataStream<UserBehavior> mapDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).filter(data->"pv".equals(data.getBehavior()));
        //2.2 设置水印
        DataStream<UserBehavior> dataStream = mapDataStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });


        //2.3 按照商品分组聚合
        DataStream<ItemViewCount> dataWindowAgg = dataStream
                .keyBy("itemId")
//                .timeWindow(Time.hours(1), Time.minutes(5))
                .timeWindow(Time.seconds(30), Time.seconds(15))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        //2.4再对窗口数据分组聚合排序
        DataStream<String> dataResult = dataWindowAgg.keyBy("windowEnd")
                .process(new TopNHotItems(5));

        //3.sink
        dataResult.print("dataResult");


        //4.执行
        env.execute("hot item analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements
            WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple,
                          TimeWindow timeWindow,
                          Iterable<Long> iterable,
                          Collector<ItemViewCount> out
        ) throws Exception {
            //获取分组信息中的key，即商品ID
            Long itemId = tuple.getField(0);
            //窗口时间
            long end = timeWindow.getEnd();
            //统计值
            Long count = iterable.iterator().next();

            //构造需要的返回值类型
            out.collect(new ItemViewCount(itemId,end,count));
        }

    }

    //实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{

        //取前几名
        private Integer topSize;

        public TopNHotItems(Integer topN){
            topSize = topN;
        }

        //状态列表,搜集同一时间窗口内的所有商品统计
        private ListState<ItemViewCount> itemViewCountListState;

        //定时器时间戳状态
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态对象
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("item-view-count-list", ItemViewCount.class)
            );
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "timer-ts",
                            Long.class
                    )
            );
        }

        @Override
        public void processElement(
                ItemViewCount value,
                KeyedProcessFunction<Tuple, ItemViewCount, String>.Context ctx,
                Collector<String> out
        ) throws Exception {
            //每来一条数据的处理
            itemViewCountListState.add(value);
            if( timerTsState.value() == null ){
                //注册定时器
                Long ts = value.getWindowEnd()+1L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
        }

        @Override
        public void close() throws Exception {
            itemViewCountListState.clear();
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {
            //闹钟(定时器)触发,对数据进行倒序排序
            ArrayList<ItemViewCount> itemViewCounts =
                    Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    if(o1.getCount() < o2.getCount()){
                        return 1;//此时交换
                    }else if (o1.getCount() > o2.getCount()){
                        return -1;
                    }else{
                        return 0;
                    }
                }
            })
            ;

            //搜集结果
            String resultStr = "";
            for (int i = 0; i < itemViewCounts.size(); i++) {
                if(i<topSize){
                    resultStr += "NO "+(i+1)+" ,商品ID:"
                            +itemViewCounts.get(i).getItemId()
                            +",热门度:"+itemViewCounts.get(i).getCount()+"\n";
                }
            }
            out.collect(resultStr+"\n\n");


        }


    }

}
