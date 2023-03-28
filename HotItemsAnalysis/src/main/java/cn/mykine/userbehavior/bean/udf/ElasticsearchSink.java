//package cn.mykine.userbehavior.bean.udf;
//import com.alibaba.fastjson.JSONObject;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.elasticsearch.action.bulk.BackoffPolicy;
//import org.elasticsearch.action.bulk.BulkProcessor;
//import org.elasticsearch.action.bulk.BulkRequest;
//import org.elasticsearch.action.bulk.BulkResponse;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.common.unit.ByteSizeUnit;
//import org.elasticsearch.common.unit.ByteSizeValue;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//
//import java.net.InetAddress;
//
//
//
//@Slf4j
//public class ElasticsearchSink extends RichSinkFunction<JSONObject> implements SinkFunction<JSONObject> {
//
//    private static BulkProcessor bulkProcessor = null;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        //BulkProcessor是一个线程安全的批量处理类,允许方便地设置 刷新 一个新的批量请求
//        Settings settings = Settings.builder()
//                .put("cluster.name", "elasticsearch")
//                .put("client.transport.sniff", false)
//                .build();
//        PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);
//        TransportClient client = preBuiltTransportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
//        BulkProcessor.Listener listener = buildListener();
//        BulkProcessor.Builder bulk = BulkProcessor.builder(client, listener);
//        //根据当前添加的操作数设置刷新新批量请求的时间(默认值为1000，-1禁用)
//        bulk.setBulkActions(Property.getIntValue("bulk_actions"));
//        //根据当前添加的操作大小设置刷新新批量请求的时间(默认为5Mb，-1禁用)
//        bulk.setBulkSize(new ByteSizeValue(Property.getLongValue("bulk_size"), ByteSizeUnit.MB));
//        //设置允许执行的并发请求数(默认为1，0仅允许执行单个请求)
//        bulk.setConcurrentRequests(Property.getIntValue("concurrent_request"));
//        //设置一个刷新间隔，如果间隔过去，刷新任何待处理的批量请求(默认为未设置)
//        bulk.setFlushInterval(TimeValue.timeValueSeconds(Property.getLongValue("flush_interval")));
//        //设置一个恒定的后退策略，最初等待1秒钟，最多重试3次
//        bulk.setBackoffPolicy(BackoffPolicy
//                .constantBackoff(TimeValue.timeValueSeconds(Property.getLongValue("time_wait")),
//                        Property.getIntValue("retry_times")));
//        bulkProcessor = bulk.build();
//        super.open(parameters);
//    }
//
//    private static BulkProcessor.Listener buildListener() throws InterruptedException {
//        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
//            @Override
//            public void beforeBulk(long l, BulkRequest bulkRequest) {
//            }
//            @Override
//            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
//            }
//            @Override
//            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
//            }
//        };
//        return listener;
//    }
//
//    @Override
//    public void invoke(JSONObject jsonObject, Context context) throws Exception {
//        try {
//            String topic = jsonObject.getString("topic");
//            String index = "index_";
//            bulkProcessor.add(new IndexRequest(index)
//                    .type(topic)
//                    .source(jsonObject));
//        } catch (Exception e) {
//            log.error("sink 出错 {{}},消息是{{}}", e.getMessage(), jsonObject);
//        }
//    }
//
//}
