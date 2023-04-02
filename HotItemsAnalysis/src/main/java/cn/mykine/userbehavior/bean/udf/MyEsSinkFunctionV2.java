package cn.mykine.userbehavior.bean.udf;

import cn.mykine.userbehavior.bean.pojo.AdClickData;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 自行封装flink操作es客户端的func类
 * */
@Slf4j
public class MyEsSinkFunctionV2 extends RichSinkFunction<AdClickData> implements SinkFunction<AdClickData> {

    private static BulkProcessor bulkProcessor = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("MyEsSinkFunctionV2 open~~~");
        log.info("MyEsSinkFunctionV2 open");
        //创建bulkProcessor实例
        initBulkProcessor();
        System.out.println("initBulkProcessor end~~~");
        super.open(parameters);
    }

    /**
     * 创建bulkProcessor实例
     * */
    private void initBulkProcessor(){
        if(null==bulkProcessor){
            System.out.println("initBulkProcessor bulkProcessor exec~~~");
            //创建连接es的客户端
            List<HttpHost> httpHosts = new ArrayList<>();
            //填充数据
            httpHosts.add(new HttpHost("192.168.10.135", 9200));
            httpHosts.add(new HttpHost("192.168.10.136", 9200));
            httpHosts.add(new HttpHost("192.168.10.137", 9200));

            //填充host节点
//            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[1]));
//            httpHosts.toArray(new HttpHost[0])
            restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> {
                requestConfigBuilder.setConnectTimeout(1000);
                requestConfigBuilder.setSocketTimeout(1000);
                requestConfigBuilder.setConnectionRequestTimeout(1000);
                return requestConfigBuilder;
            });

            //填充用户名密码
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("userName", "password"));

            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.setMaxConnTotal(30);
                httpClientBuilder.setMaxConnPerRoute(30);
//            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                return httpClientBuilder;
            });
            RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

            //创建监听器,可以控制按记录数目、大小、时间周期进行批量写入
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    log.info("1. 【创建bulkProcessor实例 beforeBulk】批次[{}] 携带 {} 请求数量", executionId, request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                                      BulkResponse response) {
                    if (!response.hasFailures()) {
                        log.info("2. 【创建bulkProcessor实例 afterBulk-成功】批量 [{}] 完成在 {} ms", executionId, response.getTook().getMillis());
                    } else {
                        BulkItemResponse[] items = response.getItems();
                        for (BulkItemResponse item : items) {
                            if (item.isFailed()) {
                                log.info("2. 【创建bulkProcessor实例 afterBulk-失败】批量 [{}] 出现异常的原因 : {}", executionId, item.getFailureMessage());
                                break;
                            }
                        }
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request,
                                      Throwable failure) {

                    List<DocWriteRequest<?>> requests = request.requests();
                    List<String> esIds = requests.stream().map(DocWriteRequest::id).collect(Collectors.toList());
                    log.error("3. 【afterBulk-failure失败】es执行bluk失败,失败的esId为：{}", esIds, failure);
                }
            };

            //通过builder创建实例对象和对属性赋值
            BulkProcessor.Builder builder = BulkProcessor.builder(
                    ((bulkRequest, bulkResponseActionListener) -> {
                        highLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener);
                    }),
                    listener
            );
            //到达1000条时刷新
            builder.setBulkActions(1000);
            //内存到达8M时刷新
            builder.setBulkSize(new ByteSizeValue(8L, ByteSizeUnit.MB));
            //设置的刷新间隔200ms
            builder.setFlushInterval(TimeValue.timeValueMillis(100));
            //设置允许执行的并发请求数。
            builder.setConcurrentRequests(3);
            //设置重试策略
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3));
            bulkProcessor = builder.build();
        }
    }

    @Override
    public void invoke(AdClickData element, Context context) throws Exception {
        try {
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("platform", element.getPlatform().toString());
            dataSource.put("clickId", element.getClickId());
            dataSource.put("clickTime", element.getClickTime().toString());
            dataSource.put("clickTimeDate",
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                            .format(new Date(element.getClickTime()))
            );
            dataSource.put("iosDeviceid", element.getIosDeviceid());
            dataSource.put("imei", element.getImei());
            dataSource.put("oaid", element.getOaid());
            dataSource.put("androidId", element.getAndroidId());
            dataSource.put("adId", element.getAdId());
            dataSource.put("adName", element.getAdName());
            log.info("dataSource={}",dataSource);
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("ug-test1")
                    .source(dataSource)
                    .id(element.getId());
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            log.error("MyEsSinkFunctionV2 出错 element：{}",element, e);
        }
    }
}
