package cn.mykine.userbehavior.bean.udf;

import cn.mykine.userbehavior.bean.pojo.AdClickData;
import cn.mykine.userbehavior.bean.pojo.LoginUserData;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MyEsSinkUserActiveFunction extends RichSinkFunction<LoginUserData> implements SinkFunction<LoginUserData> {
    private static BulkProcessor bulkProcessor = null;
    private static RestHighLevelClient highLevelClient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        initRestHighLevelClient();
        initBulkProcessor();
        super.open(parameters);
    }

    @Override
    public void invoke(LoginUserData element, Context context) throws Exception {
        //查询es是否广告投放转化的用户，进一步过滤，匹配成功附加广告投放相关的字段值
        SearchRequest searchRequest = new SearchRequest("ug-test1");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(
                StringUtils.isNotBlank(element.getIosDeviceid())
        ){
            boolQueryBuilder.should(QueryBuilders.termQuery("iosDeviceid", element.getIosDeviceid()));
        }

        if(
                StringUtils.isNotBlank(element.getImei())
        ){
            boolQueryBuilder.should(QueryBuilders.termQuery("imei", element.getImei()));
        }

        if(
                StringUtils.isNotBlank(element.getOaid())
        ){
            boolQueryBuilder.should(QueryBuilders.termQuery("oaid", element.getOaid()));
        }

        if(
                StringUtils.isNotBlank(element.getAndroidId())
        ){
            boolQueryBuilder.should(QueryBuilders.termQuery("androidId", element.getAndroidId()));
        }

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.sort("clickTime", SortOrder.DESC);
        sourceBuilder.from(0);
        sourceBuilder.size(1);
        sourceBuilder.timeout(TimeValue.timeValueMillis(2000));
        searchRequest.source(sourceBuilder);
        SearchResponse searchRes = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = searchRes.getHits().getHits();
        //未命中直接跳过
        if(hits.length<=0){
            System.out.println("未匹配转换数据，跳过~ element:"+ JSON.toJSONString(element));
            return;
        }
        //只取匹配到的最近一个记录作为归因数据
        Map<String, Object> sourceAsMap = hits[0].getSourceAsMap();

        //写入数据
        try {
            String userActiveId = "activeUser_"+sourceAsMap.get("id").toString();
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("uid", element.getId());
            dataSource.put("platform", sourceAsMap.get("platform").toString());
            dataSource.put("firstLoginTime", element.getLoginTime().toString());
            dataSource.put("firstLoginTimeDate",
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                            .format(new Date(element.getLoginTime()))
            );
            dataSource.put("iosDeviceid", sourceAsMap.get("iosDeviceid").toString());
            dataSource.put("imei", sourceAsMap.get("imei").toString());
            dataSource.put("oaid", sourceAsMap.get("oaid").toString());
            dataSource.put("androidId", sourceAsMap.get("androidId").toString());
            dataSource.put("adId", sourceAsMap.get("adId").toString());
            dataSource.put("adName", sourceAsMap.get("adName").toString());
            log.info("dataSource={}",dataSource);
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("ug-user1")
                    .source(dataSource)
                    .id(userActiveId);
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            log.error("MyEsSinkUserActiveFunction 出错 element：{}",element, e);
        }

    }

    /**
     * 创建连接ES的客户端实例
     * */
    private void initRestHighLevelClient(){
        if(highLevelClient==null){
            System.out.println("initRestHighLevelClient exec~~~");
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
            highLevelClient = new RestHighLevelClient(restClientBuilder);
        }
    }

    /**
     * 创建bulkProcessor实例
     * */
    private void initBulkProcessor(){
        if(null==bulkProcessor){
            System.out.println("initBulkProcessor bulkProcessor exec~~~");
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
            //内存到达1M时刷新
            builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
            //设置的刷新间隔200ms
            builder.setFlushInterval(TimeValue.timeValueMillis(200));
            //设置允许执行的并发请求数。
            builder.setConcurrentRequests(3);
            //设置重试策略
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3));
            bulkProcessor = builder.build();
        }
    }

}
