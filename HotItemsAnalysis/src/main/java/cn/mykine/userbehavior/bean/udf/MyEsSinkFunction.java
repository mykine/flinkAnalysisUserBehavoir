package cn.mykine.userbehavior.bean.udf;

import cn.mykine.userbehavior.bean.pojo.AdClickData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

@Slf4j
public class MyEsSinkFunction implements ElasticsearchSinkFunction<AdClickData> {
    @Override
    public void process(AdClickData element, RuntimeContext ctx, RequestIndexer indexer) {
        // 定义写入的数据source

        /**
         *
         * "mappings":{
         *         "properties":{
         *             "id":{"type":"keyword"},
         *             "platform":{"type":"integer"},
         *             "clickId":{"type":"keyword"},
         *             "clickTime":{"type":"long"},
         *             "clickTimeDate": {
         *               "type": "date",
         *               "format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
         *             },
         *             "iosDeviceid":{"type":"keyword"},
         *             "imei":{"type":"keyword"},
         *             "oaid":{"type":"keyword"},
         *             "androidId":{"type":"keyword"},
         *             "adId":{"type":"keyword"},
         *             "adName":{"type":"text","analyzer":"standard","search_analyzer":"standard"}
         *         }
         *     }
         *
         * */


        log.info("process element={}",element);
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

        // 创建请求，作为向es发起的写入命令
        IndexRequest indexRequest = Requests.indexRequest()
                .index("ug-test1")
                .source(dataSource)
                .id(element.getId());

        // 用index发送请求
        indexer.add(indexRequest);
    }
}
