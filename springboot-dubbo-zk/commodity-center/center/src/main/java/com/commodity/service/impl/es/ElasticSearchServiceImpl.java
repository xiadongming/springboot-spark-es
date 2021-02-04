package com.commodity.service.impl.es;

import com.alibaba.fastjson.JSONObject;
import com.commodity.servcie.bo.ESDataBO;
import com.commodity.servcie.es.ElasticSearchService;
import com.fasterxml.jackson.databind.util.JSONPObject;
import javafx.print.Printer;
import org.apache.dubbo.common.json.JSON;
import org.apache.dubbo.config.annotation.Service;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

@Service
public class ElasticSearchServiceImpl implements ElasticSearchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public List<ESDataBO> selectAllES() {
        logger.info("一般的查询全部数据方式：startTime={}", System.currentTimeMillis());
        // SearchRequest searchRequest, RequestOptions options

        /*BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must();*/

       /* SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);*/
        /** 查询全部不用添加条件 */
        SearchRequest searchRequest = new SearchRequest("newsale");
        searchRequest.source();

        SearchResponse search = null;
        List<ESDataBO> esDataBOS = null;
        try {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (null != search) {
                esDataBOS = new ArrayList<>();
                SearchHits hits = search.getHits();
                for (SearchHit hit : hits) {
                    String sourceAsString = hit.getSourceAsString();
                    ESDataBO esDataBO = JSONObject.parseObject(sourceAsString, ESDataBO.class);
                    esDataBOS.add(esDataBO);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("一般的查询全部数据方式：endTime={}", System.currentTimeMillis());
        return esDataBOS;
    }

    @Override
    public Map<String, Object> selectAllESScroll(String fromIndex) {
        SearchResponse searchResponse = null;
        /** 说明是第一次scroll请求，没有scrollId */
        if (StringUtils.isEmpty(fromIndex) || fromIndex.equals("1")) {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            boolQuery.should(QueryBuilders.matchAllQuery());
            // 存活时间，当索引数据量特别大时，出现超时可能性大，此值适当调大
            // Scroll会以间隔时间滚屏的方式返回全部的查询数据，可以作为数据量很大的情况下，分页的一个替代方案
            // 设定滚动时间间隔,超时之后，scroll数据会消失
            String index = "newsale";
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(5L));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(boolQuery);
            searchSourceBuilder.size(5);

            SearchRequest searchRequest = new SearchRequest().indices(index).scroll(scroll).source(searchSourceBuilder);

            try {
                searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(fromIndex);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1L));
            try {
                searchResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        logger.info("searchHits.zise={}", searchHits.length);
        List<ESDataBO> esDataBOS =  new ArrayList<>();
        //遍历搜索命中的数据，直到没有数据
        if (searchHits != null && searchHits.length > 0) {
            for (SearchHit searchHit : searchHits) {
                String sourceAsString = searchHit.getSourceAsString();
                ESDataBO esDataBO = JSONObject.parseObject(sourceAsString, ESDataBO.class);
                esDataBOS.add(esDataBO);
            }
        }
        long totalNum = searchResponse.getHits().getTotalHits().value;
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("scroll_id",searchResponse.getScrollId());
        resultMap.put("content",esDataBOS);
        resultMap.put("totalNum",totalNum);

        //clean scroll
       // ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
       // clearScrollRequest.addScrollId(scrollId);
       // ClearScrollResponse clearScrollResponse = null;
       // try {
       //     clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
       // } catch (IOException e) {
        //    e.printStackTrace();
       // }
       // boolean succeeded = clearScrollResponse.isSucceeded();
        //logger.info("succeeded={}", succeeded);
        logger.info("succeeded={}", "完毕");
        return resultMap;
    }
}
