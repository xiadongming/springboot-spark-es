package com.example.demo.controller;


import com.alibaba.fastjson.JSON;
import com.example.demo.bo.ESDataBO;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Date;

@RestController
@RequestMapping("/es")
public class ESController {

    @Autowired
    RestHighLevelClient highLevelClient;


    /**
     * 创建索引
     */
    @RequestMapping(value = "/create", method = RequestMethod.GET)
    public Object createESData() {
        //IndexRequest indexRequest = new IndexRequest("索引","type","id");
        IndexRequest indexRequest = new IndexRequest("newsale", null, null);
        ESDataBO esDataBO = getESDataBO();
        String esDataBOJson = JSON.toJSONString(esDataBO);
        indexRequest.source(esDataBOJson, XContentType.JSON);
        try {
            IndexResponse index = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "successful";
    }

    private ESDataBO getESDataBO() {
        ESDataBO esDataBO = new ESDataBO();
        //esDataBO.setDate(new Date());
        esDataBO.setIp("127.0.0.1");
        esDataBO.setController("shuishui");
        esDataBO.setMethod("createMethod");
        esDataBO.setModule("newSale");
        esDataBO.setName("elasticsearch");
        esDataBO.setUserId(100L);
        esDataBO.setParams("params");
        return esDataBO;
    }

    /**
     * 普通的拼接条件 的查询
     */
    @RequestMapping(value = "/getTotal", method = RequestMethod.GET)
    public Object getESDataTotal() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        /** term只能精准匹配，且只能匹配到一个词，不能是复合词汇，复合词汇则匹配不到 */
        //boolQueryBuilder.must(QueryBuilders.termQuery("method", "createMethod"));
        //boolQueryBuilder.must(QueryBuilders.termsQuery("method", "createMethod"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return search;
    }

    /**
     * 普通的拼接条件 的查询
     */
    @RequestMapping(value = "/get", method = RequestMethod.GET)
    public Object getESData() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        /** term只能精准匹配，且只能匹配到一个词，不能是复合词汇，复合词汇则匹配不到 */
        //boolQueryBuilder.must(QueryBuilders.termQuery("method", "createMethod"));
        //boolQueryBuilder.must(QueryBuilders.termsQuery("method", "createMethod"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (null != search) {
            return search.getHits();
        }
        return null;
    }

    /**
     * 聚合查询 aggregation
     */
    @RequestMapping(value = "/getAggHits", method = RequestMethod.GET)
    public Object getESDataAggHits() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);

        /** sourceBuilder 进行聚合操作 */
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("moduleCount").field("module");
        sourceBuilder.aggregation(aggregation);


        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (null != search) {
            return search.getHits();
        }
        return search;
    }

    /**
     * 聚合查询 aggregation
     */
    @RequestMapping(value = "/getAgg", method = RequestMethod.GET)
    public Object getESDataAgg() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);

        /** sourceBuilder 进行聚合操作 */
        /** 已 module 进行group，moduleCount作为标识 */
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("moduleCount").field("module");
        /** 已 name 进行group，nameCount   ,和上边的互不影响，是平行关系 */
        TermsAggregationBuilder aggregation2 = AggregationBuilders.terms("nameCount").field("name");

        sourceBuilder.aggregation(aggregation);
        sourceBuilder.aggregation(aggregation2);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (null != search) {
            Aggregations aggregations = search.getAggregations();
            Terms moduleCount = aggregations.get("moduleCount");
            for (Terms.Bucket bucket : moduleCount.getBuckets()) {
                System.out.println("bucketKey= " + bucket.getKey() + ",docCount= " + bucket.getDocCount());
            }

            Terms nameCount = aggregations.get("nameCount");
            for (Terms.Bucket bucket : nameCount.getBuckets()) {
                System.out.println("bucketKey2= " + bucket.getKey() + ",docCount2= " + bucket.getDocCount());
            }
            // return moduleCount.getBuckets();
        }
        return null;
    }

    /**
     * 折叠查询 即去重查询  collapse
     */
    @RequestMapping(value = "/getColl", method = RequestMethod.GET)
    public Object getESDataAggColl() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        CollapseBuilder collapse = new CollapseBuilder("userId");

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        sourceBuilder.collapse(collapse);

        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (null != search) {
             return search.getHits();
        }
        return null;
    }


    /**
     * 折叠查询 即去重查询  collapse
     *            和 innerHit 结合使用
     */
    @RequestMapping(value = "/getCollHits", method = RequestMethod.GET)
    public Object getESDataAggCollHits() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        CollapseBuilder collapse = new CollapseBuilder("userId");
        InnerHitBuilder innerHit = new InnerHitBuilder("group_by");
        innerHit.setSize(1);
        collapse.setInnerHits(innerHit);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        sourceBuilder.collapse(collapse);

        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (null != search) {
            return search.getHits();
        }
        return null;
    }
    /**
     * 折叠查询 即去重查询  collapse
     *            和 innerHit 结合使用
     */
    @RequestMapping(value = "/getCollHits2", method = RequestMethod.GET)
    public Object getESDataAggCollHits2() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        CollapseBuilder collapse = new CollapseBuilder("userId");
        /**
         * es 父子文档 inner_hits可以将关联文档带出 默认只查3条 可以自己设置 size
         * */
        InnerHitBuilder innerHit = new InnerHitBuilder("group_by");
        innerHit.setSize(5);
        collapse.setInnerHits(innerHit);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        sourceBuilder.collapse(collapse);

        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (null != search) {
            return search.getHits();
        }
        return null;
    }


    /**
     * HITS 的查询
     * */


}
