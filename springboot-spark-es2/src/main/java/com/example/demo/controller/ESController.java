package com.example.demo.controller;


import com.alibaba.fastjson.JSON;
import com.example.demo.bo.ESDataBO;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/es")
public class ESController {

    @Autowired
    RestHighLevelClient highLevelClient;


    /**
     * 创建索引 同步
     */
    @RequestMapping(value = "/create", method = RequestMethod.GET)
    public Object createESData() {
        //IndexRequest indexRequest = new IndexRequest("索引","type","id");
        IndexRequest indexRequest = new IndexRequest("newsale", null, null);
        ESDataBO esDataBO = getESDataBO();
        String esDataBOJson = JSON.toJSONString(esDataBO);
        indexRequest.source(esDataBOJson, XContentType.JSON);
        IndexResponse index = null;
        try {
            index = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return index;
    }

    /**
     * 创建索引 异步
     */
    @RequestMapping(value = "/createAsync", method = RequestMethod.GET)
    public Object createESDataAsync() {
        //IndexRequest indexRequest = new IndexRequest("索引","type","id");
        IndexRequest indexRequest = new IndexRequest("newsale", null, null);
        ESDataBO esDataBO = getESDataBO();
        String esDataBOJson = JSON.toJSONString(esDataBO);
        indexRequest.source(esDataBOJson, XContentType.JSON);
        try {
            highLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new MyCreateActionListener());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "异步创建成功";
    }

    private ESDataBO getESDataBO() {
        ESDataBO esDataBO = new ESDataBO();
        //esDataBO.setDate(new Date());
        esDataBO.setIp("111.0.0.1");
        esDataBO.setController("高圆圆");
        esDataBO.setMethod("高圆圆");
        esDataBO.setModule("高圆圆");
        esDataBO.setName("高圆圆");
        esDataBO.setUserId(100L);
        esDataBO.setParams("高圆圆");
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
     * 模糊前缀匹配
     */
    @RequestMapping(value = "/getPreTotal", method = RequestMethod.GET)
    public Object getESDataPreTotal() {
        // GetRequest getRequest, RequestOptions options
        GetRequest getRequest = new GetRequest();
        // highLevelClient.get()
        //SearchRequest searchRequest, RequestOptions options
        /** 拼接条件，使用 boolQueryBuilder 进行条件的拼接 */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.prefixQuery("ip", "192"));
        //    boolQueryBuilder.must(QueryBuilders.termQuery("ip", "127.0.0.1"));
        /** term只能精准匹配，且只能匹配到一个词，不能是复合词汇，复合词汇则匹配不到 */
        //boolQueryBuilder.must(QueryBuilders.termQuery("method", "createMethod"));
        //boolQueryBuilder.must(QueryBuilders.termsQuery("method", "createMethod"));
        //    boolQueryBuilder.must(QueryBuilders.matchQuery("method", "createMethod"));

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
     * 普通的拼接条件 的查询
     * fetchSource
     */
    @RequestMapping(value = "/get2", method = RequestMethod.GET)
    public Object getESData2() {
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
        /** 是否返回_source字段,没有source相关字段，即没有检索到数据 */
        sourceBuilder.fetchSource(false);

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
     * 和 innerHit 结合使用
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
     * 和 innerHit 结合使用
     */
    @RequestMapping(value = "/getCollHits2", method = RequestMethod.GET)
    public Object getESDataAggCollHits2() {
        // GetRequest getRequest, RequestOptions options
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
     * 更新一条数据 update
     */
    @RequestMapping(value = "/updateOne")
    public Object updateOneESData() {
        /**
         * Upserts      如果文档还不存在，upsert元素的内容将作为新文档插入
         * docAsUpsert  true 更新请求检查发现文档不存在时(对应的ID文档),会新增 request.doc 中设置的文档;
         *              false doc的方式更新数据：更新请求检查发现文档不存在时(对应的ID文档)，会抛出ElasticsearchException [document missing]
         *                    upsert数据源时，当文档不存在时，会新增request.upsert内容设置的文档
         * detectNoop   true      如果检测到数据并未发生变化，则返回结果为noop（空操作）
         *              false，   每次操作都会执行，版本号将自增
         * */

        // UpdateRequest updateRequest, RequestOptions options
        UpdateRequest updateRequest = new UpdateRequest("newsale", null, "ByypHncBwlYi6tZy0NtX");

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("name", "刘亦菲");
        updateRequest.doc(sourceMap);

        //updateRequest.upsert(sourceMap);
        updateRequest.docAsUpsert(true);
        //updateRequest.detectNoop(true);

        UpdateResponse update = null;
        try {
            update = highLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return update;
    }

    /**
     * 更新数据 updateAsync
     */
    @RequestMapping(value = "/updateOneAsync")
    public Object updateOneESData2() {
        /**
         * Upserts      如果文档还不存在，upsert元素的内容将作为新文档插入
         * docAsUpsert  true 更新请求检查发现文档不存在时(对应的ID文档),会新增 request.doc 中设置的文档;
         *              false doc的方式更新数据：更新请求检查发现文档不存在时(对应的ID文档)，会抛出ElasticsearchException [document missing]
         *                    upsert数据源时，当文档不存在时，会新增request.upsert内容设置的文档
         * detectNoop   true      如果检测到数据并未发生变化，则返回结果为noop（空操作）
         *              false，   每次操作都会执行，版本号将自增
         * */
        // UpdateRequest updateRequest, RequestOptions options
        UpdateRequest updateRequest = new UpdateRequest("newsale", null, "EyypHncBwlYi6tZy2dsQ");

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("name", "高圆圆");
        updateRequest.doc(sourceMap);

        //updateRequest.upsert(sourceMap);
        updateRequest.docAsUpsert(true);
        //updateRequest.detectNoop(true);
        try {
            highLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, new MyActionListener());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "successful";
    }

    /**
     * 更新数据 updateByQuery
     */
    @RequestMapping(value = "/updateOneByQuery")
    public Object updateOneESData3() {

        // UpdateByQueryRequest updateByQueryRequest, RequestOptions options
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.prefixQuery("ip", "192"));
        //boolQueryBuilder.must(QueryBuilders.matchQuery("ip","192"));
        // boolQueryBuilder.must(QueryBuilders.matchQuery("method","createMethod"));

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest("newsale");
        /** 需要更新的数据 */
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("name", "高圆圆");
        ScriptType type = ScriptType.INLINE;
        String lang = "painless";
        /** idOrCode 是执行的命令  */
        updateByQueryRequest.setScript(new Script(type, lang, "ctx._source.name = params.name", paramsMap));

        /**   默认情况下，版本冲突会中止UpdateByQueryRequest进程  //设置版本冲突时继续 */
        updateByQueryRequest.setConflicts("proceed");
        /** 查询文档  */
        updateByQueryRequest.setQuery(boolQueryBuilder);
        /** 单次处理1000个文档 */
        //updateByQueryRequest.setBatchSize(1000);

        BulkByScrollResponse bulkByScrollResponse = null;
        try {
            bulkByScrollResponse = highLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulkByScrollResponse;
    }

    /**
     * 更新数据 updateByQueryAsync
     */
    @RequestMapping(value = "/updateOneByQueryAsync")
    public Object updateOneESData4() {

        // UpdateByQueryRequest updateByQueryRequest, RequestOptions options
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.prefixQuery("ip", "192"));
        //boolQueryBuilder.must(QueryBuilders.matchQuery("ip","192"));
        // boolQueryBuilder.must(QueryBuilders.matchQuery("method","createMethod"));

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest("newsale");
        /** 需要更新的数据 */
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("name", "高圆圆谁谁谁");
        paramsMap.put("controller", "美女美女");
        ScriptType type = ScriptType.INLINE;
        String lang = "painless";
        /** idOrCode 是执行的命令 ,更新多个字段，使用";"隔开 */
        updateByQueryRequest.setScript(new Script(type, lang, "ctx._source.module = params.name;ctx._source.controller = params.controller", paramsMap));

        /**   默认情况下，版本冲突会中止UpdateByQueryRequest进程  //设置版本冲突时继续 */
        updateByQueryRequest.setConflicts("proceed");
        /** 查询文档  */
        updateByQueryRequest.setQuery(boolQueryBuilder);
        /** 单次处理1000个文档 */
        //updateByQueryRequest.setBatchSize(1000);
        try {
            highLevelClient.updateByQueryAsync(updateByQueryRequest, RequestOptions.DEFAULT, new MyActionListener2());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "successful";
    }


    /**
     * bulk是  index/update/delete 操作
     * 批量创建 bulk
     * 批量删除
     * 批量修改
     */
    @RequestMapping(value = "/bulkAdd", method = RequestMethod.GET)
    public Object bulkOPerator() {
        // BulkRequest bulkRequest, RequestOptions options
        IndexRequest request = new IndexRequest("newsale", null, null);
        Map<String, Object> source1 = getSource1();
        String source1String = JSON.toJSONString(source1);
        request.source(source1String, XContentType.JSON);

        IndexRequest request2 = new IndexRequest("newsale", null, null);
        Map<String, Object> source2 = getSource2();
        String source2String = JSON.toJSONString(source2);
        request2.source(source2String, XContentType.JSON);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.add(request2);

        BulkResponse bulk = null;
        try {
            bulk = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulk;
    }

    private Map<String, Object> getSource2() {
        Map<String, Object> sourceMap = new HashMap();
        sourceMap.put("ip", "168.0.0.1");
        sourceMap.put("controller", "金银财宝");
        sourceMap.put("method", "金牛招财2");
        sourceMap.put("module", "newSale");
        sourceMap.put("name", "elasticsearch");
        sourceMap.put("userId", 100L);
        sourceMap.put("params", "params方法");
        return sourceMap;
    }

    private Map<String, Object> getSource1() {
        Map<String, Object> sourceMap = new HashMap();
        sourceMap.put("ip", "186.0.0.1");
        sourceMap.put("controller", "招财进宝");
        sourceMap.put("method", "金牛招财");
        sourceMap.put("module", "newSale过大年");
        sourceMap.put("name", "elasticsearch");
        sourceMap.put("userId", 300L);
        sourceMap.put("params", "params方法");
        return sourceMap;
    }


    /**
     * bulk是  index/update/delete 操作
     * 批量创建
     * 批量删除
     * 批量修改 bulk
     */
    @RequestMapping(value = "/bulkUpdate", method = RequestMethod.GET)
    public Object bulkOPerator2() {
        // BulkRequest bulkRequest, RequestOptions options
        Map<String, Object> source1 = getSource1();
        UpdateRequest updateRequest1 = new UpdateRequest("newsale", "BCypHncBwlYi6tZyQ9tz");
        updateRequest1.doc(source1);


        Map<String, Object> source2 = getSource2();
        UpdateRequest updateRequest2 = new UpdateRequest("newsale", "CCypHncBwlYi6tZy0Nv5");
        updateRequest2.doc(source2);


        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(updateRequest2);
        bulkRequest.add(updateRequest1);

        BulkResponse bulk = null;
        try {
            bulk = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulk;
    }

    /**
     * bulk是  index/update/delete 操作
     * 批量创建
     * 批量删除
     * 批量修改
     * 没有批量查询的操作
     */
    @RequestMapping(value = "/bulkQuery", method = RequestMethod.GET)
    public Object bulkOPerator3() {
        // BulkRequest bulkRequest, RequestOptions options
        Map<String, Object> source1 = getSource1();
        UpdateRequest updateRequest1 = new UpdateRequest("newsale", "BCypHncBwlYi6tZyQ9tz");
        updateRequest1.doc(source1);


        Map<String, Object> source2 = getSource2();
        UpdateRequest updateRequest2 = new UpdateRequest("newsale", "CCypHncBwlYi6tZy0Nv5");
        updateRequest2.doc(source2);


        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.prefixQuery("ip", "168"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);


        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);

        BulkRequest bulkRequest = new BulkRequest();
        //bulkRequest.add(searchRequest);

        BulkResponse bulk = null;
        try {
            bulk = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulk;
    }

    /**
     * 单个删除
     */
    @RequestMapping(value = "/esDelete", method = RequestMethod.GET)
    public Object deleteOperator() {
        // DeleteRequest deleteRequest, RequestOptions options
        DeleteRequest deleteRequest = new DeleteRequest("newsale", "1");

        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = highLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return deleteResponse;
    }


    /**
     * 单个删除
     * deleteByQuery
     */
    @RequestMapping(value = "/esDeleteByQuery", method = RequestMethod.GET)
    public Object deleteOperator2() {
        // DeleteByQueryRequest deleteByQueryRequest, RequestOptions options
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("method", "get"));

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest("newsale");
        deleteByQueryRequest.setQuery(boolQueryBuilder);
        /** 更新时版本冲突仍继续  */
        deleteByQueryRequest.setConflicts("proceed");
        /** 最大更新100条 */
        deleteByQueryRequest.setBatchSize(100);
        /** 并行 */
        deleteByQueryRequest.setSlices(2);
        /** 使用滚动参数来控制“搜索上下文”存活的时间  */
        deleteByQueryRequest.setScroll(TimeValue.timeValueMinutes(10));
        /** 设置超时时间 */
        deleteByQueryRequest.setTimeout(TimeValue.timeValueMinutes(2));
        /** 刷新索引 */
        deleteByQueryRequest.setRefresh(true);

        BulkByScrollResponse bulkByScrollResponse = null;
        try {
            bulkByScrollResponse = highLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulkByScrollResponse;
    }

    /**
     * bulk是  index/update/delete 操作
     * 批量创建
     * 批量删除  bulk
     * 批量修改
     */
    @RequestMapping(value = "/bulkDelete", method = RequestMethod.GET)
    public Object bulkOPerator4() {

        DeleteRequest deleteRequest1 = new DeleteRequest("newsale", null, "FCypHncBwlYi6tZy2tta");
        DeleteRequest deleteRequest2 = new DeleteRequest("newsale", null, "FiypHncBwlYi6tZy29u3");

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(deleteRequest1);
        bulkRequest.add(deleteRequest2);

        BulkResponse bulk = null;
        try {
            bulk = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulk;
    }

    /**
     * 批量操作 bulkAsync
     */
    @RequestMapping(value = "/bulkUpdateAsync", method = RequestMethod.GET)
    public Object bulkUpdateOPerator() {
        // BulkRequest bulkRequest, RequestOptions options
        Map<String, Object> source1 = getSource1();
        UpdateRequest updateRequest1 = new UpdateRequest("newsale", "FSypHncBwlYi6tZy29sI");
        updateRequest1.doc(source1);


        Map<String, Object> source2 = getSource2();
        UpdateRequest updateRequest2 = new UpdateRequest("newsale", "BSypHncBwlYi6tZyz9se");
        updateRequest2.doc(source2);


        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(updateRequest2);
        bulkRequest.add(updateRequest1);


        try {
            highLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new MyActionListener3());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "successful";
    }

    /**
     * 重建索引 ReIndex
     * 索引迁移
     */
    @RequestMapping(value = "/reIndex", method = RequestMethod.GET)
    public Object reIndexOperator() {
        //ReindexRequest reindexRequest, RequestOptions options
        // SearchRequest search, IndexRequest destination
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("newsale");
        reindexRequest.setDestIndex("newindex");

        /** 设置目标索引的版本类型 */
        reindexRequest.setDestVersionType(VersionType.EXTERNAL);
        /** 设置目标索引的操作类型 */
        reindexRequest.setDestOpType("create");
        /** 默认情况下，版本冲突会中止重新索引进程 */
        reindexRequest.setConflicts("proceed");

        // 通过添加查询限制文档，比如这里就是只对language字段词条是包括java的进行操作
        // 简单了来说就是进行文档的过滤
        //reindexRequest.setSourceQuery(new TermQueryBuilder("language", "java"));
        /** 超时时间 */
        reindexRequest.setTimeout(TimeValue.timeValueMinutes(10));
        /** 刷新索引 */
        reindexRequest.setRefresh(true);
        BulkByScrollResponse reindex = null;
        try {
            reindex = highLevelClient.reindex(reindexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reindex;
    }

    /**
     * 重建索引 ReIndex 异步
     */
    @RequestMapping(value = "/reIndexAsync", method = RequestMethod.GET)
    public Object reIndexOperator2() {
        //ReindexRequest reindexRequest, RequestOptions options
        // SearchRequest search, IndexRequest destination
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("newsale");
        reindexRequest.setDestIndex("newindex2");

        /** 设置目标索引的版本类型 */
        reindexRequest.setDestVersionType(VersionType.EXTERNAL);
        /** 设置目标索引的操作类型 */
        reindexRequest.setDestOpType("create");
        /** 默认情况下，版本冲突会中止重新索引进程 */
        reindexRequest.setConflicts("proceed");

        // 通过添加查询限制文档，比如这里就是只对language字段词条是包括java的进行操作
        // 简单了来说就是进行文档的过滤
        //reindexRequest.setSourceQuery(new TermQueryBuilder("language", "java"));
        /** 超时时间 */
        reindexRequest.setTimeout(TimeValue.timeValueMinutes(10));
        /** 刷新索引 */
        reindexRequest.setRefresh(true);
        try {
            highLevelClient.reindexAsync(reindexRequest, RequestOptions.DEFAULT,new MyActionListenerReIndex());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "successful";
    }
    /**
     * scroll 即游标查询
     * search scroll API是用于处理search request里面的大量数据的
     * 使用ES做分页查询有两种方法。
     *     一是配置search request的from,size参数。
     *     二是使用scroll API。搜索结果建议使用scroll API，查询效率高
     * */
    @RequestMapping(value = "/scroll",method = RequestMethod.GET)
     public Object scrollQuery(){
        // SearchRequest searchRequest, RequestOptions options

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("controller","金银财宝"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(5);
        /** 折叠即去重 */
       // CollapseBuilder collapse = new CollapseBuilder("controller");
       // sourceBuilder.collapse(collapse);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        /** 游标的存活时间 */
        searchRequest.scroll(TimeValue.timeValueSeconds(2L));
        SearchResponse search = null;
        try {
            search = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = search.getScrollId();
            System.out.println("scrollId= " + scrollId);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return search;
     }
     /**
      * 分页查询，from，pageSize
      * */

     /**
      * FetchSource 查询
      * */


    /**
     * HITS 的查询
     * */


}
