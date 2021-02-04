package com.gateway.controller.es;

import com.commodity.servcie.bo.ESDataBO;
import com.commodity.servcie.es.ElasticSearchService;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/es", method = {RequestMethod.GET, RequestMethod.POST})
public class ESController {

    @Reference
    private ElasticSearchService elasticSearchService;

    @RequestMapping(value = "/all")
    public Object getAllES() {
        List<ESDataBO> esDataBOS = elasticSearchService.selectAllES();
        return esDataBOS;
    }

    @RequestMapping(value = "/allScroll")
    public Object getAllESScroll(String fromIndex) {
        System.out.println("fromIndex= " + fromIndex);
        Map<String, Object> stringObjectMap = elasticSearchService.selectAllESScroll(fromIndex);
        return stringObjectMap;
    }


}
