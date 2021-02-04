package com.commodity.servcie.es;

import com.commodity.servcie.bo.ESDataBO;

import java.util.List;
import java.util.Map;

public interface ElasticSearchService {

    public List<ESDataBO> selectAllES();
    public Map<String,Object> selectAllESScroll(String scrollId);

}
