package com.example.demo.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

public class MyActionListenerReIndex implements ActionListener<BulkByScrollResponse> {
    @Override
    public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        System.out.println("重建索引 bulkByScrollResponse= " + String.valueOf(bulkByScrollResponse));
    }

    @Override
    public void onFailure(Exception e) {
        System.out.println("重建索引 bulkByScrollResponse= " + String.valueOf(e.getMessage()));
    }
}
