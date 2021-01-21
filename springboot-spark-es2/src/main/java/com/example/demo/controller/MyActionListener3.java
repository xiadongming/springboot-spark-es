package com.example.demo.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

public class MyActionListener3 implements ActionListener<BulkResponse> {
    @Override
    public void onResponse(BulkResponse bulkItemResponses) {
        System.out.println("批量更新异步 bulkItemResponses= " + String.valueOf(bulkItemResponses));
    }

    @Override
    public void onFailure(Exception e) {
        System.out.println("批量更新异步 bulkItemResponses= " + String.valueOf(e.getMessage()));
    }
}
