package com.example.demo.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

public class MyActionListener2 implements ActionListener<BulkByScrollResponse> {
    @Override
    public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
        System.out.println("updateAsync= " + String.valueOf(bulkByScrollResponse));
    }

    @Override
    public void onFailure(Exception e) {
        e.printStackTrace();
        System.out.println("updateAsync= " + String.valueOf(e.getMessage()));
    }
}
