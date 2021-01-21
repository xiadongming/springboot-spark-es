package com.example.demo.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;

public class MyCreateActionListener implements ActionListener<IndexResponse> {
    @Override
    public void onResponse(IndexResponse indexResponse) {
        System.out.println("异步创建索引 indexResponse= " + String.valueOf(indexResponse));
    }

    @Override
    public void onFailure(Exception e) {
        System.out.println("异步创建索引 indexResponse= " + String.valueOf(e.getMessage()));
    }
}
