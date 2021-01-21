package com.example.demo.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.update.UpdateResponse;

public class MyActionListener implements ActionListener<UpdateResponse> {

    @Override
    public void onResponse(UpdateResponse updateResponse) {
        System.out.println("updateAsync结果: "+String.valueOf(updateResponse));
    }

    @Override
    public void onFailure(Exception e) {
        System.out.println("updateAsync结果: "+String.valueOf(e.getMessage()));
    }
}
