package com.commodity.service.impl;

import com.commodity.servcie.ComSelectService;
import org.apache.dubbo.config.annotation.Service;

@Service
public class ComSelectServiceImpl implements ComSelectService {
    @Override
    public Object selectComAll() {
        return "success";
    }
}
