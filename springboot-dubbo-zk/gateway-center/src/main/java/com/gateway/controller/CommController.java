package com.gateway.controller;

import com.commodity.servcie.ComSelectService;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/gateway",method = {RequestMethod.GET,RequestMethod.POST})
public class CommController {

    @Reference
    private ComSelectService comSelectService;

    @RequestMapping(value = "/all")
    public Object getCommAll(){
        Object object = comSelectService.selectComAll();
        return object;
    }
}
