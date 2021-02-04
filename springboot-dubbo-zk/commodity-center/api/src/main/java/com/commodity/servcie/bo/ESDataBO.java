package com.commodity.servcie.bo;

import java.io.Serializable;
import java.util.Date;

public class ESDataBO implements Serializable {
    private String method;
    private String ip;
    private String params;
    private String controller;
    private String module;
    private String name;
    private Long userId;
    private Date date;

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getController() {
        return controller;
    }

    public void setController(String controller) {
        this.controller = controller;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "ESDataBO{" +
                "method='" + method + '\'' +
                ", ip='" + ip + '\'' +
                ", params='" + params + '\'' +
                ", controller='" + controller + '\'' +
                ", module='" + module + '\'' +
                ", name='" + name + '\'' +
                ", userId=" + userId +
                ", date=" + date +
                '}';
    }
}
