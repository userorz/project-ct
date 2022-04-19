package com.ct.common.constant;

import com.ct.common.bean.Val;

/**
 * 名称常量枚举类
 */
public enum Names implements Val {
    NAMESPACE("ct"),
    TOPIC("ct"),
    TABLE("ct:calllog"),//表名
    CF_CALLER("caller"),//主叫
    CF_CALLEE("callee"),//被叫
    CF_INFO("info");

    private String name;
    private Names(String name){
        this.name = name;
    }

    @Override
    public void setValue(Object value) {
        this.name = (String) value;
    }

    @Override
    public String getValue() {
        return name;
    }
}
