package com.ct.producer.bean;

import com.ct.common.bean.Data;

public class Contact extends Data {
    private String name;
    private String tel;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public void setValue(Object value) {
        content = (String) value;
        String[] split = content.split("\t");
        setName(split[1]);
        setTel(split[0]);
    }

    public Object getValue() {
        return name + "," + tel;
    }

    @Override
    public String  toString() {
        return "Contact{" +
                "name='" + name + '\'' +
                ", tel='" + tel + '\'' +
                '}';
    }
}
