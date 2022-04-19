package com.ct.common.bean;

/**
 * 数据对象
 */
public abstract class Data implements Val{
    public String content;

    @Override
    public abstract void setValue(Object value);

    @Override
    public abstract Object  getValue();
}
