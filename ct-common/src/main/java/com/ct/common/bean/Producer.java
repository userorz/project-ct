package com.ct.common.bean;

import java.io.Closeable;

/**
 * 生产接口
 */
public interface Producer extends Closeable {

    public void setIn(DataIn in);
    public void setOut(DataOut out);

    /**
     * 生产数据
     */
    public void produce();
}
