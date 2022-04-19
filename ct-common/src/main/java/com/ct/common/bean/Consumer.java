package com.ct.common.bean;

import java.io.Closeable;

/**
 * 消费者接口
 */

public interface Consumer extends Closeable {
    public void  consume() throws InterruptedException;
}
