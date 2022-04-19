package com.ct.common.util;

/**
 * 数字工具类
 */
public class NumberUtil {
    /**
     * 将数字格式化为字符串
     * @param num
     * @param length
     * @return
     */
    public static String format(int num,int length){
        String len = "%0" + Integer.toString(length) + "d";
        return String.format(len,num);
    }

}
