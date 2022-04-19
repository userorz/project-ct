package com.ct.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtil {

    /**
     * 将日期字符串按照指定格式解析
     * @param dataString
     * @param format
     * @return
     */
    public static Date parse(String dataString, String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date data = null;
        try {
            data = sdf.parse(dataString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * 将指定日期格式化为字符串
     * @param date
     * @param format
     * @return
     */
    public static String format(Date date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }
}
