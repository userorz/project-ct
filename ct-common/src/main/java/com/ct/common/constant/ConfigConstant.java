package com.ct.common.constant;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class ConfigConstant {
    private static Map<String,String> valueMap = new HashMap<String,String>();

    static {
        ResourceBundle ct = ResourceBundle.getBundle("ct");

        Enumeration<String> enums = ct.getKeys();
        while(enums.hasMoreElements()){
            String key = enums.nextElement();
            String value = ct.getString(key);
            valueMap.put(key,value);
        }
    }

    public static String getVal(String key){
        return valueMap.get(key);
    }
}
