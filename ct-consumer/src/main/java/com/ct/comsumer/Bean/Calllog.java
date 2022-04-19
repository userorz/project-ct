package com.ct.comsumer.Bean;

import com.ct.common.api.Column;
import com.ct.common.api.RowKey;
import com.ct.common.api.TableRef;

/**
 * 通话日志
 */
@TableRef("ct:calllog")
public class Calllog {
    @RowKey
    private String roeKey;

    @Column(family = "caller")
    private String call1;
    @Column(family = "caller")
    private String call2;
    @Column(family = "caller")
    private String calltime;
    @Column(family = "caller")
    private String duration;

    private String flag = "1";

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getRoeKey() {
        return roeKey;
    }

    public void setRoeKey(String roeKey) {
        this.roeKey = roeKey;
    }


    public Calllog(){

    }
    public Calllog(String data){
        String[] info = data.split("\t");
        call1 = info[0];
        call2 = info[1];
        calltime = info[2];
        duration = info[3];
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }
}
