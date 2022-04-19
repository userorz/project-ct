package com.ct.producer.bean;

public class Calllog {
    private String call1;
    private String call2;
    private String callTime;
    private String calldDuration;

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

    public String getCallTime() {
        return callTime;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public String getCalldDuration() {
        return calldDuration;
    }

    public void setCalldDuration(String calldDuration) {
        this.calldDuration = calldDuration;
    }

    public Calllog(String call1, String call2, String callTime, String calldDuration) {
        this.call1 = call1;
        this.call2 = call2;
        this.callTime = callTime;
        this.calldDuration = calldDuration;
    }

    public Calllog() {
    }

    @Override
    public String toString() {
        return call1 + "\t" + call2 + "\t" + callTime + "\t" + calldDuration;
    }
}
