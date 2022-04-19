package com.ct.analysis.kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisValue implements Writable {
    private String sumCall;
    private String sumDruation;
    public AnalysisValue() {
    }

    public AnalysisValue(String sumCall, String sumDruation) {
        this.sumCall = sumCall;
        this.sumDruation = sumDruation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sumCall);
        out.writeUTF(sumDruation);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sumCall = in.readUTF();
        sumDruation = in.readUTF();
    }

    public String getSumCall() {
        return sumCall;
    }

    public void setSumCall(String sumCall) {
        this.sumCall = sumCall;
    }

    public String getSumDruation() {
        return sumDruation;
    }

    public void setSumDruation(String sumDruation) {
        this.sumDruation = sumDruation;
    }
}
