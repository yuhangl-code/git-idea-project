package com.yh.entity;

public class MyData {

    public Integer keyId;
    public long timestamp;

    public String etime;

    public double value;

    public MyData() {
    }

    public MyData(Integer keyId, long timestamp, String etime, double value) {
        this.keyId = keyId;
        this.timestamp = timestamp;
        this.etime = etime;
        this.value = value;
    }

    public Integer getKeyId() {
        return keyId;
    }

    public void setKeyId(Integer keyId) {
        this.keyId = keyId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEtime() {
        return etime;
    }

    public void setEtime(String etime) {
        this.etime = etime;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyData{" +
                "keyId=" + keyId +
                ", timestamp=" + timestamp +
                ", etime='" + etime + '\'' +
                ", value=" + value +
                '}';
    }
}
