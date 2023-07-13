package com.yh.entity;

public class SensorReading {

    private String sensor;
    private Long sensor_id;
    private Double sensor_score;

    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    public Long getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(Long sensor_id) {
        this.sensor_id = sensor_id;
    }

    public Double getSensor_score() {
        return sensor_score;
    }

    public void setSensor_score(Double sensor_score) {
        this.sensor_score = sensor_score;
    }

    public SensorReading(String sensor, Long sensor_id, Double sensor_score) {
        this.sensor = sensor;
        this.sensor_id = sensor_id;
        this.sensor_score = sensor_score;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensor='" + sensor + '\'' +
                ", sensor_id=" + sensor_id +
                ", sensor_score=" + sensor_score +
                '}';
    }
}
