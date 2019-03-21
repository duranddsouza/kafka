package com.kafka.model;


public class Record {
    private String recordTime;
    private Double lattitude;
    private Double longitude;
    private Long mmsi;
    private String heading;
    private Double rateOfTurn;
    private Double speed;
    private Long status;

    public Record() {}

    public Record(String recordTime, Double lattitude, Double longitude, Long mmsi, String heading, Double rateOfTurn, Double speed, Long status) {
        this.recordTime = recordTime;
        this.lattitude = lattitude;
        this.longitude = longitude;
        this.mmsi = mmsi;
        this.heading = heading;
        this.rateOfTurn = rateOfTurn;
        this.speed = speed;
        this.status = status;
    }

    public Record(String [] data) {
        this.recordTime = data[0];
        this.longitude = Double.valueOf(data[1]);
        this.lattitude = Double.valueOf(data[2]);
        this.mmsi = Long.valueOf(data[3]);
        this.heading = data[4];
        this.rateOfTurn = data[5].isEmpty() ? null : Double.valueOf(data[5]);
        this.speed = Double.valueOf(data[6]);
        this.status = Long.valueOf(data[7]);
    }

    public String getRecordTime() {
        return recordTime;
    }

    public Double getLattitude() {
        return lattitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Long getMmsi() {
        return mmsi;
    }

    public String getHeading() {
        return heading;
    }

    public Double getRateOfTurn() {
        return rateOfTurn;
    }

    public Double getSpeed() {
        return speed;
    }

    public Long getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "Record{" +
                "recordTime=" + recordTime +
                ", lattitude=" + lattitude +
                ", longitude=" + longitude +
                ", mmsi=" + mmsi +
                ", heading=" + heading +
                ", rateOfTurn=" + rateOfTurn +
                ", speed=" + speed +
                ", status=" + status +
                '}';
    }
}
