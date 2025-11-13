package SMK;

public class DataPoint {
    private String dateTimeStamp;
    private Double airTempC;
    private Double waterTempC;
    private Double waterTempCwAirRemoved;

    public DataPoint(String dateTimeStamp, Double airTempC, Double waterTempC, Double waterTempCwAirRemoved) {
        this.dateTimeStamp = dateTimeStamp;
        this.airTempC = airTempC;
        this.waterTempC = waterTempC;
        this.waterTempCwAirRemoved = waterTempCwAirRemoved;
    }

    public String getDateTimeStamp() {
        return dateTimeStamp;
    }

    public Double getAirTempC() {
        return airTempC;
    }

    public Double getWaterTempC() {
        return waterTempC;
    }

    public Double getWaterTempCwAirRemoved() {
        return waterTempCwAirRemoved;
    }
}