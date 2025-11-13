public class DataPoint {
    private String dateTimeStamp;
    private double airTempC;
    private double waterTempC;
    private double waterTempCwAirRemoved;

    public DataPoint(String dateTimeStamp, double airTempC, double waterTempC, double waterTempCwAirRemoved) {
        this.dateTimeStamp = dateTimeStamp;
        this.airTempC = airTempC;
        this.waterTempC = waterTempC;
        this.waterTempCwAirRemoved = waterTempCwAirRemoved;
    }

    public String getDateTimeStamp() {
        return dateTimeStamp;
    }

    public double getAirTempC() {
        return airTempC;
    }

    public double getWaterTempC() {
        return waterTempC;
    }

    public double getWaterTempCwAirRemoved() {
        return waterTempCwAirRemoved;
    }
}

