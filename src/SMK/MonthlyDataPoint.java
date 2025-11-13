package SMK;

public class MonthlyDataPoint {
    private String dateTimeStamp;
    private double value;

    public MonthlyDataPoint(String dateTimeStamp, double value) {
        this.dateTimeStamp = dateTimeStamp;
        this.value = value;
    }

    public String getDateTimeStamp() {
        return dateTimeStamp;
    }

    public double getValue() {
        return value;
    }
}