package SMK;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import SMK.DataPoint;
import SMK.MonthlyDataPoint;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.Month;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class SMKutils {

    public ArrayList<String> seasonalMannKendall(List<DataPoint> dataPoints, String station) 
        throws NoSuchFieldException, IllegalAccessException {
        ArrayList<String> seasonalResults = new ArrayList<>();
        String[] fieldNames = { "airTempC", "waterTempC", "waterTempCwAirRemoved"};
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M-yyyy");
    
        for (String fieldName : fieldNames) {
            // Group MonthlyDataPoint objects by month
            Map<Month, List<MonthlyDataPoint>> monthlyDataPoints = new HashMap<>();
            List<Double> allSlopes = new ArrayList<>(); // for Sen estimate
            Set<Integer> uniqueYears = new HashSet<>();
            for (DataPoint dataPoint : dataPoints) {
                YearMonth yearMonth;
                // Split the date string by "-" and construct YearMonth
                String[] dateParts = dataPoint.getDateTimeStamp().split("-");
                int month = Integer.parseInt(dateParts[0].trim());
                int year = Integer.parseInt(dateParts[1].trim());
                LocalDate date = LocalDate.of(year, month, 1);
                uniqueYears.add(year);

                Month monthName = date.getMonth();

                // Extract field value
                Field field = DataPoint.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                Double value = (Double) field.get(dataPoint);
    
                if (value != null) {
                    List<MonthlyDataPoint> pointsForMonth = monthlyDataPoints.get(monthName);
                    if (pointsForMonth == null) {
                        pointsForMonth = new ArrayList<>();
                        monthlyDataPoints.put(monthName, pointsForMonth);
                    }
                    pointsForMonth.add(new MonthlyDataPoint(dataPoint.getDateTimeStamp(), value));
                }
            }
             // if the there is only data in one year 
             if (uniqueYears.size() < 2) {
                seasonalResults.add(String.format("%s, Insufficient data", fieldName));
                continue; // skip to the next fieldName
            }
    
            // Run Mann-Kendall for each month and accumulate S values
            Double totalS = 0.0;
            Double totalV = 0.0;
            int totalN = 0;
            double totalTau = 0.0;
            int seasonsWithData = 0;

            for (Month month : Month.values()) {
                List<MonthlyDataPoint> monthDataPoints = monthlyDataPoints.getOrDefault(month, new ArrayList<>());
    
                // Only run Mann-Kendall if there are enough points
                if (monthDataPoints.size() > 1) {
                    String result = mannKendall(monthDataPoints);
                    double monthS = parseS(result);
                    double monthV = parseV(result);
                    totalS += monthS;
                    totalV += monthV;
                    totalN += monthDataPoints.size();
                   
                     // calculate Tau for this month
                    double tauMonth = monthS / (monthDataPoints.size() * (monthDataPoints.size() - 1) / 2.0);
                    totalTau += tauMonth;
                    seasonsWithData++;
                    if (!monthDataPoints.isEmpty()) {
                        allSlopes.addAll(calculateSlopes(monthDataPoints));
                    }

                }


            }

            // Calculate final T, Z, and significance based on totalS
            // SMK versions
            double t = totalS / (totalN * (totalN - 1) / 2.0);
            double z = (totalS + 1) / (Math.sqrt(totalV));
            String psig = calcPsig(z);
            double tauSMK = totalTau / seasonsWithData;
            if (allSlopes.isEmpty()) {
                System.out.printf("station: %s, field: %s", station, fieldName);
            }
            double senSlope = calculateMedian(allSlopes);
    
            seasonalResults.add(String.format("%s, Total S=%.2f, T=%.2f, Z=%.2f, Sen=%.2f, %s", fieldName, totalS, tauSMK, z, senSlope, psig));
        }
        return seasonalResults;
    }

    public double parseS(String result) {
        String sPart = result.split(",")[0];
        // get the numeric part after "S=" and convert to double
        return Double.parseDouble(sPart.split("=")[1].trim());
    }

    public double parseV(String result) {
        String sPart = result.split(",")[1];
        // get the numeric part after "V=" and convert to double
        return Double.parseDouble(sPart.split("=")[1].trim());
    }

    public String mannKendall(List<MonthlyDataPoint> dataPoints) { 
        // Helper method for the SMK test (not just regular MK test)
        ArrayList<Map.Entry<String, Double>> variablePoints = new ArrayList<>();
        
        for (MonthlyDataPoint dataPoint : dataPoints) {
            double value = dataPoint.getValue();
            variablePoints.add(new SimpleEntry<>(dataPoint.getDateTimeStamp(), value));
        }
    
        // working with new date format: "M-yyyy"
        Collections.sort(variablePoints, Comparator.comparing(entry -> {
            String[] dateParts = entry.getKey().split("-");
            int month = Integer.parseInt(dateParts[0].trim());
            int year = Integer.parseInt(dateParts[1].trim());
            return LocalDate.of(year, month, 1);
        })); 
        
        ArrayList<Double> values = new ArrayList<>();
        for (Map.Entry<String, Double> entry : variablePoints) {
            values.add(entry.getValue());
        }
    
        int n = values.size();
        double s = sCalc(values);
        double v = seasonalVarianceWithTies(values);
     
        return String.format("S=%.2f, V=%.2f", s, v);
    }

    // for regular MannKendall - didn't wind up using
    public double calcZ(double s, double sd){
        double z;
        if (s == 0) {
			z = (s + 0)/sd;
		}
		else if (s > 0) {
			z = (s + 1)/sd;
		}
		else {
			z = (s - 1)/sd;
		}

        return z;
    }
    
    public String calcPsig(double z) {
        // calculate significance level of p value
        String psig;
        if (z < -1.96 || z > 1.96) {
            psig = "p value below .05";
        }
        else if (z < -2.576 || z > 2.576) {
            psig = "p value below .01";
        }
        else {
            psig = "p value above .05";
        }
        return psig;
    }

	public double sCalc(ArrayList<Double> values) {
		int s = 0;
		int length = values.size(); 
		for (int i=1; i<length; i++) {
			for (int j=0; j<i; j++) {
				double z =(double)values.get(i) - (double)values.get(j);
				s += (z > 0) ? 1 : (z < 0) ? -1 : 0;
			}
		}	
		return s;
	}

    public double seasonalVariance(List<Double> values) {
        // adjusting for ties. Source EPA 2011 page 22
        int n = values.size();
        if (n < 2) {
            return 0.00001;  // variance is zero if there’s only one or no data point
        }
        Double variance = (n /18.0) * (n - 1) * (2 * n + 5);
        return variance;
    }

    public double seasonalVarianceWithTies(List<Double> values) {
        //EPA paper doesn't really go into ties, but could impact I think
        int n = values.size();
        if (n < 2) {
            return 0.00001;  // to avoid division by zero elsewhere
        }
    
        // initial variance calculation without tie adjustment
        double variance = (n * (n - 1) * (2 * n + 5)) / 18.0;
    
        Map<Double, Integer> tieCounts = new HashMap<>();
        for (double value : values) {
            tieCounts.put(value, tieCounts.getOrDefault(value, 0) + 1);
        }
   
        double tieAdjustment = 0.0;
        for (int count : tieCounts.values()) {
            if (count > 1) { 
                tieAdjustment += count * (count - 1) * (2 * count + 5);
            }
        }
    
        variance = (variance - tieAdjustment / 18.0);
        return variance;
    }

   // helper method to calculate Sen slope estimate
   public List<Double> calculateSlopes(List<MonthlyDataPoint> monthDataPoints) {
       List<Double> slopes = new ArrayList<>();
   
       // sort by date using the new "M-yyyy" format
       monthDataPoints.sort(Comparator.comparing(mp -> {
           String[] dateParts = mp.getDateTimeStamp().split("-");
           int month = Integer.parseInt(dateParts[0].trim());
           int year = Integer.parseInt(dateParts[1].trim());
           return LocalDate.of(year, month, 1);
       }));
   
       // calculate slopes for all unique pairs within this month
       for (int i = 0; i < monthDataPoints.size(); i++) {
           for (int j = i + 1; j < monthDataPoints.size(); j++) {
               double deltaValue = monthDataPoints.get(j).getValue() - monthDataPoints.get(i).getValue();
            
               String[] datePartsJ = monthDataPoints.get(j).getDateTimeStamp().split("-");
               String[] datePartsI = monthDataPoints.get(i).getDateTimeStamp().split("-");
               int monthJ = Integer.parseInt(datePartsJ[0].trim());
               int yearJ = Integer.parseInt(datePartsJ[1].trim());
               int monthI = Integer.parseInt(datePartsI[0].trim());
               int yearI = Integer.parseInt(datePartsI[1].trim());
   
               LocalDate dateJ = LocalDate.of(yearJ, monthJ, 1);
               LocalDate dateI = LocalDate.of(yearI, monthI, 1);
   
               long deltaTime = dateJ.toEpochDay() - dateI.toEpochDay();
               
               if (deltaTime != 0) {
                   double slope = (deltaValue / deltaTime) * 365.25; // yearly basis
                   slopes.add(slope);
               }
           }
       }
       return slopes;
   }
 // helper method to calc Sen slope estimate
 public double calculateMedian(List<Double> slopes) {
    if (slopes.isEmpty()) {
        return 0.0;
    }
    Collections.sort(slopes);
    int size = slopes.size();
    if (size % 2 == 0) {
        return (slopes.get(size / 2 - 1) + slopes.get(size / 2)) / 2.0;
    } else {
        return slopes.get(size / 2);
    }
}


}
