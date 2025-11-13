# Project Plan

## 1. Down-sample files

- Down-sample each file to monthly/weekly values by taking the average of all the temperatures for the period
- Output is a smaller file of data. We can reduce the data stored in each file to just air and water temperatures, depending on whether the file is a water station or atmosphere station
- Any nutrient stations can be dropped
- Any stations with insufficient data can be dropped

## 2. Join the two station types

- For each file, join the atmosphere sensor data with the water sensor data to create a new joined data entry for the period with both the water and the air temperatures
- Any region without sufficient data from both an atmosphere sensor and a water sensor can be dropped

## 3. Remove the impact of the air temperature on the water temperature

- We want to isolate the changes in water temperature not caused by the temperature in the air by performing a linear regression
  - [See this document for more information](https://www.rga78.com/blog/2015/9/23/intro-to-regression-part-8-multiple-regression-regressing-on-two-numeric-variables#:~:text=By%20%22regress%20out%22%2C%20we,isolation%22%2C%20so%20to%20speak)

## 4. Seasonal Mann-Kendall Test

- Perform the Seasonal Mann-Kendall Test on the entire dataset for the time span
- For each station, we need to compare historical data for the entire time span (e.g., we need to compare each January to the previous Januaries)

## 5. Analysis and Summaries

- Show top 10 trends, based on Z score
- Ouput counts and % of stations with 1) Significant Positive Trends 2) Significant Negative Trends 3) No significant trends
- Find the median Sen slope for each of the three variables for the whole set of stations
