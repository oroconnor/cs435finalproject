"""
Calculates the Pearson Coefficient between the water and air temperatures
from the dataset.

The data should be in TSV format with the following columns (in order):
    - station code
    - date
    - air temp
    - water temp
"""

import matplotlib.pyplot as plt
from math import sqrt
from sys import argv

AIR_COLUMN_INDEX = 2
WATER_COLUMN_INDEX = 3

if len(argv) < 2:
    print(f"Usage:\n\tpython {argv[0]} <input-file>")
    exit()

input_file = argv[1]

print(f"Reading file: '{input_file}'")

values = []
avg_air_temp = 0
avg_water_temp = 0

with open(input_file, "r") as file:
    for line in file:
        if len(line) < 1:
            continue

        columns = line.split()

        air_temp =  float(columns[AIR_COLUMN_INDEX])
        water_temp = float(columns[WATER_COLUMN_INDEX])

        values += [(air_temp, water_temp)]
        avg_air_temp += air_temp
        avg_water_temp += water_temp

count = len(values)

if count == 0:
    print("EMPTY FILE, NOTHING TO DO")
    exit()

avg_air_temp /= count
avg_water_temp /= count

x = 0
y = 0
z = 0

for air_temp, water_temp in values:
    x += (air_temp - avg_air_temp) * (water_temp - avg_water_temp)
    y += (air_temp - avg_air_temp) ** 2
    z += (water_temp - avg_water_temp) ** 2

r = x / sqrt(y * z)

print(f"Pearson Coefficient: {r}")

# Plotting the data
air_temps = []
water_temps = []

for air_temp, water_temp in values:
    air_temps.append(air_temp)
    water_temps.append(water_temp)


plt.figure(figsize=(8, 6))

plt.grid(True, linestyle='-', alpha=0.5, zorder=0)
plt.gca().set_facecolor('whitesmoke')

plt.scatter(air_temps, water_temps, c='#440469', s=100, edgecolors='black', alpha=0.7, zorder=2)

plt.title(f"Air vs. Water Temperature\nPearson Correlation coefficient (r) = {r:.2f}", fontsize=16, fontweight='bold')
plt.xlabel("Air Temperature (°C)", fontsize=14, fontweight='bold')
plt.ylabel("Water Temperature (°C)", fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()
