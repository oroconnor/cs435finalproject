"""
Take an input file and converts the lines to TSV (Tab-separated values).
This fixes any issues where separators are inconsistent or spaces were used.
"""

from sys import argv

if len(argv) < 3:
    print(f"Usage:\n\tpython {argv[0]} <input-file> <output-file>")
    exit()

input_file = argv[1]
output_file = argv[2]

print(f"Processing '{input_file}'")

result = []

with open(input_file, "r") as in_file:
    for line in in_file:
        if len(line) < 1:
            continue

        cells = line.split()

        result += ["\t".join(cells)]

content = "\n".join(result) 

print(f"Writing content to '{output_file}'")

with open(output_file, "w") as out_file:
    out_file.write(content)
