# Scripts

To prepare to execute these scripts, you should create Python virtual environments whenever possible.

```shell
cd scripts

# Create a virtual environment for Python
python -m venv venv

# Activate the virtual environment (Windows)
.\venv\Scripts\Activate
# Activate the virtual environment (Linux, MacOS)
source venv/bin/activate

# Install requirements
pip install --requirement requirements.txt

# Execute the file
python path/to/file.py
```

## Missing Data Check

You can either use the Jupyter notebook [`missingdatacheck.ipynb`](./missingdatacheck.ipynb) or you can use the [`missing_data_check.py`](./missing_data_check.py) file directly in Python.

Before you execute the file, be sure to edit the `files_path` at the top of the file with the path to the data files.

## Normalize TSV

Takes a DSV (Delimiter-separated values) file with arbitrary whitespace as the separators between columns and converts it to a TSV (Tab-separated values) file.

```shell
python normalize_tsv.py input.dsv output.tsv
```

### Pearson Coefficient

Calculates the [Pearson Coefficient](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient) of a DSV (Delimiter-separated values). The only requirements for the input file are that the columns being compared are the third and fourth columns in the values.
However, this can be adjusted by using the constants at the top of the script

```shell
python pearson_coefficient.py input.tsv
```
