# iiif-manifest-lang-detect

> A script for identifying language codes from OCR data linked to IIIF manifests.

Initially written to identify the language codes for the British Library
collection items digitised by Google Books, although it is possible to
feed in alternative inputs (see below).

## Usage

```bash
# install dependencies
pip install -r requirements.txt

# run
python run.py
```

By default, the [/data/bl-gbooks.csv](/data/bl-gbooks.csv) will be used as
the input. To use an alterntive list of IIIF manifest URIs pass the path
to a CSV file containing those URIs when running the script, like so:

```
python run.py /path/to/csv
```

The CSV file must contain a column with the header `Manifest-URI`; all other
columns will be ignored.
