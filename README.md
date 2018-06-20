# iiif-manifest-lang-detect

> A script for identifying language codes from OCR data linked to IIIF manifests.

Initially written to identify the language codes for the British Library
collection items digitised by Google Books, although it is possible to
feed in alternative inputs (see below).

For each IIIF manifest we check the `seeAlso` key for each canvases in an
attempt to locate OCR data in the `text/plain` format, returning the URIs for
all such content. These OCR URIs are then iterated over in a random order, the
text downloaded and an attempt made to detect the language. If we detect a
language with >= 95% `CONFIDENCE` we add the language code to a running count
of language codes detected for that item. In order to save on processing
time, if the count passes a `THRESHOLD` of 20% of the number of pages in the
book we make an assumption that this is most prevelant language in the book
and return the code. Otherwise, we wait until all pages have been checked and
return the code with the highest count. The `THRESHOLD` and `CONFIDENCE` can
be modifed by updating their values at the top of the script.

The results are added to a `lang` column in the original CSV file, as each
100 rows are processed. If the script is later run against the same CSV file
rows that already contain a lang code will be ignored.

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
