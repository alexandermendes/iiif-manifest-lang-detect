# iiif-manifest-lang-detect

> A script for identifying language codes from OCR data linked to IIIF manifests.

While

Initially written to identify the language codes for British Library
collection items digitised by Google Books, although it is possible to
feed in alternative inputs (see below).

For each IIIF manifest we check the `seeAlso` key for each canvases in an
attempt to locate OCR data in the `text/plain` format, returning the URIs for
all such content. These OCR URIs are then shuffled into a random order and
batched into groups, where proportional size of each group is determined by
`THRESHOLD`. These groups are iterated over and for each the related set
of OCRed pages requested in parallel, the text combined and an attempt made
to detect the language. If we detect a language with >= 95% `CONFIDENCE` we
set this as the language code for that item. If not, we add the text from
the next group and check again. At the end of the process we should have
identified, with >= 95% `CONFIDENCE`, that a random sample of at least
`THRESHOLD` percent of each book is written in a particular language. The
`THRESHOLD` and `CONFIDENCE` can be modified by updating their values at the
top of the script.

The results are added to a `lang` column in the original CSV file. If the
script is later run against the same CSV file rows that already contain a lang
code will be ignored.

## Requirements

Python >=3.6

## Usage

```bash
# install dependencies
pip install -r requirements.txt

# run
python run.py
```

By default, [/data/bl-gbooks.csv](/data/bl-gbooks.csv) will be used as
the input. To use an alternative list of IIIF manifest URIs pass the path
to a CSV file containing those URIs when running the script, like so:

```
python run.py /path/to/csv
```

The CSV file must contain a column with the header `Manifest-URI`; all other
columns will be ignored.
