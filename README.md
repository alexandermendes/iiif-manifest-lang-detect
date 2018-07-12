# iiif-manifest-lang-detect

> A set of scripts for identifying language codes from OCR data linked to IIIF manifests.

Initially written to identify the language codes for British Library
collection items digitised by Google Books. It is also possible to
feed in alternative inputs (see below).

## Overview

A list of IIIF manifest URIs is given as the initial input. For each
manifest we iterate over its canvases and check the `seeAlso` key to identify
URIs to related a documents in the `text/plain` format; these should contain
our OCR data.

For each manifest, the OCR URIs identified above are then shuffled into a
random order and batched into groups. The proportional size of each group is
determined by the `THRESHOLD` variable. If `THRESHOLD` is set to 20 (as is
the default) then each batch will be 20% of the total number of OCR pages.

These groups are iterated over and for each the related set of OCRed pages
requested, the text combined and an attempt made to detect the language. If we
detect a language with >= 95% `CONFIDENCE` we store the related ISO 8601
two-letter language code against the item. If a lanugage code can't by
identified from the group we add the text from the next group and check again.

At the end of the process we should have identified, with >= 95% `CONFIDENCE`,
that a random sample of at least `THRESHOLD` percent of each book is written
in a particular language. If no language can be established for an item the
`xx` placeholder is used.

The results are added to a file called `success.csv`. Rows where errors are
encountered while parsing will be added to the `error.csv` file.

The `THRESHOLD` and `CONFIDENCE` can be modified by updating their values at
the top of the script.

## Requirements

Python >= 3.6
Ubuntu >= 14.04 (for Redis)

## Installation

```bash
# install dependencies
pip install -r requirements.txt

# install Redis
sudo apt-get install redis-server
```

## Usage

```bash
# queue tasks
python run.py

# run worker
arq run.py
```

By default, [/data/bl-gbooks.csv](/data/bl-gbooks.csv) will be used as
the input. To use an alternative list of IIIF manifest URIs pass the path
to a CSV file containing those URIs when running the script, like so:

```
python run.py /path/to/csv
arq run.py /path/to/csv
```

The CSV file must contain a column with the `HEADER` identified in
[settings.py](bin/settings.py) (default `Manifest-URI`).

## Concurrency

To speed things up run multiple workers by calling `arq run.py` from seperate
terminals.
