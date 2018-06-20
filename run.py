#-*- coding: utf8 -*-
import sys
import csv
import pandas
import requests
import pycountry
from Queue import Queue
from threading import Thread
from random import shuffle
from langdetect import detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException


N_THREADS = 5
THRESHOLD = 20
CONFIDENCE = 95


def load_dataframe(path, header):
    """Load a CSV file into a dataframe."""
    df = pandas.read_csv(path, dtype=str)
    if header not in df:
        raise ValueError('Manifest-URI column not in {}'.format(path))

    # Drop rows with no manifest URI
    df.dropna(subset=[header], inplace=True)

    # Set index and drop duplicates
    n_dupes = len(df[df.duplicated(subset=header, keep=False)])
    if n_dupes:
        print('WARNING: {} duplicate manifest URIs dropped'.format(n_dupes))
        df.drop_duplicates(subset=header, inplace=True)
    df.set_index(header, inplace=True, verify_integrity=True, drop=False)
    return df


def get_ocr_uris(manifest):
    """Get the plain text OCR URIs from a manifest."""
    uris = []
    canvases = manifest['sequences'][0]['canvases']
    for canvas in canvases:
        try:
            uri = [item for item in canvas['seeAlso']
                   if item['format'] == 'text/plain'][0]['@id']
        except (KeyError, IndexError):
            continue
        uris.append(uri)
    return uris


def check_ocr(ocr_uris):
    """Detect languages in OCR data for a set of URIs."""
    languages = {}
    shuffle(ocr_uris)
    for uri in ocr_uris:
        r = requests.get(uri)
        try:
            top = detect_langs(r.text)[0]
        except (IndexError, LangDetectException):
            continue

        if top.prob >= CONFIDENCE / float(100):
            count = languages.get(top.lang, 0)
            count += 1
            if (100 * float(count)) / float(len(ocr_uris)) > THRESHOLD:
                return top.lang
            languages[top.lang] = count

    return max(languages, key=languages.get) if languages else None


def convert_lang_code(iso639_1_code):
    """Convert from two character to three character language codes."""
    try:
        return pycountry.languages.get(alpha_2=iso639_1_code).alpha_3
    except Exception:
        return None


def queue_read_tasks(q, df, header):
    """Queue tasks as manifest URI with related OCR URIs."""
    index = df.index.tolist()
    for i in index:
        row = df.loc[i].to_dict()
        if row.get('lang') and row.get('lang') != 'nan':
            continue

        r = requests.get(row[header])
        manifest = r.json()
        ocr_uris = get_ocr_uris(manifest)
        row['ocr_uris'] = ocr_uris
        q.put(row)


def input_worker(in_queue, out_queue):
    """Check languages for items in the queue and persist."""
    while True:
        task = in_queue.get()
        ocr_uris = task['ocr_uris']
        lang_code = check_ocr(ocr_uris)
        task['lang'] = convert_lang_code(lang_code)
        del task['ocr_uris']
        out_queue.put(task)
        in_queue.task_done()


def output_worker(out_queue, df, header, csv_path):
    """Update the CSV file with language codes."""
    processed = 0
    while True:
        task = out_queue.get()
        manifest_uri = task[header]
        df.at[manifest_uri, 'lang'] = task['lang']
        processed += 1
        if processed % 100 == 0 or out_queue.qsize() < 10:
            print('{} rows updated'.format(processed))
            df.to_csv(csv_path, index=False)
        out_queue.task_done()


def start_workers(in_queue, out_queue, df, header, csv_path):
    """Start workers."""
    for _ in range(N_THREADS):
        t = Thread(target=input_worker, args=(in_queue, out_queue,))
        t.daemon = True
        t.start()

    t = Thread(target=output_worker, args=(out_queue, df, header, csv_path, ))
    t.daemon = True
    t.start()


def run(csv_path):
    DetectorFactory.seed = 0
    header = 'Manifest-URI'
    df = load_dataframe(csv_path, header)
    in_queue = Queue()
    out_queue = Queue()
    start_workers(in_queue, out_queue, df, header, csv_path)
    queue_read_tasks(in_queue, df, header)
    in_queue.join()
    out_queue.join()


if __name__ == '__main__':
    try:
        run(sys.argv[1])
    except IndexError as e:
        run('./data/bl-gbooks.csv')
