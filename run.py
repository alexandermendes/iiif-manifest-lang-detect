#-*- coding: utf8 -*-

import pandas
import requests
from Queue import Queue
from threading import Thread
from langdetect import detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException


QUEUE = Queue()


def load_dataframe(path):
    """Load a CSV file into a dataframe."""
    df = pandas.read_csv(path)
    if 'Manifest-URI' not in df:
        raise ValueError('Manifest-URI column not in {}'.format(path))
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
    languages = {}
    for uri in ocr_uris:
        r = requests.get(uri)
        try:
            top = detect_langs(r.text)[0]
        except (IndexError, LangDetectException):
            continue

        if top.prob > 0.95:
            count = languages.get(top.lang, 0)
            count += 1
            languages[top.lang] = count
    return languages


def queue_tasks(df, n_threads):
    """Queue tasks as manifest URI with related OCR URIs."""
    manifest_uris = df['Manifest-URI'].tolist()
    for manifest_uri in manifest_uris[:1]:
        r = requests.get(manifest_uri)
        manifest = r.json()
        ocr_uris = get_ocr_uris(manifest)
        job = {
            'manifest_uri': manifest_uri,
            'ocr_uris': ocr_uris
        }
        QUEUE.put(job)


def worker():
    """Check languages for items in the queue and persist."""
    while True:
        task = QUEUE.get()
        ocr_uris = task['ocr_uris']
        langs = check_ocr(ocr_uris)
        print langs
        QUEUE.task_done()


def start_workers(n_threads):
    for i in range(n_threads):
        t = Thread(target=worker)
        t.daemon = True
        t.start()


def run():
    DetectorFactory.seed = 0
    df = load_dataframe('./data/bl-gbooks.csv')
    n_threads = 3
    start_workers(n_threads)
    queue_tasks(df, n_threads)
    QUEUE.join()


if __name__ == '__main__':
    run()
