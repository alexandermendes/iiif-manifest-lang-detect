#-*- coding: utf-8 -*-
import csv
import sys
import tqdm
import json
import time
import pandas
import asyncio
from random import shuffle
from langdetect import detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from aiohttp import ClientSession
from arq import Actor, BaseWorker, concurrent

import settings


def load_dataframe(path):
    """Load a CSV file into a dataframe."""
    print('Loading dataframe...')
    df = pandas.read_csv(path, dtype=str)
    if settings.HEADER not in df:
        raise ValueError('{0} column not in {1}'.format(settings.HEADER, path))

    # Drop rows with no manifest URI
    df.dropna(subset=[settings.HEADER], inplace=True)

    # Set index and drop duplicates
    n_dupes = len(df[df.duplicated(subset=settings.HEADER, keep=False)])
    if n_dupes:
        print('WARNING: {} duplicate manifest URIs found'.format(n_dupes))
    df.set_index(settings.HEADER, inplace=True, drop=False)
    return df


class Shadow(Actor):
    async def startup(self):
        success_file = open('success.csv', 'a')
        errors_file = open('errors.csv', 'a')
        self.success_writer = csv.writer(success_file)
        self.errors_writer = csv.writer(errors_file)
        self.session = ClientSession(loop=self.loop)
        self.n_processed = 0
        self.start_time = time.time()

    async def fetch(self, url, session):
        async with session.get(url) as response:
            return await response.read()

    @concurrent
    async def process(self, manifest_uri):
        async with self.session.get(manifest_uri) as response:
            content = await response.read()
            try:
                manifest = json.loads(content.decode('utf-8'))
            except Exception as e:
                self.errors_writer.writerow([manifest_uri])
                self.report()
                return
        lang_code = await self.process_manifest(manifest_uri, manifest)
        self.success_writer.writerow([manifest_uri, lang_code])
        self.report()

    async def process_manifest(self, manifest_uri, manifest):
        ocr_uris = self.get_ocr_uris(manifest)
        lang_code = await self.check_ocr(ocr_uris)
        return lang_code

    async def download_ocr(self, ocr_uris):
        tasks = []
        for uri in ocr_uris:
            task = asyncio.ensure_future(self.fetch(uri, self.session))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return ' '.join([str(r) for r in responses])


    def get_ocr_uris(self, manifest):
        """Get the OCR URIs from a manifest."""
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

    async def check_ocr(self, ocr_uris):
        """Detect languages from OCR data.

        Shuffle the OCR URIs into a random order then batch by the number of
        pages required to pass the THRESHOLD.
        """
        shuffle(ocr_uris)
        total = len(ocr_uris)
        batch_size = int((float(settings.THRESHOLD) / 100) * total)
        ocr = ''
        for group in self.get_chunks(ocr_uris, batch_size):
            ocr += await self.download_ocr(group)
            code = self.detect_language(ocr)
            if code:
                return code
        return 'xx'

    def get_chunks(self, seq, size):
        """Return a list as chunks."""
        if not size:
            return []
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def detect_language(self, ocr):
        """Detect language from OCR text."""
        try:
            langs = detect_langs(ocr)
        except LangDetectException:
            return None

        if not langs:
            return None

        top = langs[0]
        code = top.lang
        if top.prob >= settings.CONFIDENCE / float(100):
            return top.lang

    def report(self):
        self.n_processed += 1
        if self.n_processed % 100 == 0:
            now = time.time()
            diff = now - self.start_time
            per_s = format(self.n_processed / diff, '.2f')
            print('{0} PROCESSED : {1}/s'.format(self.n_processed, per_s))

    async def shutdown(self):
        self.session.close()


class Worker(BaseWorker):
    timeout_seconds = 3600
    max_concurrent_tasks = settings.MAX_CONCURRENT_TASKS
    shadows = [Shadow]


def get_csv_path():
    path = './data/bl-gbooks.csv'
    if len(sys.argv) > 2:
        path = sys.argv[1]
    return path



def count_csv(fn):
    """List items in first column of a CSV file."""
    try:
        f = open(fn, 'r')
    except FileNotFoundError:
        return []
    return [row[0] for row in csv.reader(f)]


async def run(path):
    """Run the script."""
    df = load_dataframe(path)
    shadow = Shadow()
    success = count_csv('success.csv')
    errors = count_csv('errors.csv')
    unchecked = [uri for uri in df.index if uri not in success + errors]
    for manifest_uri in tqdm.tqdm(unchecked):
        await shadow.process(manifest_uri)
    await shadow.close()


if __name__ == '__main__':
    path = './data/bl-gbooks.csv'
    if len(sys.argv) > 2:
        path = sys.argv[1]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(path))
