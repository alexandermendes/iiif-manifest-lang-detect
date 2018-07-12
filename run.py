#-*- coding: utf8 -*-
import sys
import tqdm
import json
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

    # Add empty lang column
    if 'lang' not in df:
        df['lang'] = None

    # Drop rows with no manifest URI
    df.dropna(subset=[settings.HEADER], inplace=True)

    # Set index and drop duplicates
    n_dupes = len(df[df.duplicated(subset=settings.HEADER, keep=False)])
    if n_dupes:
        print('WARNING: {} duplicate manifest URIs found'.format(n_dupes))
    df.set_index(settings.HEADER, inplace=True, drop=False)
    return df


def get_csv_path():
    """Get the input CSV path."""
    path = './data/bl-gbooks.csv'
    if len(sys.argv) > 2:
        path = sys.argv[1]
    return path


def get_chunks(seq, size):
    """Return a list as chunks."""
    if not size:
        return []
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


class Shadow(Actor):
    async def startup(self):
        self.csv_path = get_csv_path()
        self.df = load_dataframe(self.csv_path)
        self.session = ClientSession(loop=self.loop)

    async def fetch(self, url, session):
        async with session.get(url) as response:
            return await response.read()

    @concurrent
    async def process(self, manifest_uri, save=False):
        async with self.session.get(manifest_uri) as response:
            content = await response.read()
            try:
                manifest = json.loads(content.decode('utf-8'))
            except Exception as e:
                self.update_dataframe(manifest_uri, 'Invalid manifest URI',
                                      'error')
                if save:
                    self.save()
                return
        await self.process_manifest(manifest_uri, manifest)
        if save:
            self.save()

    async def process_manifest(self, manifest_uri, manifest):
        ocr_uris = self.get_ocr_uris(manifest)
        lang_code = await self.check_ocr(ocr_uris)
        self.update_dataframe(manifest_uri, lang_code, 'lang')

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
        for group in get_chunks(ocr_uris, batch_size):
            ocr += await self.download_ocr(group)
            code = self.detect_language(ocr)
            if code:
                return code
        return 'xxx'

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

    def update_dataframe(self, manifest_uri, value, field):
        """Update the dataframe."""
        self.df.at[manifest_uri, field] = value

    def save(self):
        """Save the current dataframe to CSV."""
        self.df.to_csv(self.csv_path, index=False)

    async def shutdown(self):
        self.session.close()


class Worker(BaseWorker):
    timeout_seconds = 3600
    max_concurrent_tasks = settings.MAX_CONCURRENT_TASKS
    shadows = [Shadow]


async def run():
    """Run the script."""
    csv_path = get_csv_path()
    df = load_dataframe(csv_path)
    shadow = Shadow()
    unchecked_df = df.loc[~df.index.isin(df.dropna(subset=['lang']).index)]
    index = unchecked_df.index.tolist()
    pbar = tqdm.tqdm(total=df[settings.HEADER].count(),
                     initial=df['lang'].count() if 'lang' in df else 0)

    for group in get_chunks(index, 1000):
        [await shadow.process(manifest_uri) for manifest_uri in group[:-1]]
        await shadow.process(group[-1], True)
        pbar.update(len(group))
    await shadow.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


