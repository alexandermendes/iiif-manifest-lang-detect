#-*- coding: utf8 -*-
import sys
import tqdm
import json
import pandas
import click
import pycountry
from random import shuffle
from langdetect import detect_langs, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from tornado import ioloop, httpclient
from tornado.gen import multi


THRESHOLD = 20
CONFIDENCE = 95
HEADER = 'Manifest-URI'


def load_dataframe(path):
    """Load a CSV file into a dataframe."""
    df = pandas.read_csv(path, dtype=str)
    if HEADER not in df:
        raise ValueError('Manifest-URI column not in {}'.format(path))

    # Add empty lang column
    if 'lang' not in df:
        df['lang'] = None

    # Drop rows with no manifest URI
    df.dropna(subset=[HEADER], inplace=True)

    # Set index and drop duplicates
    n_dupes = len(df[df.duplicated(subset=HEADER, keep=False)])
    if n_dupes:
        print('WARNING: {} duplicate manifest URIs found'.format(n_dupes))
    df.set_index(HEADER, inplace=True, drop=False)
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


def detect_language(responses, total):
    """Detect language from OCR text."""
    text = ' '.join([str(r.body) for r in responses])
    try:
        langs = detect_langs(text)
    except LangDetectException:
        return None

    if not langs:
        return None

    top = langs[0]
    code = convert_lang_code(top.lang)
    if top.prob >= CONFIDENCE / float(100):
        return convert_lang_code(top.lang)


def get_chunks(seq, size):
    """Return a list as chunks."""
    if not size:
        return []
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


async def check_ocr(ocr_uris, http_client):
    """Detect languages from OCR data.

    Shuffle the OCR URIs into a random order then batch by the number of
    pages required to pass the THRESHOLD.
    """
    shuffle(ocr_uris)
    total = len(ocr_uris)
    batch_size = int((float(THRESHOLD) / 100) * total)
    responses = []
    for group in get_chunks(ocr_uris, batch_size):
        responses += await multi([http_client.fetch(uri) for uri in group])
        code = detect_language(responses, total)
        if code:
            return code
    return 'xxx'


def convert_lang_code(iso639_1_code):
    """Convert from two character to three character language codes."""
    try:
        return pycountry.languages.get(alpha_2=iso639_1_code).alpha_3
    except Exception:
        return None


def get_unchecked_index(df):
    """List index for rows in the dataframe without a language code."""
    no_langs_df = df.loc[~df.index.isin(df.dropna(subset=['lang']).index)]
    index = no_langs_df.index.tolist()
    return index


async def generate_tasks(df, http_client, offset=0):
    """Generate tasks as manifest URI with related OCR URIs.

    Manifests are requested in batches of 100.
    """
    index = get_unchecked_index(df)[offset:]
    for group in get_chunks(index, 100):
        responses = await multi([http_client.fetch(uri, raise_error=False)
                                 for uri in group])
        for r in responses:
            manifest_uri = r.request.url
            try:
                manifest = json.loads(r.body.decode('utf-8'))
            except (ValueError, AttributeError):
                update_dataframe(manifest_uri, 'Invalid manifest', df, 'error')
                continue
            ocr_uris = get_ocr_uris(manifest)
            yield manifest_uri, ocr_uris


async def process(manifest_uri, ocr_uris, df, http_client):
    """Check languages for items in the queue and persist."""
    lang_code = await check_ocr(ocr_uris, http_client)
    update_dataframe(manifest_uri, lang_code, df)


def update_dataframe(manifest_uri, value, df, field='lang'):
    """Update the dataframe."""
    df.at[manifest_uri, field] = value


def get_csv_path():
    """Get the input CSV path."""
    path = './data/bl-gbooks.csv'
    if len(sys.argv) > 2:
        path = sys.argv[1]
    return path


async def main(offset):
    """Run the script."""
    csv_path = get_csv_path()
    df = load_dataframe(csv_path)
    pbar = tqdm.tqdm(total=df[HEADER].count(),
                     initial=df['lang'].count() if 'lang' in df else 0)
    http_client = httpclient.AsyncHTTPClient()
    DetectorFactory.seed = 0
    task_gen = generate_tasks(df, http_client, offset)
    async for manifest_uri, ocr_uris in task_gen:
        await process(manifest_uri, ocr_uris, df, http_client)
        pbar.update(1)
        if pbar.n and pbar.n % 100 == 0:
            df.to_csv(csv_path, index=False)
    df.to_csv(csv_path, index=False)


@click.command()
@click.option('--offset', default=0)
def run(offset):
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(lambda: main(offset))


if __name__ == '__main__':
    run()
