#-*- coding: utf-8 -*-
"""
A script to collate the results of run.py with the original CSV.
"""
import sys
import click
import pandas

import settings


def lookup_lang(manifest_uri, lang_df):
    try:
        return lang_df.loc[manifest_uri].lang
    except KeyError:
        return None


@click.command()
@click.option('--path', default='./data/bl-gbooks.csv', help='Path to CSV.')
def run(path):
    print('This might take a while...')
    main_df = pandas.read_csv(path, dtype=str)
    lang_df = pandas.read_csv('success.csv', dtype=str)
    main_df.set_index(settings.HEADER, inplace=True, drop=False)
    lang_df.drop_duplicates(subset=[settings.HEADER], inplace=True)
    lang_df.set_index(settings.HEADER, inplace=True, drop=False,
                      verify_integrity=True)
    main_df['lang'] = main_df[settings.HEADER].apply(lookup_lang,
                                                     args=(lang_df,))
    main_df = main_df[pandas.notnull(main_df['lang'])]
    main_df.to_csv('out.csv', index=False)


if __name__ == "__main__":
    run()
