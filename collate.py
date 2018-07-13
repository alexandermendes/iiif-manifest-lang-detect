#-*- coding: utf-8 -*-
"""
A script to collate the results of run.py with the original CSV.
"""
import sys
import pandas

import settings


def lookup_lang(manifest_uri, lang_df):
    try:
        return lang_df.loc[manifest_uri].lang
    except KeyError:
        return None


def run(path):
    main_df = pandas.read_csv(path, dtype=str)
    lang_df = pandas.read_csv('success.csv', dtype=str)
    main_df.set_index(settings.HEADER, inplace=True, drop=False)
    lang_df.set_index(settings.HEADER, inplace=True, drop=False)
    main_df['lang'] = main_df[settings.HEADER].apply(lookup_lang,
                                                     args=(lang_df,))
    main_df.dropna(subset=['lang'], inplace=True)
    main_df.to_csv('out.csv')


if __name__ == "__main__":
    path = './data/bl-gbooks.csv'
    if len(sys.argv) > 2:
        path = sys.argv[1]
    run(path)
