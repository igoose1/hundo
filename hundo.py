#!/usr/bin/env python3
#
# Copyright 2020 Ekaterina Tochilina
# Copyright 2020 Oskar Sharipov <oskarsh[at]riseup[dot]net>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""hundo.py - search enrollees in admlist.ru database

Usage:
  python hundo.py [--quiet] [--json | --raw]
  python hundo.py [--quiet] [--json | --raw] < file_with_names

Arguments:
  --quiet   Do not output in stderr.
  --json    Use json format for output.
  --raw     Use plain text for output."""

import json
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import _exit, cpu_count
from sys import argv, exit, stderr, stdin

import ahocorasick
from requests_futures.sessions import FuturesSession
from tqdm import tqdm

from rich.console import Console
from rich.table import Table

SITE = 'http://admlist.ru/'
TIMEOUT = 25
WORKERS = min(32, cpu_count() * 4)
# do not afraid of worker number
# they take web-requests so they must be lightweight

failed_universities = 0
failed_directions = 0
executor = ThreadPoolExecutor(max_workers=WORKERS)
session = FuturesSession(executor)
is_verbose = '--quiet' not in argv
progress_bar_config = dict(
    ascii=True,
    unit='page',
    mininterval=.3,
    dynamic_ncols=True,
    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]',
    disable=not is_verbose
)

def log(*args, **kwargs):
    if is_verbose:
        print(*args, **kwargs, file=stderr)


def contents(futures, with_url=False):
    for future in as_completed(futures):
        try:
            content = future.result().content.decode(errors='ignore')
        except Exception as e:
            log(e)
            content = None
        if with_url:
            yield content, future.result().url
        else:
            yield content


def get_page(url):
    future = session.get(url, timeout=TIMEOUT)
    return list(contents([future]))[0]


def name_list():
    result = []
    for line in map(lambda x: x.strip(), stdin.readlines()):
        if not line.startswith('#') and line:
            result.append(line)
    return result


UNIV_LIST_RE = re.compile(r'=[\w-]*/')
UNIV_LIST_LINK_RE = re.compile(r'href=[\w-]*/index.html>[\w-]*<')
def univ_list():
    main_page = get_page(SITE)
    result = []
    for s in re.findall(UNIV_LIST_LINK_RE, main_page):
        result.append(
            re.findall(UNIV_LIST_RE, s)[0][1:]
        )
    return result


SPEC_LIST_RE = re.compile(r'=[0-9a-f]*.html')
SPEC_LIST_LINK_RE = re.compile(r'href=[0-9a-f]*.html>.*?')
def spec_list(univ_page):
    result = []
    for s in re.findall(SPEC_LIST_LINK_RE, univ_page):
        result.append(re.findall(SPEC_LIST_RE, s)[0][1:])
    return result


SPEC_NAME_RE = re.compile(r'<h1><center><a href=index.html>.*?</center></h1>')
def spec_name(spec_page):
    result = re.search(SPEC_NAME_RE, spec_page).group(0)[31:-14]
    return result.replace('</a>', '')


def kwtree(asked_people):
    result = ahocorasick.Automaton()
    for name in asked_people:
        result.add_word(name, name)
    result.make_automaton()
    return result


def future_univ():
    result = []
    for u in univ_list():
        result.append(
            session.get(SITE + u + 'index.html')
        )
    return result


def future_spec(future_jobs_univ):
    global failed_universities
    result = []


    progress_bar = tqdm(**progress_bar_config, total=len(future_jobs_univ))
    for univ_page, url in contents(future_jobs_univ, with_url=True):
        if univ_page is None:
            failed_universities += 1
            progress_bar.update()
            continue
        for spec_url in spec_list(univ_page):
            result.append(
                session.get(url[: -len('index.html')] + spec_url)
            )
        progress_bar.update()
    progress_bar.close()
    return result


def line_content(line):
    prop = line.split('</td><td>')
    for i in range(len(prop)):
        prop[i] = prop[i][3:-4] if prop[i][:2] == '<b' else prop[i]
    name = prop[3]
    agreement = prop[4] == 'Да'
    comp_type = prop[5]
    return name, comp_type, agreement


def line(pos, page):
    st, fin = pos, pos
    while st >= 0 and page[st:st+4] != '<tr>':
        st -= 1
    while fin < len(page) and page[fin-5:fin] != '</tr>':
        fin += 1
    if st < 0 or fin >= len(page):
        return ''
    return page[st:fin]


def seek_people(asked_people):
    global failed_directions
    automaton_of_ak = kwtree(asked_people)

    university_futures = future_univ()
    log('looking for possible direction pages')
    future_jobs_spec = future_spec(university_futures)
    result = defaultdict(list)
    log('looking for people in direction pages')

    progress_bar = tqdm(**progress_bar_config, total=len(future_jobs_spec))
    for i, spec_page in enumerate(contents(future_jobs_spec)):
        if spec_page is None:
            failed_directions += 1
            progress_bar.update()
            continue
        found = automaton_of_ak.iter(spec_page)
        for end_index, _ in found:
            name, comp_type, agreement = line_content(line(end_index, spec_page))
            result[name].append(
                {
                    'spec': spec_name(spec_page),
                    'type': comp_type,
                    'agreement': agreement
                }
            )
        progress_bar.update()
    progress_bar.close()
    log('pages were parsed')
    return result


if __name__ == '__main__':
    if '--help' in argv:
        log(__doc__)
        exit(0)
    time_of_start = time.time()
    asked_people = name_list()
    try:
        found_people = seek_people(asked_people)
    except KeyboardInterrupt:
        log('force exit, wait')
        try:
            executor.shutdown(wait=False)
            log('bye')
            exit(130)
        except KeyboardInterrupt:
            log('you did a terrible thing')
            _exit(130)

    stat = 'completed in {:.2f} seconds\n' +\
        'found {:d} people ({:.1f}% of asked)\n' +\
        '{:d} university pages failed\n' +\
        '{:d} direction pages failed\n'
    log(
        stat.format(
            time.time() - time_of_start,
            len(found_people),
            len(found_people) / len(asked_people) * 100,
            failed_universities,
            failed_directions
        )
    )
    if '--json' in argv:
        print(json.dumps(found_people, sort_keys=True, ensure_ascii=False))
    elif '--raw' in argv:
        for name, directions in sorted(found_people.items()):
            print(name)
            for direction in directions:
                output_line = ' '.join([
                    direction['spec'],
                    direction['type'],
                    '+' if direction['agreement'] else '-'
                ])
                print('  ', output_line)
    else:
        console = Console()
        for name, directions in sorted(found_people.items()):
            table = Table(title=name, header_style='bold', expand=True)
            table.add_column('Программа')
            table.add_column('Тип', justify='center', style='magenta3')
            table.add_column('Согласие?', justify='center', style='cyan')
            for direction in directions:
                table.add_row(
                    direction['spec'],
                    direction['type'],
                    '[green]+[/green]' if direction['agreement'] else '-'
                )
            console.print(table)

