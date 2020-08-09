import re
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import as_completed
from os import cpu_count
from sys import stderr, stdin

from ahocorapy.keywordtree import KeywordTree
from requests_futures.sessions import FuturesSession
from tqdm import tqdm

SITE = 'http://admlist.ru/'
TIMEOUT = 25
WORKERS = min(32, cpu_count() + 5)

failed_universities = failed_directions = 0


def log(*args, **kwargs):
    print(*args, **kwargs, file=stderr)


def contents(futures, with_url=False):
    for future in as_completed(futures):
        try:
            content = future.result().content.decode()
        except Exception as e:
            log(e)
            content = None
        if with_url:
            yield content, future.result().url
        else:
            yield content


def get_page(session, url):
    future = session.get(url, timeout=TIMEOUT)
    return list(contents([future]))[0]


def upml_list():
    result = [line.strip() for line in stdin.readlines()]
    return result


UNIV_LIST_RE = re.compile(r'=[\w-]*/')
UNIV_LIST_LINK_RE = re.compile(r'href=[\w-]*/index.html>[\w-]*<')
def univ_list(session):
    main_page = get_page(session, SITE)
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


PEOPLE_TABLE_RE = re.compile(
    r'<table class="tableFixHead">.*', flags=re.DOTALL
)
def people_table(spec_page):
    return re.search(PEOPLE_TABLE_RE, spec_page).group(0)


def line_content(line):
    prop = line.split('</td><td>')
    for i in range(len(prop)):
        prop[i] = prop[i][3:-4] if prop[i][:2] == '<b' else prop[i]
    name = prop[3]
    orig = ' +' if prop[4] == 'Да' else ''
    comp_type = prop[5]
    return (name, comp_type, orig)


def kwtree(name_list):
    result = KeywordTree(case_insensitive=True)
    for name in name_list:
        result.add(name)
    result.finalize()
    return result


def future_univ(session):
    result = []
    for u in univ_list(session):
        result.append(
            session.get(SITE + u + 'index.html')
        )
    return result


def future_spec(session, future_jobs_univ):
    result = []
    for univ_page, url in contents(future_jobs_univ, with_url=True):
        if univ_page is None:
            failed_universities += 1
            continue
        for spec_url in spec_list(univ_page):
            result.append(
                session.get(url[: -len('index.html')] + spec_url)
            )
    return result


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
    session = FuturesSession(max_workers=WORKERS)
    _kwtree = kwtree(asked_people)

    university_futures = future_univ(session)
    log('looking for possible direction pages')
    future_jobs_spec = future_spec(session, university_futures)
    result = defaultdict(list)
    log('looking for people in direction pages')
    progress_bar = tqdm(
        total=len(future_jobs_spec),
        ascii=True,
        unit='page'
    )
    for spec_page in contents(future_jobs_spec):
        progress_bar.update()
        if spec_page is None:
            failed_directions += 1
            continue
        table = people_table(spec_page)
        found = _kwtree.search_all(table)
        if found is None:
            continue
        for _, shift in found:
            content = line_content(line(shift, table))
            result[content[0]].append(
                spec_name(spec_page) + ' ' + content[1] + content[2]
            )
    log('pages were parsed')
    return result


if __name__ == '__main__':
    time_of_start = time.time()
    asked_people = upml_list()
    found_people = seek_people(asked_people)
    for name, directions in sorted(found_people.items()):
        print(name)
        print(*('  ' + dir for dir in directions), sep='\n')
    stat = 'completed in {:.2f} seconds\n' +\
        'found {:d} people ({:.1f}% of asked)\n' +\
        '{:d} university pages failed\n' +\
        '{:d} direction pages failed\n'
    log(
        stat.format(
            time.time() - time_of_start,
            len(found_people),
            len(asked_people) / len(found_people) * 100,
            failed_universities,
            failed_directions
        )
    )
