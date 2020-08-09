import re
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import as_completed

from ahocorapy.keywordtree import KeywordTree
from requests_futures.sessions import FuturesSession

SITE = 'http://admlist.ru/'


def get_page(url):
    return urllib.request.urlopen(url, timeout=50).read().decode()


def upml_list():
    result = []
    with open('upml_names.txt', 'r') as f:
        for line in f.readlines():
            result.append(line[:-1])
    return result


def univ_list():
    main_page = get_page(SITE)
    result = (re.findall(r'=[\w-]*/', s)[0][1:] for s in re.findall(r'href=[\w-]*/index.html>[\w-]*<', main_page))
    return result


def spec_list(univ_page):
    result = [re.findall(r'=[0-9a-f]*.html', s)[0][1:] for s in re.findall(r'href=[0-9a-f]*.html>.*?', univ_page)]
    return result


def get_spec_name(spec_page):
    result = re.search('<h1><center><a href=index.html>.*?</center></h1>', spec_page).group(0)[31:-14]
    return result.replace('</a>', '')


def people_table(spec_page):
    return re.search('<table class="tableFixHead">.*', spec_page, flags=re.DOTALL).group(0)


def line_content(line):
    prop = line.split('</td><td>')
    for i in range(len(prop)):
        prop[i] = prop[i][3:-4] if prop[i][:2] == '<b' else prop[i]
    name = prop[3]
    orig = " +" if prop[4] == 'Да' else ''
    comp_type = prop[5]
    return (name, comp_type, orig)


def name_kwtree(name_list):
    result = KeywordTree(case_insensitive=True)
    for name in name_list:
        result.add(name)
    result.finalize()
    return result


def future_univ(s):
    result = []
    for univ_url in univ_list():
        result.append(s.get(SITE + univ_url + 'index.html'))
    return result


def future_spec(s, future_jobs_univ):
    result = []
    for resp in map(lambda x: x.result(), as_completed(future_jobs_univ)):
        univ_page = resp.content.decode()
        for spec_url in spec_list(univ_page):
            result.append(s.get(resp.url[:-11] + '/' + spec_url))
    return result


def get_line(pos, page):
    st, fin = pos, pos
    while page[st:st+4] != '<tr>':
        st -= 1
    while page[fin-5:fin] != '</tr>':
        fin += 1
    return page[st:fin]


def seek_people(kwtree):
    s = FuturesSession(max_workers=20)
    future_jobs_spec = future_spec(s, future_univ(s))
    result = defaultdict(list)
    for spec_page in map(lambda x: x.result().content.decode(), as_completed(future_jobs_spec)):
        spec_name = get_spec_name(spec_page)
        table = people_table(spec_page)
        found = kwtree.search_all(table)
        for person in found:
            content = line_content(get_line(person[1], table))
            result[content[0]] += [spec_name + ' ' + content[1] + content[2]]
    return result


def write(found):
    with open('upml.txt', 'w') as f:
        names = list(found.keys())
        names.sort()
        for name in names:
            f.write(name + '\n')
            for to in found[name]:
                f.write("   " + to + '\n')
            f.write('\n')

start = time.time()
found = seek_people(name_kwtree(upml_list()))
print("found {} people from list".format(len(found)))
write(found)
print("completed in", time.time() - start, 'seconds')
