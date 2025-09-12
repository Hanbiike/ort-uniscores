import os, re, json, glob
from bs4 import BeautifulSoup

REPORTS_DIR = 'online-htmls'
RANJIR_DIR = 'downloaded'


def compute_stats(scores):
    if not scores:
        return {"min": None, "max": None, "avg": None, "count": 0}
    return {
        "min": min(scores),
        "max": max(scores),
        "avg": round(sum(scores)/len(scores), 2),
        "count": len(scores)
    }


def parse_ranjir(report_id):
    for prefix in ('Ranjirk', 'Ranjirb'):
        path = os.path.join(RANJIR_DIR, f"personalcabinet_report_{prefix}_i-{report_id}_t-1.html")
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            rows = soup.select('tbody tr')
            green, white = [], []
            for tr in rows:
                cells = tr.find_all('td')
                if len(cells) < 4:
                    continue
                try:
                    score = int(cells[3].get_text(strip=True))
                except ValueError:
                    continue
                classes = tr.get('class', [])
                if 'coloredRow' in classes:
                    green.append(score)
                else:
                    white.append(score)
            return {
                'green_scores': green,
                'white_scores': white,
                'all_scores': green + white,
                'green': compute_stats(green),
                'white': compute_stats(white),
                'all': compute_stats(green + white)
            }
    return {
        'green_scores': [],
        'white_scores': [],
        'all_scores': [],
        'green': compute_stats([]),
        'white': compute_stats([]),
        'all': compute_stats([])
    }


def parse_reports():
    data = {}
    for path in glob.glob(os.path.join(REPORTS_DIR, 'reports*.html')):
        with open(path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        title = soup.title.get_text(strip=True) if soup.title else ''
        uni_name = title.split('|')[-1].strip() if '|' in title else title
        uni = data.setdefault(uni_name, {'directions': {}})
        for li in soup.select('li.card-item'):
            p = li.select_one('p.university-name')
            if not p:
                continue
            direction_name = re.sub(r'^\d+\.\s*', '', p.get_text(strip=True))
            dir_entry = uni['directions'].setdefault(direction_name, {})
            rows = li.select('div.rows')
            for r in rows[2:]:
                cells = r.select('div.cell')
                if len(cells) < 7:
                    continue
                code = cells[0].get_text(strip=True)
                pay_type = cells[2].get_text(strip=True).lower()
                quota_text = cells[4].get_text(strip=True)
                quota = int(re.sub(r'[^0-9]', '', quota_text) or 0)
                link = li.find('a', href=re.compile('Ranjir'))
                report_id = None
                if link:
                    m = re.search(r'i=(\d+)', link['href'])
                    if m:
                        report_id = m.group(1)
                ranjir = parse_ranjir(report_id) if report_id else parse_ranjir('0')
                dir_entry[pay_type] = {
                    'code': code,
                    'quota': quota,
                    'report_id': report_id,
                    'stats': {k: ranjir[k] for k in ['green','white','all']},
                    '_scores': {
                        'green': ranjir['green_scores'],
                        'white': ranjir['white_scores'],
                        'all': ranjir['all_scores']
                    }
                }
    return data


def aggregate_university(data):
    for uni, udata in data.items():
        agg = {}
        for direction in udata['directions'].values():
            for pay_type, info in direction.items():
                p = agg.setdefault(pay_type, {'quota': 0, 'green': [], 'white': [], 'all': []})
                p['quota'] += info['quota']
                p['green'].extend(info['_scores']['green'])
                p['white'].extend(info['_scores']['white'])
                p['all'].extend(info['_scores']['all'])
                # remove scores from direction to keep json small
                del info['_scores']
        udata['stats'] = {}
        for pay_type, info in agg.items():
            udata['stats'][pay_type] = {
                'quota': info['quota'],
                'green': compute_stats(info['green']),
                'white': compute_stats(info['white']),
                'all': compute_stats(info['all'])
            }
    return data


def main():
    data = parse_reports()
    data = aggregate_university(data)
    with open('stats.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print('Written stats.json')

if __name__ == '__main__':
    main()
