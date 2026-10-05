#!/usr/bin/env python3
"""Summarizes benchmark results: the median of the rounds, for each producer count, rate and queue.

Usage: report.py [results.csv]
"""
import collections
import csv
import statistics
import sys

ORDER = ['lock', 'unbounded', 'bounded']

path = sys.argv[1] if len(sys.argv) > 1 else 'results.csv'
groups = collections.defaultdict(list)
for row in csv.DictReader(open(path)):
    if row['achieved'] in ('ERROR', 'TIMEOUT'):
        print(f"skipped: round {row['round']} {row['queue']} {row['producers']} producers at "
              f"{row['target']}: {row['achieved']}", file=sys.stderr)
        continue
    groups[(int(row['producers']), float(row['target']), row['queue'])].append(row)


def median(rows, column):
    return statistics.median(float(r[column]) for r in rows)


def rate_label(rate):
    return f'{rate / 1e6:g}M' if rate >= 1e6 else f'{rate / 1e3:g}k'


def key(group):
    producers, rate, queue = group
    return producers, rate, ORDER.index(queue) if queue in ORDER else len(ORDER), queue


columns = [
    ('achieved', 'ops/s', lambda v: f'{v / 1e6:.2f}M'),
    ('e2e_p50', 'e2e p50', lambda v: f'{v:,.1f}'),
    ('e2e_p99', 'e2e p99', lambda v: f'{v:,.0f}'),
    ('e2e_p999', 'e2e p99.9', lambda v: f'{v:,.0f}'),
    ('e2e_max', 'e2e max', lambda v: f'{v:,.0f}'),
    ('put_mean', 'put mean', lambda v: f'{v:,.0f}'),
    ('put_p99', 'put p99', lambda v: f'{v:,.0f}'),
    ('put_p999', 'put p99.9', lambda v: f'{v:,.0f}'),
    ('prod_cores', 'prod cores', lambda v: f'{v:.2f}'),
    ('cons_cores', 'cons cores', lambda v: f'{v:.2f}'),
]
rounds = max((len(rows) for rows in groups.values()), default=0)
print(f'Median of {rounds} rounds. Latencies: e2e in µs, put in ns.')
print(f"{'prod':>4} {'rate':>5} {'queue':<10} " + ' '.join(f'{title:>10}' for _, title, _ in columns))
for group in sorted(groups, key=key):
    producers, rate, queue = group
    rows = groups[group]
    print(f'{producers:>4} {rate_label(rate):>5} {queue:<10} '
          + ' '.join(f'{fmt(median(rows, column)):>10}' for column, _, fmt in columns))

print()
print('e2e p99.9 of each round, in µs')
for group in sorted(groups, key=key):
    producers, rate, queue = group
    print(f'{producers:>4} {rate_label(rate):>5} {queue:<10} '
          + ' '.join(f"{float(r['e2e_p999']):>10,.0f}" for r in groups[group]))
