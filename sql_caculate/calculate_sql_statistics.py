#! /usr/bin/env python
# -*- coding: utf-8 -*-

import Levenshtein

fp = open("his_impala_sql_fragments.txt")
logs = fp.readlines()
fp.close()

SQLs = [log.strip() for log in logs if len(log) > 10]
SQLs = [sql.split('\t') for sql in SQLs]

print "统计 SQL 出现的频率"
print "SQLs total number : "+str(len(SQLs))
SQLs_frequency = {}
num = 1
for sql in SQLs:
    if num % 1000 == 0:
        print num
    if sql[2] in SQLs_frequency:
        SQLs_frequency[sql[2]] += 1
    else:
        SQLs_frequency[sql[2]] = 1
    num += 1
fp = open("SQLs_frequency.txt", "w")
for sql in SQLs_frequency:
    fp.write(str(SQLs_frequency[sql])+"\t"+sql+"\n")
fp.close()
del SQLs_frequency

print "分析 SQL 的相似度"
print "SQLs total number : "+str(len(SQLs))
SQLs_similar = {}
num = 1
for sql in SQLs:
    if num % 1000 == 0:
        print num
    for s in SQLs_similar:
        if len(s) != len(sql[2]):
            continue
        if Levenshtein.ratio(s, sql[2]) > 0.9:
            SQLs_similar[s].append(sql[2])
            break
    else:
        SQLs_similar[sql[2]] = []
fp = open("SQLs_similar.txt", "w")
for sql in SQLs_similar:
    fp.write(sql+'\n')
    for s in SQLs_similar[sql]:
        fp.write(s+'\n')
    fp.write('---------------------------------------------------------------------------------------'+'\n')
fp.close()
