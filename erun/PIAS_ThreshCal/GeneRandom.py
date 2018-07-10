#!/usr/bin/env python
# encoding: utf-8

import random

pareto_shape = 1.55
num_length = 1000000
list = []
cdf = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95,0.96, 0.97, 0.98, 0.99, 1]
fp = open('flow_pareto.txt', 'w')
fp_cdf = open('flow_pareto_cdf.txt', 'w')

num_sum = 0
for i in range(0, num_length):
    num = int(random.paretovariate(pareto_shape) * 1500 / 1460)
    fp.write(str(num) + '\n')
    list.append(num)
    print "random:", num
    num_sum += num
fp.close()

print "mean:", num_sum/num_length
fp_cdf.write("mean:" + str(num_sum/num_length) + '\n')
list.sort()
for i in range(0, len(cdf)):
    num = list[int(cdf[i]*num_length) - 1]
    fp_cdf.write(str(num) + ' ' + str(1) + ' ' + str(cdf[i]) + '\n')
    list.append(num)
    print "cdf_num:", num
    num_sum += num
fp_cdf.close()

