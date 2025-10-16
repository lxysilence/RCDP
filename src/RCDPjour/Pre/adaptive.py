# import math
# import sys
# from collections import defaultdict
#
# def read_data(file_path):
#     data = defaultdict(dict)
#     with open(file_path, 'r',encoding="utf-8-sig") as f:
#         for line in f:
#             parts = line.strip().split(',')
#             site = parts[0]
#             time = int(parts[1])
#             volume = int(parts[2])
#             data[site][time] = volume
#     sites = list(data.keys())
#     return data, sites
#
# def precompute_site_stats(data, sites):
#     site_stats = {}
#     for site in sites:
#         volumes = [data[site][t] for t in range(97)]
#         prefix_sum = [0] * (97 + 1)
#         prefix_sq_non_zero = [0] * (97 + 1)
#         prefix_non_zero = [0] * (97 + 1)
#         for t in range(97):
#             prefix_sum[t+1] = prefix_sum[t] + volumes[t]
#             if volumes[t] > 0:
#                 prefix_sq_non_zero[t+1] = prefix_sq_non_zero[t] + volumes[t]**2
#                 prefix_non_zero[t+1] = prefix_non_zero[t] + 1
#             else:
#                 prefix_sq_non_zero[t+1] = prefix_sq_non_zero[t]
#                 prefix_non_zero[t+1] = prefix_non_zero[t]
#         V_max = max(volumes)
#         mu = sum(volumes) / 97
#         sigma = (sum((v - mu)**2 for v in volumes) / 97) ** 0.5
#         threshold = min(0.5 * V_max, mu + sigma)
#         site_stats[site] = {
#             'prefix_sum': prefix_sum,
#             'prefix_sq_non_zero': prefix_sq_non_zero,
#             'prefix_non_zero': prefix_non_zero,
#             'threshold': threshold,
#             'mu': mu,
#             'sigma': sigma,
#             'V_max': V_max
#         }
#     return site_stats
#
# def compute_violations(site_stats, sites, delta):
#     violations = defaultdict(int)
#     for i in range(-1, 96):
#         max_j = min(i + 6, 96)
#         # max_j =96
#         for j in range(i + 1, max_j + 1):
#             a = i + 1
#             b = j
#             count = 0
#             for site in sites:
#                 stats = site_stats[site]
#                 sum_v = stats['prefix_sum'][b+1] - stats['prefix_sum'][a]
#                 threshold = stats['threshold']
#                 cond3 = sum_v >= threshold
#                 if not cond3:
#                     count += 1
#                     continue
#                 length = b - a + 1
#                 if length == 1:
#                     continue
#                 n_non_zero = stats['prefix_non_zero'][b+1] - stats['prefix_non_zero'][a]
#                 zero_ratio = (length - n_non_zero) / length
#                 if n_non_zero == 0:
#                     continue
#                 sum_sq = stats['prefix_sq_non_zero'][b+1] - stats['prefix_sq_non_zero'][a]
#                 mean_nz = sum_v / n_non_zero
#                 if n_non_zero == 1:
#                     cv = 0.0
#                 else:
#                     var_nz = (sum_sq - n_non_zero * mean_nz ** 2) / (n_non_zero - 1)
#                     std_nz = var_nz ** 0.5
#                     cv = (std_nz / mean_nz) * (1 - zero_ratio)
#                 if cv >= delta:
#                     count += 1
#             violations[(i, j)] = count
#     return violations
#
# def dynamic_programming(violations, m):
#     INF = float('inf')
#     dp = [[INF] * (98) for _ in range(97)]
#     prev = [[(-1, 0)] * (98) for _ in range(97)]
#     for j in range(0, 6):
#     # for j in range(0, 96):
#         if (-1, j) in violations:
#             dp[j][1] = violations[(-1, j)]
#             prev[j][1] = (-1, 0)
#     for j in range(97):
#         for t in range(1, 97):
#             if dp[j][t] == INF:
#                 continue
#             for next_j in range(j + 1, min(j + 7, 97)):
#             # for next_j in range(j + 1, 97):
#                 if (j, next_j) not in violations:
#                     continue
#                 new_sum = dp[j][t] + violations[(j, next_j)]
#                 new_t = t + 1
#                 if new_sum < dp[next_j][new_t]:
#                     dp[next_j][new_t] = new_sum
#                     prev[next_j][new_t] = (j, t)
#     min_obj = INF
#     best_t = 0
#     for t in range(1, 98):
#         if dp[96][t] < INF:
#             obj = dp[96][t] / (m * t)
#             if obj < min_obj:
#                 min_obj = obj
#                 best_t = t
#     if min_obj == INF:
#         return [], 0.0
#     current_j, current_t = 96, best_t
#     path = []
#     while current_j != -1:
#         path.append(current_j)
#         current_j, current_t = prev[current_j][current_t]
#     path = [x for x in reversed(path) if x != -1]
#     return path, min_obj
#
# def main(file_path, delta=0.3):
#     data, sites = read_data(file_path)
#     m = len(sites)
#     site_stats = precompute_site_stats(data, sites)
#     violations = compute_violations(site_stats, sites, delta)
#     split_points, obj_value = dynamic_programming(violations, m)
#     print(f"Optimal split points: {split_points}")
#     print(f"Objective value: {obj_value}")
#
# if __name__ == "__main__":
#
#     main("D:\\traffic\\TripChain\\experientData\\time\\station10min")

import math
import sys
from collections import defaultdict

def read_data(file_path):
    data = defaultdict(dict)
    with open(file_path, 'r',encoding="utf-8-sig") as f:
        for line in f:
            parts = line.strip().split(',')
            site = parts[0]
            time = int(parts[1])
            volume = int(parts[2])
            data[site][time] = volume
    sites = list(data.keys())
    return data, sites

def precompute_site_stats(data, sites):
    site_stats = {}
    for site in sites:
        volumes = [data[site][t] for t in range(109)]
        prefix_sum = [0] * (109 + 1)
        prefix_sq_non_zero = [0] * (109 + 1)
        prefix_non_zero = [0] * (109 + 1)
        for t in range(109):
            prefix_sum[t+1] = prefix_sum[t] + volumes[t]
            if volumes[t] > 0:
                prefix_sq_non_zero[t+1] = prefix_sq_non_zero[t] + volumes[t]**2
                prefix_non_zero[t+1] = prefix_non_zero[t] + 1
            else:
                prefix_sq_non_zero[t+1] = prefix_sq_non_zero[t]
                prefix_non_zero[t+1] = prefix_non_zero[t]
        V_max = max(volumes)
        mu = sum(volumes) / 109
        sigma = (sum((v - mu)**2 for v in volumes) / 109) ** 0.5
        threshold = min(0.5 * V_max, mu + sigma)
        site_stats[site] = {
            'prefix_sum': prefix_sum,
            'prefix_sq_non_zero': prefix_sq_non_zero,
            'prefix_non_zero': prefix_non_zero,
            'threshold': threshold,
            'mu': mu,
            'sigma': sigma,
            'V_max': V_max
        }
    return site_stats

def compute_violations(site_stats, sites, delta):
    violations = defaultdict(int)
    for i in range(-1, 108):
        max_j = min(i + 6, 108)
        # max_j =96
        for j in range(i + 1, max_j + 1):
            a = i + 1
            b = j
            count = 0
            for site in sites:
                stats = site_stats[site]
                sum_v = stats['prefix_sum'][b+1] - stats['prefix_sum'][a]
                threshold = stats['threshold']
                cond3 = sum_v >= threshold
                if not cond3:
                    count += 1
                    continue
                length = b - a + 1
                if length == 1:
                    continue
                n_non_zero = stats['prefix_non_zero'][b+1] - stats['prefix_non_zero'][a]
                zero_ratio = (length - n_non_zero) / length
                if n_non_zero == 0:
                    continue
                sum_sq = stats['prefix_sq_non_zero'][b+1] - stats['prefix_sq_non_zero'][a]
                mean_nz = sum_v / n_non_zero
                if n_non_zero == 1:
                    cv = 0.0
                else:
                    var_nz = (sum_sq - n_non_zero * mean_nz ** 2) / (n_non_zero - 1)
                    std_nz = var_nz ** 0.5
                    cv = (std_nz / mean_nz) * (1 - zero_ratio)
                if cv >= delta:
                    count += 1
            violations[(i, j)] = count
    return violations

def dynamic_programming(violations, m):
    INF = float('inf')
    dp = [[INF] * (110) for _ in range(109)]
    prev = [[(-1, 0)] * (110) for _ in range(109)]
    for j in range(0, 6):
    # for j in range(0, 96):
        if (-1, j) in violations:
            dp[j][1] = violations[(-1, j)]
            prev[j][1] = (-1, 0)
    for j in range(109):
        for t in range(1, 109):
            if dp[j][t] == INF:
                continue
            for next_j in range(j + 1, min(j + 7, 109)):
            # for next_j in range(j + 1, 97):
                if (j, next_j) not in violations:
                    continue
                new_sum = dp[j][t] + violations[(j, next_j)]
                new_t = t + 1
                if new_sum < dp[next_j][new_t]:
                    dp[next_j][new_t] = new_sum
                    prev[next_j][new_t] = (j, t)
    min_obj = INF
    best_t = 0
    for t in range(1, 110):
        if dp[108][t] < INF:
            obj = dp[108][t] / (m * t)
            if obj < min_obj:
                min_obj = obj
                best_t = t
    if min_obj == INF:
        return [], 0.0
    current_j, current_t = 108, best_t
    path = []
    while current_j != -1:
        path.append(current_j)
        current_j, current_t = prev[current_j][current_t]
    path = [x for x in reversed(path) if x != -1]
    return path, min_obj

def main(file_path, delta=0.3):
    data, sites = read_data(file_path)
    m = len(sites)
    site_stats = precompute_site_stats(data, sites)
    violations = compute_violations(site_stats, sites, delta)
    split_points, obj_value = dynamic_programming(violations, m)
    print(f"Optimal split points: {split_points}")
    print(f"Objective value: {obj_value}")

if __name__ == "__main__":

    main("D:\\experiment\\RCDPjour\\commuteData\\RegionFlow\\part-00000")
