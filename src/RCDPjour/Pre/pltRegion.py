# pip install pyshp
# pip install osgeo
# conda install geopandas


import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
import time
import sys

import matplotlib.pyplot as plt

RegionData = []


width = 32
height = 20
dpi = 100

xysize = 13
size3 = -0.3
psize = 0.1
font = {'family': 'Times New Roman', 'weight': 'bold', 'size': psize}
fig = plt.figure(dpi=dpi, figsize=(width, height))

with open("D:\\code\\python\\sz_points_v3.csv", "r") as fin:
    lines = fin.readlines()
    for line in lines:
        if line[-1] == "\n":
            line = line[:-1]
        line_split = line.split(",")
        oneRegionData = []
        for pointsData in line_split:
            oneRegionData.append(float(pointsData))
        RegionData.append(oneRegionData)

for k in range(len(RegionData)):#绘制整个城市的区域
    y_arr = []
    x_arr = []
    for j in range(7, len(RegionData[k]), 2):
        # print(j)
        x_arr.append(float(RegionData[k][j]))
        y_arr.append(float(RegionData[k][j + 1]))

    plt.plot(x_arr, y_arr,color="k",linewidth=0.1)

    centerX = RegionData[k][1]
    centerY = RegionData[k][2]
    plt.annotate(text =str(), xy=(centerX, centerY),fontsize = 2, color="r", weight="ultralight")
# # targetRegionNum = [235,225,265,239,251,285,287,292,274,224,282,60,288,293,5,183]
# targetRegionNum = [451,449,450,429,460,459,357,358,356,515,532,285,286,514,513,422,439,416,415,414,
#                    530,413,412,339,336,341,468,310,453,452,441,454,455,442,440,443,428,427,425,424,
#                    426,433,431,423,432,430,444,437,435,434,438,436,417,475,474,421,419,420,418,483,
#                    480,482,479,481,486,484,485,526,355,527,359,528,529,471,462,461,448,445,447,446,
#                    351,354,353,352,457,334,531,361,360,365,363,364,362,385,382,384,383,349,347,346,
#                    350,348,458,456,335,333,332,330,331,340,338,337,545,512,544,284,2017,546,315,314,
#                    317,366,373,374,367,370,369,372,371,378,376,379,375,377,533,345,344,342,343,509,
#                    510,511,316,368,380,381,388,387,389,386,311,313,312,472,470,473,469,508,506,507,
#                    505,504,328,548,329,547,523,516,322,323,320,318,321,319,477,476,411,410,401,400,
#                    393,391,392,390,309,525,524,543,325,324,535,395,534,394,536,397,537,478,501,467,
#                    500,465,498,466,503,290,288,502,289,287,549,551,550,304,302,297,296,301,300,303,
#                    295,298,294,291,293,292,562,399,398,522,521,404,538,519,405,520,517,539,409,541,
#                    542,518,540,408,407,406,402,403,561,563,559,558,556,555,557,499,326,327,299,560,
#                    463,464,488,487,305,306,489,553,554,552,490,308,307,491,492,564,565,495,496,493,
#                    494,497,396]
# colors = ["green"]
# for k in range(len(targetRegionNum)):
#     i = targetRegionNum[k]
#     y_arr = []
#     x_arr = []
#
#     for j in range(7, len(RegionData[i]), 2):
#         # print(j)
#         x_arr.append(float(RegionData[i][j]))
#         y_arr.append(float(RegionData[i][j + 1]))
#     plt.fill(x_arr, y_arr, alpha=0.3, color=colors[0])
#     centerX = RegionData[i][1]
#     centerY = RegionData[i][2]
#     plt.scatter(centerX, centerY, color=colors[0], s=0.1)
#     plt.annotate(text=str(), xy=(centerX, centerY),fontsize = 2, color="r", weight="ultralight")

# colors = ["orange"]
# targetRegion = [527,359,351,352,354,457]
# for k in range(len(targetRegion)):
#     i = targetRegion[k]
#     y_arr = []
#     x_arr = []
#
#     for j in range(7, len(RegionData[i]), 2):
#         # print(j)
#         x_arr.append(float(RegionData[i][j]))
#         y_arr.append(float(RegionData[i][j + 1]))
#     plt.fill(x_arr, y_arr, alpha=0.3, color=colors[0])
#     centerX = RegionData[i][1]
#     centerY = RegionData[i][2]
#     plt.scatter(centerX, centerY, color=colors[0], s=0.1)
#     plt.annotate(text = str(i), xy=(centerX, centerY),fontsize = 1, color="r", weight="ultralight")

plt.savefig("D:\\code\\python\\plt.pdf")
#plt.show()
