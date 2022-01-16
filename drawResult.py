import matplotlib.pyplot as plt
import re

label = []
x = []
y = []
with open('kmeans_cluster_result.txt') as file:
    lines = file.readlines()
    for line in lines:
        arr = re.split(' |\t',line)
        label.append(int(arr[0]))
        x.append(float(arr[1]))
        y.append(float(arr[2]))
        
mark = ['or', 'ob', 'og', 'ok', '^r', '+r', 'sr', 'dr', '<r', 'pr']

for i in range(0, len(label)):
    plt.plot(x[i], y[i], mark[label[i]], markersize = 5 )

plt.show()