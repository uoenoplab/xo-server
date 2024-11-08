import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

np.set_printoptions(legacy='1.25')

experiment = 'skew'
link_speed= 25
osd_nodes = ['n12', 'n08', 'n09', 'n11']
obj_sizes = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]
versions = ['uniform', 'zipf0.9', 'zipf1.1']

fig, axs = plt.subplots(1, 3, figsize=(8, 8))

for rep_idx, ver in enumerate(versions):
    cpu_usages = []
    for node in osd_nodes:
        usage = []
        for s3_object_size in  obj_sizes:
            csv_path = '../' + str(link_speed) + 'Gbps/' + experiment + '_' + ver + '/' + str(s3_object_size) + 'kib/dstat_' + node + '.csv'
            data = pd.read_csv(csv_path, skiprows=5)
            trimmed_data = data[10:-10] # trim the first and last three seconds
            usage.append((trimmed_data['usr'] + trimmed_data['sys'] + trimmed_data['wai'] + trimmed_data['stl']).mean())
        cpu_usages.append(usage)

    x = np.arange(len(obj_sizes))
    bottom = np.zeros(len(obj_sizes))

    axs[rep_idx].set_title(ver, loc = 'left')
    axs[rep_idx].set_ylim(top=400)
    axs[rep_idx].set_xlabel('S3 Object Size [KiB]')
    axs[rep_idx].set_ylabel('CPU %')
    axs[rep_idx].set_xticks(x)
    axs[rep_idx].set_yticks([0, 100, 200, 300, 400])
    axs[rep_idx].set_xticklabels(obj_sizes, rotation=45, ha='right')
    axs[rep_idx].grid(axis = 'y')
    #axs[forward_index][repeat-1].legend()

    for idx, cpu_usage in enumerate(cpu_usages):
        axs[rep_idx].bar(x, cpu_usage, bottom=bottom, label=osd_nodes[idx])
        bottom = bottom + np.array(cpu_usage)

    #plt.show()

plt.tight_layout()
plt.savefig("cpu_usage.pdf")

        #print(','.join(map(str, cpu_usages)))

#for repeat in range(1,n_repeats+1):
for rep_idx, ver in enumerate(versions):
    print(ver)
    print('S3 Object Size [KiB]', end=',')
    print(','.join(map(str, osd_nodes)), end=',')
    print()
    for s3_object_size in  obj_sizes:
        cpu_usages = []
    #for repeat in range(1,n_repeats+1):
        cpu_usages.append(s3_object_size)
        for node in osd_nodes:
            csv_path = '../' + str(link_speed) + 'Gbps/' + experiment + '_' + ver + '/' + str(s3_object_size) + 'kib/dstat_' + node + '.csv'
            #print(csv_path)
            data = pd.read_csv(csv_path, skiprows=5)
            trimmed_data = data[10:-10] # trim the first and last three seconds
            concat_data = trimmed_data['usr'].astype(float) + trimmed_data['sys'].astype(float) + trimmed_data['wai'].astype(float) + trimmed_data['stl'].astype(float)
            cpu_usages.append(concat_data.mean())
        print(','.join(map(str, cpu_usages)))
