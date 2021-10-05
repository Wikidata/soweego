"""
This script creates a summary and graphs derived from the evaluation performances.

It needs to be executed inside the directory which contains the performance files
"""

import itertools
from glob import glob

import matplotlib.pyplot as plt
import seaborn as sns

sns.set()

import pandas as pd

df = pd.DataFrame(
    columns=[
        "Catalog",
        "Entity",
        "Model",
        "F1.Mean",
        "F1.STD",
        "Prec.Mean",
        "Prec.STD",
        "Recall.Mean",
        "Recall.STD",
    ]
)

files = glob("*_performance.txt")

# load performance data into dataframe
for f in files:
    catalog = f[: f.index("_")]

    et = f[f.index("_") + 1 :]
    entity = et[: et.index("_")]

    model = "_".join(et.split("_")[1:-1])

    cnts = open(f).read().split('\n')
    cnts = [x.replace(":", "").replace('\t', '') for x in cnts if x is not '']

    for x in range(len(cnts)):
        if " = " in cnts[x]:
            w = cnts[x]
            cnts[x] = float(w[w.index(' = ') + 3 :])

    cnts = [x for x in cnts if isinstance(x, float)]
    cnts = ['%.6f' % x for x in cnts]

    df = df.append(
        {
            "Catalog": catalog,
            "Entity": entity,
            "Model": model,
            "Prec.Mean": cnts[0],
            "Prec.STD": cnts[1],
            "Recall.Mean": cnts[2],
            "Recall.STD": cnts[3],
            "F1.Mean": cnts[4],
            "F1.STD": cnts[5],
        },
        ignore_index=True,
    )

# Print table with performances
df = df.sort_values(by=['Model', 'Catalog'])
print(df.to_csv(index=False))

#####
# Draw graphics
for_graph = df.copy(True)
for_graph["Catalog/Entity"] = for_graph["Catalog"] + "/" + for_graph["Entity"]
for_graph["F1.Mean"] = for_graph["F1.Mean"].astype(float)
for_graph["Prec.Mean"] = for_graph["Prec.Mean"].astype(float)
for_graph["Recall.Mean"] = for_graph["Recall.Mean"].astype(float)

for_graph = for_graph.sort_values("Catalog/Entity")

# F1 Graph
g = sns.catplot(
    x="Model", y="F1.Mean", hue="Catalog/Entity", data=for_graph, kind="bar"
)
g.ax.set_yscale("log")
g.ax.set_title("F1 Scores")
g.set_ylabels("F1 Mean (Log Scale)")


# Precision Graph
g = sns.catplot(
    x="Model", y="Prec.Mean", hue="Catalog/Entity", data=for_graph, kind="bar"
)
g.ax.set_yscale("log")
g.ax.set_title("Precision Scores")
g.set_ylabels("Precision Mean (Log Scale)")

# Recall Graph
g = sns.catplot(
    x="Model", y="Recall.Mean", hue="Catalog/Entity", data=for_graph, kind="bar"
)
g.ax.set_yscale("log")
g.ax.set_title("Recall Scores")
g.set_ylabels("Recall Mean (Log Scale)")


# show graph for precision vs recall
cmaps = [
    'Blues',
    'Reds',
    'Purples_d',
    'BuGn_r',
    'GnBu_d',
    sns.cubehelix_palette(light=1, as_cmap=True),
]
fig, axes = plt.subplots(3, 2)
cat_models = iter(for_graph["Model"].unique())
for i, axi in enumerate(itertools.product([0, 1, 2], [0, 1])):
    cm = next(cat_models)
    print(cm, axi)
    axes[axi].set_title(cm)

    only_ent_model = for_graph[for_graph["Model"] == cm]
    sns.kdeplot(
        only_ent_model["Recall.Mean"],
        only_ent_model["Prec.Mean"],
        cmap=cmaps[i],
        ax=axes[axi],
    )
    axes[axi].axis('equal')


#####
# Print summaries of performances
summaries = []
for _, gg in df.groupby('Model'):
    # F1.Mean    F1.STD Prec.Mean  Prec.STD Recall.Mean Recall.STD
    summaries.append(
        {
            "Model": gg["Model"].values[0],
            "Average F1": "%.6f" % gg['F1.Mean'].astype(float).mean(),
            "Average F1.STD": "%.6f" % gg['F1.STD'].astype(float).mean(),
            "Average Prec": "%.6f" % gg['Prec.Mean'].astype(float).mean(),
            "Average Prec.STD": "%.6f" % gg['Prec.STD'].astype(float).mean(),
            "Average Recall": "%.6f" % gg['Recall.Mean'].astype(float).mean(),
            "Average Recall.STD": "%.6f"
            % gg['Recall.STD'].astype(float).mean(),
        }
    )

summaries = pd.DataFrame(summaries).sort_values(
    by="Average F1", ascending=False
)

print(summaries.to_csv(index=False))

######
# Summaries by catalog
summaries = []
for cla, cat in df.groupby("Catalog"):
    for _, gg in cat.groupby('Model'):
        # F1.Mean    F1.STD Prec.Mean  Prec.STD Recall.Mean Recall.STD
        summaries.append(
            {
                "Catalog": cla,
                "Model": gg["Model"].values[0],
                "Average F1": "%.6f" % gg['F1.Mean'].astype(float).mean(),
                "Average F1.STD": "%.6f" % gg['F1.STD'].astype(float).mean(),
                "Average Prec": "%.6f" % gg['Prec.Mean'].astype(float).mean(),
                "Average Prec.STD": "%.6f"
                % gg['Prec.STD'].astype(float).mean(),
                "Average Recall": "%.6f"
                % gg['Recall.Mean'].astype(float).mean(),
                "Average Recall.STD": "%.6f"
                % gg['Recall.STD'].astype(float).mean(),
            }
        )

summaries = pd.DataFrame(summaries).sort_values(
    by=["Catalog", "Average F1"], ascending=False
)

print(summaries.to_csv(index=False))
