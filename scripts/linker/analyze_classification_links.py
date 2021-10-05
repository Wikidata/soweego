"""
This script creates a summary and graphs derived from the classification results.

It needs to be executed inside the directory which contains the classification links files
"""

from glob import glob

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set(rc={"lines.linewidth": 0.5})

df = pd.DataFrame(
    columns=[
        "Catalog",
        "Entity",
        "Model",
        "Count",
        "Mean",
        "STD",
        "Min",
        "25%",
        "50%",
        "75%",
        "Max",
    ]
)

files = [x for x in glob("*_links.csv.gz") if "evaluation" not in x]
all_links = pd.DataFrame(
    columns=["Catalog", "Entity", "Model", "QID", "TID", "Prediction"]
)

continuous_classifiers = [
    "voting_classifier_soft",
    "logistic_regression",
    "multi_layer_perceptron",
    "naive_bayes",
    "single_layer_perceptron",
    "random_forest",
    "gated_classifier",
    "stacked_classifier",
]

# For every "links" file
for f in files:
    catalog = f[: f.index("_")]

    et = f[f.index("_") + 1 :]
    entity = et[: et.index("_")]

    model = "_".join(et.split("_")[1:-1])
    print(f)
    current_preds = pd.read_csv(
        f,
        header=None,
        names=['QID', 'TID', 'Prediction'],
        index_col=['QID', 'TID'],
    )

    current_preds["Catalog"] = catalog
    current_preds["Entity"] = entity
    current_preds["Model"] = model

    all_links = all_links.append(current_preds.reset_index(), sort=False)

    classfs = current_preds['Prediction'].describe().to_dict()

    for k in classfs.keys():
        classfs[k] = "%.6f" % classfs[k]

    classfs['count'] = classfs['count'][: classfs['count'].index(".")]

    # add the files statistics to the dataframe
    df = df.append(
        {
            "Catalog": catalog,
            "Entity": entity,
            "Model": model,
            "Count": classfs['count'],
            "Mean": classfs['mean'],
            "STD": classfs['std'],
            "Min": classfs['min'],
            "25%": classfs['25%'],
            "50%": classfs['50%'],
            "75%": classfs['75%'],
            "Max": classfs['max'],
        },
        ignore_index=True,
    )

# print all files' statistics
df = df.sort_values(by=['Model', 'Catalog'])
print(df.to_csv(index=False))

###############
# Get summaries of the statistics and print them
summaries = []
for _, gg in df.groupby('Model'):
    # F1.Mean    F1.STD Prec.Mean  Prec.STD Recall.Mean Recall.STD
    summaries.append(
        {
            "Model": gg["Model"].values[0],
            "Average Mean": "%.6f" % gg['Mean'].astype(float).mean(),
            "Average STD": "%.6f" % gg['STD'].astype(float).mean(),
            "Average 25%": "%.6f" % gg['25%'].astype(float).mean(),
            "Average 50%": "%.6f" % gg['50%'].astype(float).mean(),
            "Average 75%": "%.6f" % gg['75%'].astype(float).mean(),
            "Average Max": "%.6f" % gg['Max'].astype(float).mean(),
        }
    )

summaries = pd.DataFrame(summaries).sort_values(by="Average Mean", ascending=False)

print(summaries.to_csv(index=False))

#############
# Draw graph
# NOTE: Here we suppose that we'll only need 9 graphs (one for each entity).

for_graph = all_links.copy(True)
for_graph["Catalog/Entity"] = for_graph["Catalog"] + "/" + for_graph["Entity"]

f, axes = plt.subplots(3, 3, sharex=True)

c = i = j = 0
colors = sns.color_palette()
gg = None

models = sorted(list(for_graph["Model"].unique()))
colors = {mod: colors[i] for i, mod in enumerate(models)}

for cent in for_graph["Catalog/Entity"].unique():
    use_onlt = for_graph[for_graph["Catalog/Entity"] == cent]

    axes[i, j].set_title(cent)
    for mod in models:
        capl = use_onlt[use_onlt["Model"] == mod]

        print(f"Drawing cent {cent} , model {mod}, i{i} j{j}")

        if mod in continuous_classifiers:
            sns.kdeplot(
                capl["Prediction"],
                bw=0.009,
                shade=False,
                label=mod,
                ax=axes[i, j],
            )

        axes[i, j].set(yscale="log")
        c += 1
        c = c % len(colors)

    i += 1
    if i >= 3:
        i = 0
        j += 1
    if j >= 3:
        j = 0

import itertools

fbinary, axes_binary = plt.subplots(3, 3, sharex=True, sharey=True)

# suppose there are nine catalog/entities
cat_entites = iter(for_graph["Catalog/Entity"].unique())
for axi in itertools.product([0, 1, 2], [0, 1, 2]):
    ce = next(cat_entites)
    print(ce, axi)
    axes_binary[axi].set_title(ce)

    only_ent_catalog = for_graph[for_graph["Catalog/Entity"] == ce]
    data: pd.DataFrame = None
    for m in set(models) - set(continuous_classifiers):
        print(m)
        d = only_ent_catalog[only_ent_catalog["Model"] == m]

        dcounts: pd.DataFrame = (
            d["Prediction"].value_counts(normalize=True).reset_index()
        )

        dcounts = dcounts.rename(columns={"index": "Value", "Prediction": "Counts"})
        dcounts["Model"] = m
        dcounts["Catalog/Entity"] = ce

        if data is None:
            data = dcounts
        else:
            data = data.append(dcounts, ignore_index=True)

    sns.barplot(x="Value", y="Counts", data=data, hue="Model", ax=axes_binary[axi])
