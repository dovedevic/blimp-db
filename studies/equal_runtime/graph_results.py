import matplotlib
import statistics
import math
import json
import os

import matplotlib.pyplot as plt

study_name = "" or input("Enter the study name: ")
study_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), study_name)
result_set_file = os.path.join(study_dir, f"equal_result.json")
figure_output = os.path.join(study_dir, f"equal_result.png")

if not os.path.exists(study_dir):
    print("no study found; use the setup script to generate one first")
    exit()

if not os.path.exists(result_set_file):
    print("no result set found; use the collect results script to generate one first")
    exit()

with open(result_set_file, 'r') as fp:
    runtime_results = json.load(fp)

queries = [
    "ambit_equal",
    "ambit_et_equal",

    "ambit_not_equal",
    "ambit_et_not_equal",

    "blimp_equal",

    "blimp_not_equal",

    "blimp_v_equal",
    "blimp_v_et_equal",

    "blimp_v_not_equal",
    "blimp_v_et_not_equal",
]

cases = [
    "best_case",
    "worst_case"
]

##################################################

labels = [
    "BLIMP EQ",
    "BLIMP !EQ",
    "Ambit EQ",
    "Ambit !EQ",
    "BLIMP-V EQ",
    "BLIMP-V !EQ",
]

values = [
    statistics.mean([runtime_results["best_case_blimp_equal"], runtime_results["worst_case_blimp_equal"]]),
    statistics.mean([runtime_results["best_case_blimp_not_equal"], runtime_results["worst_case_blimp_not_equal"]]),
    statistics.mean([runtime_results["best_case_ambit_equal"], runtime_results["worst_case_ambit_equal"]]),
    statistics.mean([runtime_results["best_case_ambit_not_equal"], runtime_results["worst_case_ambit_not_equal"]]),
    statistics.mean([runtime_results["best_case_blimp_v_equal"], runtime_results["worst_case_blimp_v_equal"]]),
    statistics.mean([runtime_results["best_case_blimp_v_not_equal"], runtime_results["worst_case_blimp_v_not_equal"]]),
]

errors = [
    [
        0,
        0,
        values[2] - runtime_results["best_case_ambit_et_equal"],
        values[3] - runtime_results["best_case_ambit_et_not_equal"],
        values[4] - runtime_results["best_case_blimp_v_et_equal"],
        values[5] - runtime_results["best_case_blimp_v_et_not_equal"],
    ],
    [
        0,
        0,
        runtime_results["worst_case_ambit_et_equal"] - values[2],
        runtime_results["worst_case_ambit_et_not_equal"] - values[3],
        runtime_results["worst_case_blimp_v_et_equal"] - values[4],
        runtime_results["worst_case_blimp_v_et_not_equal"] - values[5],
    ]
]

# matplotlib.rc('text', usetex=True)
matplotlib.rc('font', **{'family': 'linux libertine'})
fig, ax = plt.subplots()  # type: plt.Figure, plt.Axes

plt.bar(labels, values, yerr=errors, width=0.6)
ax.set_xticks(labels, labels, rotation=45, fontweight="bold")
ax.set_yscale('log')

ax.set_title("Runtimes for Full Scan", fontweight="bold", fontsize=16)
ax.set_xlabel("System Architecture", fontweight="bold", fontsize=14)
ax.set_ylabel("Runtime (ns)", fontweight="bold", fontsize=14)

plt.tight_layout()

plt.savefig(figure_output)
plt.show()
