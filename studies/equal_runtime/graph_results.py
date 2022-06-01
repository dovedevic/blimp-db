import matplotlib
import statistics
import math
import json
import os

import matplotlib.pyplot as plt

from src.configurations.bank_layout import AmbitBankLayoutConfiguration

study_name = "" or input("Enter the study name: ")
study_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), study_name)
result_set_file = os.path.join(study_dir, f"equal_result.json")
figure_output = os.path.join(study_dir, f"equal_result.png")
configuration_directory = os.path.join(study_dir, f"configuration.json")

if not os.path.exists(study_dir) or not os.path.exists(configuration_directory):
    print("no study found; use the setup script to generate one first")
    exit()

if not os.path.exists(result_set_file):
    print("no result set found; use the collect results script to generate one first")
    exit()

with open(result_set_file, 'r') as fp:
    runtime_results = json.load(fp)

configuration = AmbitBankLayoutConfiguration.load(configuration_directory)

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

runtime_values = [
    runtime_results["best_case_blimp_equal"],
    runtime_results["best_case_blimp_not_equal"],
    runtime_results["best_case_ambit_equal"],
    runtime_results["best_case_ambit_not_equal"],
    runtime_results["best_case_blimp_v_equal"],
    runtime_results["best_case_blimp_v_not_equal"]
]


def lat_to_thpt(lat):
    op_scale = 1e9  # GOps
    return configuration.total_records_processable / (lat * 1e-9) / op_scale


throughput_values = [lat_to_thpt(v) for v in runtime_values]

runtime_errors = [
    [
        0,
        0,
        runtime_values[2] - runtime_results["best_case_ambit_et_equal"],
        runtime_values[3] - runtime_results["best_case_ambit_et_not_equal"],
        runtime_values[4] - runtime_results["best_case_blimp_v_et_equal"],
        runtime_values[5] - runtime_results["best_case_blimp_v_et_not_equal"],
    ],
    [
        0,
        0,
        runtime_results["worst_case_ambit_et_equal"] - runtime_values[2],
        runtime_results["worst_case_ambit_et_not_equal"] - runtime_values[3],
        runtime_results["worst_case_blimp_v_et_equal"] - runtime_values[4],
        runtime_results["worst_case_blimp_v_et_not_equal"] - runtime_values[5],
    ]
]

throughput_errors = [
    [
        0,
        0,
        throughput_values[2] - lat_to_thpt(runtime_results["best_case_ambit_et_equal"]),
        throughput_values[3] - lat_to_thpt(runtime_results["best_case_ambit_et_not_equal"]),
        throughput_values[4] - lat_to_thpt(runtime_results["best_case_blimp_v_et_equal"]),
        throughput_values[5] - lat_to_thpt(runtime_results["best_case_blimp_v_et_not_equal"]),
    ],
    [
        0,
        0,
        lat_to_thpt(runtime_results["worst_case_ambit_et_equal"]) - throughput_values[2],
        lat_to_thpt(runtime_results["worst_case_ambit_et_not_equal"]) - throughput_values[3],
        lat_to_thpt(runtime_results["worst_case_blimp_v_et_equal"]) - throughput_values[4],
        lat_to_thpt(runtime_results["worst_case_blimp_v_et_not_equal"]) - throughput_values[5],
    ]
]

# matplotlib.rc('text', usetex=True)
matplotlib.rc('font', **{'family': 'linux libertine'})
fig, (rt_ax, ops_ax) = plt.subplots(1, 2, figsize=(13, 5))  # type: plt.Figure, (plt.Axes, plt.Axes)

rt_bars = rt_ax.bar(labels, runtime_values, yerr=runtime_errors, width=0.6, edgecolor='black', linewidth=1.5, color='red')

#rt_ax.set_xticks(labels, labels, rotation=45)
rt_ax.set_yscale('log')
rt_ax.set_title("Query Scan Runtime", fontweight="bold", fontsize=16)
rt_ax.set_xlabel("System Architecture", fontweight="bold", fontsize=14)
rt_ax.set_ylabel("Runtime (ns)", fontweight="bold", fontsize=14)
rt_ax.set_ylim(bottom=100)
rt_ax.spines['top'].set_visible(False)
rt_ax.spines['right'].set_visible(False)

ops_bars = ops_ax.bar(labels, throughput_values, yerr=throughput_errors, width=0.6, edgecolor='black', linewidth=1.5, color='green')

ops_ax.set_yscale('log')
ops_ax.set_title("Query Kernel Throughput", fontweight="bold", fontsize=16)
ops_ax.set_xlabel("System Architecture", fontweight="bold", fontsize=14)
ops_ax.set_ylabel("Throughput (GOps/s)", fontweight="bold", fontsize=14)
ops_ax.set_ylim(bottom=3e-3)
ops_ax.spines['top'].set_visible(False)
ops_ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig(figure_output)
plt.show()
