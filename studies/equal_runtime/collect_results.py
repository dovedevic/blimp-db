import json
import os

study_name = input("Enter the study name: ")
study_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), study_name)

if not os.path.exists(study_dir):
    print("no study found; use the setup script to generate one first")
    exit()

queries = [
    "blimp_equal",
    "blimp_not_equal",
    "ambit_equal",
    "ambit_et_equal",
    "ambit_not_equal",
    "ambit_et_not_equal",
    "blimp_v_equal",
    "blimp_v_et_equal",
    "blimp_v_not_equal",
    "blimp_v_et_not_equal",
]

cases = [
    "best_case",
    "worst_case"
]

print("Parsing runtime.sim files")
runtime_results = {}
for query in queries:
    for case in cases:
        result_file = os.path.join(study_dir, f"{case}_{query}.runtime.sim")
        if not os.path.exists(result_file):
            print(f"results for {case}_{query} are not found; run the simulate script first")
            exit()
        print(f"parsing {result_file}")
        with open(result_file, "r") as fp:
            # runtime: #####.######ns
            runtime_line = fp.readline()
        runtime_results[f"{case}_{query}"] = float(runtime_line.strip().split(" ")[1][:-2])

print("parsing complete, saving result set")
result_set_file = os.path.join(study_dir, f"equal_result.json")
result_tsv_file = os.path.join(study_dir, f"equal_result.tsv")
with open(result_set_file, 'w') as fp:
    json.dump(runtime_results, fp, indent=4)
with open(result_tsv_file, 'w') as fp:
    for k, v in runtime_results.items():
        fp.write(f"{k}\t{v}\n")
print(f"result set saved to {result_set_file} and {result_tsv_file}")
