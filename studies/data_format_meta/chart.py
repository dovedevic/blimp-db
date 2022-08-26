import json
import matplotlib

import matplotlib.pyplot as plt

from studies.data_format_meta.study import layout_formats


def plot_record_charts(save_name, studies):
    matplotlib.rc('font', **{'family': 'linux libertine'})
    fig, (record_ax, record_zoom_ax) = plt.subplots(1, 2, figsize=(13, 5))  # type: plt.Figure, (plt.Axes, plt.Axes)

    record_labels = [
        (layout_formats[6].name, "A"),
        (layout_formats[8].name, "AH"),
        (layout_formats[10].name, "B"),
        (layout_formats[11].name, "BH"),
        (layout_formats[14].name, "Bv"),
        (layout_formats[16].name, "BvH"),
        (layout_formats[18].name, "ABvH"),
    ]

    _ = record_ax.bar(
        [label[1] for label in record_labels],
        [studies[name[0]][0]['total_records_processable'] for name in record_labels],
        width=0.6,
        edgecolor='black',
        linewidth=1.5,
        color='red'
    )

    record_ax.set_xticks([label[1] for label in record_labels], [label[1] for label in record_labels], rotation=45)
    record_ax.set_title("Records Available Under Different Layouts", fontweight="bold", fontsize=16)
    record_ax.set_xlabel("Layouts", fontweight="bold", fontsize=14)
    record_ax.set_ylabel("Records Processable", fontweight="bold", fontsize=14)
    record_ax.spines['top'].set_visible(False)
    record_ax.spines['right'].set_visible(False)

    record_ax.axhline(
        y=studies[layout_formats[0].name][0]['total_records_processable'],
        linewidth=1, color='black', label='Total Maximum'
    )

    record_ax.axhline(
        y=studies[layout_formats[1].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Aligned Maximum'
    )

    record_ax.axhline(
        y=studies[layout_formats[4].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Bitweave-V Maximum'
    )

    _ = record_zoom_ax.bar(
        [label[1] for label in record_labels],
        [studies[name[0]][0]['total_records_processable'] for name in record_labels],
        width=0.6,
        edgecolor='black',
        linewidth=1.5,
        color='red'
    )

    record_zoom_ax.set_xticks([label[1] for label in record_labels], [label[1] for label in record_labels], rotation=45)
    record_zoom_ax.set_title("Records Available Under Different Layouts (zoomed)", fontweight="bold", fontsize=16)
    record_zoom_ax.set_xlabel("Layouts", fontweight="bold", fontsize=14)
    record_zoom_ax.set_ylabel("Records Processable", fontweight="bold", fontsize=14)
    record_zoom_ax.spines['top'].set_visible(False)
    record_zoom_ax.spines['right'].set_visible(False)
    record_zoom_ax.set_ylim(
        bottom=min([i for i in [studies[name[0]][0]['total_records_processable'] for name in record_labels]]) * .995,
        top=max([i for i in [studies[name[0]][0]['total_records_processable'] for name in record_labels]]) * 1.005
    )

    record_zoom_ax.axhline(
        y=studies[layout_formats[0].name][0]['total_records_processable'],
        linewidth=1, color='black', label='Total Maximum'
    )

    record_zoom_ax.axhline(
        y=studies[layout_formats[1].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Aligned Maximum'
    )

    record_zoom_ax.axhline(
        y=studies[layout_formats[4].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Bitweave-V Maximum'
    )

    record_ax.legend()
    plt.tight_layout()
    plt.savefig(f"{save_name}.records_chart.png")
    plt.show()


def plot_index_charts(save_name, studies):
    matplotlib.rc('font', **{'family': 'linux libertine'})
    fig, (index_ax, index_zoom_ax) = plt.subplots(1, 2, figsize=(13, 5))  # type: plt.Figure, (plt.Axes, plt.Axes)

    index_labels = [
        (layout_formats[7].name, "Ai"),
        (layout_formats[9].name, "AiH"),
        (layout_formats[12].name, "Bi"),
        (layout_formats[13].name, "BiH"),
        (layout_formats[15].name, "Bvi"),
        (layout_formats[17].name, "BviH"),
        (layout_formats[18].name, "ABviH"),
    ]

    _ = index_ax.bar(
        [label[1] for label in index_labels],
        [studies[name[0]][0]['total_records_processable'] for name in index_labels],
        width=0.6,
        edgecolor='black',
        linewidth=1.5,
        color='blue'
    )

    index_ax.set_xticks([label[1] for label in index_labels], [label[1] for label in index_labels], rotation=45)
    index_ax.set_title("Index Records Available Under Different Layouts", fontweight="bold", fontsize=16)
    index_ax.set_xlabel("Layouts", fontweight="bold", fontsize=14)
    index_ax.set_ylabel("Index Records Processable", fontweight="bold", fontsize=14)
    index_ax.spines['top'].set_visible(False)
    index_ax.spines['right'].set_visible(False)

    index_ax.axhline(
        y=studies[layout_formats[2].name][0]['total_records_processable'],
        linewidth=1, color='black', label='Total Maximum'
    )

    index_ax.axhline(
        y=studies[layout_formats[3].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Aligned Maximum'
    )

    index_ax.axhline(
        y=studies[layout_formats[5].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Bitweave-V Maximum'
    )

    _ = index_zoom_ax.bar(
        [label[1] for label in index_labels],
        [studies[name[0]][0]['total_records_processable'] for name in index_labels],
        width=0.6,
        edgecolor='black',
        linewidth=1.5,
        color='blue'
    )

    index_zoom_ax.set_xticks([label[1] for label in index_labels], [label[1] for label in index_labels], rotation=45)
    index_zoom_ax.set_title("Index Records Available Under Different Layouts (zoomed)", fontweight="bold", fontsize=16)
    index_zoom_ax.set_xlabel("Layouts", fontweight="bold", fontsize=14)
    index_zoom_ax.set_ylabel("Index Records Processable", fontweight="bold", fontsize=14)
    index_zoom_ax.spines['top'].set_visible(False)
    index_zoom_ax.spines['right'].set_visible(False)
    index_zoom_ax.set_ylim(
        bottom=min([i for i in [studies[name[0]][0]['total_records_processable'] for name in index_labels]]) * .995,
        top=max([i for i in [studies[name[0]][0]['total_records_processable'] for name in index_labels]]) * 1.005
    )

    index_zoom_ax.axhline(
        y=studies[layout_formats[2].name][0]['total_records_processable'],
        linewidth=1, color='black', label='Total Maximum'
    )

    index_zoom_ax.axhline(
        y=studies[layout_formats[3].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Aligned Maximum'
    )

    index_zoom_ax.axhline(
        y=studies[layout_formats[5].name][0]['total_records_processable'],
        linewidth=1, color='grey', label='Total Bitweave-V Maximum'
    )

    index_ax.legend()
    index_ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    index_zoom_ax.ticklabel_format(useOffset=False, style='plain', axis='y')
    plt.tight_layout()
    plt.savefig(f"{save_name}.index_chart.png")
    plt.show()


def main(save_name=""):
    with open(save_name or input(" > Provide a save name: "), "r") as fp:
        studies = dict()

        for study in range(len(layout_formats)):
            study_name = fp.readline().strip()
            layout_metadata = json.loads(fp.readline().strip())
            row_mapping = json.loads(fp.readline().strip())
            studies[study_name] = (layout_metadata, row_mapping)

    plot_record_charts(save_name, studies)
    plot_index_charts(save_name, studies)


if __name__ == '__main__':
    main()
