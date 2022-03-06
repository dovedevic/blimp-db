import matplotlib
import matplotlib.pyplot as plt

from matplotlib.axes import Axes
from matplotlib.figure import Figure
from src.configuration import SystemConfiguration
from configuration_enumerator import ROW_BUFFER_SIZES, HITMAP_SIZES, PRIMARY_INDEX_SIZES, TOTAL_RECORD_SIZES

plt.rcParams["font.family"] = "Times New Roman"


def hitmap_sensitivity_to_hitmap_size(axis: Axes, r_s=1024, i_s=32, c_s=1.0) -> Axes:
    w_p_values = []
    w_d_values = []
    w_h_values = []
    total_record_values = []
    baseline_blimp_record_values = []

    for h_c in HITMAP_SIZES:
        cfg = SystemConfiguration.construct_32mb_default_ambit_bank(
            r_s,
            h_c,
            i_s,
            c_s
        )
        w_p_values.append(cfg.total_available_rows_for_ambit)
        w_d_values.append(cfg.total_available_rows_for_data)
        w_h_values.append(cfg.total_available_rows_for_hitmap)
        total_record_values.append(cfg.total_available_rows_for_data // cfg.record_to_rb_ratio)
        baseline_blimp_record_values.append(cfg.bank_size // (cfg.row_buffer_size * cfg.record_to_rb_ratio))

    axis.plot(HITMAP_SIZES, total_record_values, color='cyan', linestyle='solid', linewidth=2)
    axis.plot(HITMAP_SIZES, w_p_values, color='purple', linestyle='dashed', linewidth=2)
    axis.plot(HITMAP_SIZES, w_d_values, color='blue', linestyle='dashed', linewidth=2)
    axis.plot(HITMAP_SIZES, w_h_values, color='red', linestyle='dashed', linewidth=2)
    axis.plot(HITMAP_SIZES, baseline_blimp_record_values, color='grey', linestyle='dotted', linewidth=2)

    return axis


def row_distribution_sensitivity_to_row_buffer_size(axis: Axes, h_c=1, i_s=32, c_s=1.0, record_size=None) -> Axes:
    w_p_values = []
    w_d_values = []
    w_h_values = []
    total_record_values = []
    baseline_blimp_record_values = []

    for idx, r_s in enumerate(ROW_BUFFER_SIZES):
        cfg = SystemConfiguration.construct_32mb_default_ambit_bank(
            r_s,
            h_c,
            i_s,
            c_s if not record_size else (record_size / r_s)
        )
        w_p_values.append(cfg.total_available_rows_for_ambit)
        w_d_values.append(cfg.total_available_rows_for_data)
        w_h_values.append(cfg.total_available_rows_for_hitmap)
        total_record_values.append(cfg.total_available_rows_for_data // cfg.record_to_rb_ratio)
        baseline_blimp_record_values.append(cfg.bank_size // (cfg.row_buffer_size * cfg.record_to_rb_ratio))

    axis.plot(range(len(ROW_BUFFER_SIZES)), total_record_values, color='cyan', linestyle='solid', linewidth=2, label="records")
    axis.plot(range(len(ROW_BUFFER_SIZES)), w_p_values, color='purple', linestyle='dashed', linewidth=2, label="ambit p/i")
    axis.plot(range(len(ROW_BUFFER_SIZES)), w_d_values, color='blue', linestyle='dashed', linewidth=2, label="data")
    axis.plot(range(len(ROW_BUFFER_SIZES)), w_h_values, color='red', linestyle='dashed', linewidth=2, label="hitmap")
    axis.plot(range(len(ROW_BUFFER_SIZES)), baseline_blimp_record_values, color='grey', linestyle='dotted', linewidth=2, label="blimp baseline")

    axis.set_title(f"{record_size:,}B Records" if record_size else f"{c_s:,}:1 Record to Row Buffer")

    axis.set_xlabel("Row Buffer Size")
    axis.set_xticks(range(len(ROW_BUFFER_SIZES)))
    axis.set_xticklabels(ROW_BUFFER_SIZES, rotation=45)
    axis.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    axis.set_ylabel("Available Rows (B)")
    axis.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    axis.legend(loc='upper center', bbox_to_anchor=(0.5, -0.25), fancybox=True, shadow=True, ncol=5)

    return axis


def row_distribution_sensitivity_to_pi_size(axis: Axes, h_c=1, r_s=1024, c_s=1.0, record_size=None) -> Axes:
    w_p_values = []
    w_d_values = []
    w_h_values = []
    total_record_values = []
    baseline_blimp_record_values = []

    for idx, i_s in enumerate(PRIMARY_INDEX_SIZES):
        cfg = SystemConfiguration.construct_32mb_default_ambit_bank(
            r_s,
            h_c,
            i_s,
            c_s if not record_size else (record_size / r_s)
        )
        w_p_values.append(cfg.total_available_rows_for_ambit)
        w_d_values.append(cfg.total_available_rows_for_data)
        w_h_values.append(cfg.total_available_rows_for_hitmap)
        total_record_values.append(cfg.total_available_rows_for_data // cfg.record_to_rb_ratio)
        baseline_blimp_record_values.append(cfg.bank_size // (cfg.row_buffer_size * cfg.record_to_rb_ratio))

    axis.plot(range(len(PRIMARY_INDEX_SIZES)), total_record_values, color='cyan', linestyle='solid', linewidth=2, label="records")
    axis.plot(range(len(PRIMARY_INDEX_SIZES)), baseline_blimp_record_values, color='grey', linestyle='dotted',linewidth=2, label="blimp baseline")

    axis2 = axis.twinx()
    axis2.plot(range(len(PRIMARY_INDEX_SIZES)), w_p_values, color='purple', linestyle='dashed', linewidth=2, label="ambit p/i")
    axis2.plot(range(len(PRIMARY_INDEX_SIZES)), w_d_values, color='blue', linestyle='dashed', linewidth=2, label="data")
    axis2.plot(range(len(PRIMARY_INDEX_SIZES)), w_h_values, color='red', linestyle='dashed', linewidth=2, label="hitmap")
    axis2.plot([], [], color='grey', linestyle='dotted',linewidth=2, label="blimp baseline")
    axis2.plot([], [], color='cyan', linestyle='solid', linewidth=2, label="records")

    axis.set_title(f"{record_size:,}B Records" if record_size else f"{c_s:,}:1 Record to Row Buffer")

    axis.set_xlabel("P/I Key Size (B)")
    axis.set_xticks(range(len(ROW_BUFFER_SIZES)))
    axis.set_xticklabels(ROW_BUFFER_SIZES, rotation=45)
    axis.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    axis.set_ylabel("Available Rows")
    axis2.set_ylabel("Records Available")
    axis.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    axis2.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    axis2.legend(loc='upper center', bbox_to_anchor=(0.5, -0.22), fancybox=True, shadow=True, ncol=5)

    return axis


def hitmap_sensitivity_to_hitmap_size_by_record_size():
    #fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, sharex=True, sharey=True)  # type: Figure, (Axes, Axes, Axes, Axes)
    fig, ax1 = plt.subplots(1, 1, sharex=True, sharey=True)  # type: Figure, Any

    ax1 = hitmap_sensitivity_to_hitmap_size(ax1, c_s=0.5)
    #ax2 = hitmap_sensitivity_to_hitmap_size(ax2, c_s=1.0)
    #ax3 = hitmap_sensitivity_to_hitmap_size(ax3, c_s=2.0)
    #ax4 = hitmap_sensitivity_to_hitmap_size(ax4, c_s=4.0)

    plt.xlabel("Hitmap Size")
    plt.ylabel("Available Rows")
    plt.title("Row Ristribution Sensitivity to Hitmap Size\nBaseline (1,024B records)")

    fig.show()


def row_distribution_sensitivity_to_row_buffer_size_by_record_size():
    fig, \
    (ax1) = \
    plt.subplots(
        1, 1, sharex='all', sharey='all'
    )  # type: Figure, (Axes, ...)

    #ax1 = row_distribution_sensitivity_to_row_buffer_size(ax1, record_size=512)
    ax2 = row_distribution_sensitivity_to_row_buffer_size(ax1, record_size=1024)
    #ax3 = row_distribution_sensitivity_to_row_buffer_size(ax3, record_size=2048)

    fig.suptitle("Row Distribution Sensitivity to Row Buffer Size", fontsize=16)
    plt.xticks(range(len(ROW_BUFFER_SIZES)), [f"{v:,}" for v in ROW_BUFFER_SIZES])

    plt.tight_layout()
    plt.show()


def row_distribution_sensitivity_to_pi_size_by_record_size():
    fig, \
    (ax1) = \
    plt.subplots(
        1, 1, sharex='all', sharey='all'
    )  # type: Figure, (Axes, ...)

    #ax1 = row_distribution_sensitivity_to_pi_size(ax1, record_size=512)
    ax2 = row_distribution_sensitivity_to_pi_size(ax1, record_size=2048, r_s=8192)
    #ax3 = row_distribution_sensitivity_to_pi_size(ax3, record_size=2048)

    fig.suptitle("Row Distribution Sensitivity to P/I Key Size", fontsize=16)
    plt.xticks(range(len(PRIMARY_INDEX_SIZES)), [f"{v:,}" for v in PRIMARY_INDEX_SIZES])

    plt.tight_layout()
    plt.show()


row_distribution_sensitivity_to_pi_size_by_record_size()
