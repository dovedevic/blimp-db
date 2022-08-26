from studies.data_format_meta.study import main as study_main
from studies.data_format_meta.chart import main as chart_main

study_name = input(" > Enter study name: ")
study_main(study_name)
chart_main(study_name)
