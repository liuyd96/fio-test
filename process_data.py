"""
APIs to process fio result files to get a summary test report.

Typically, FIO test need to be repeated multiple times to obtain a more
accurate data. So we will get several fio test reports.

Process all of these test reports to get a summary report.
"""

import os
import re

import logging

import utils

logging.basicConfig(format='%(message)s', level=logging.DEBUG)


def get_data_file_list(result_dir):
    """
    Get the list of data files.

    :param result_dir: The path of result dir.

    :return: A list of data files.
    """
    result = []
    filename = "fio_result.RHS"

    # Walking top-down from the root
    for root, _, files in os.walk(result_dir):
        if filename in files:
            result.append(os.path.join(root, filename))

    logging.debug(result)
    return result


def get_metadata(file_list):
    """
    Get metadata from data_list

    :param data_list: The list of data files

    :return: A list of metadata
    """
    data_list = []

    for file in file_list:
        with open(file, "r") as f:  # pylint: disable=C0103
            content = []
            for line in f.readlines():
                content.append("".join(line.strip().split(" ")))
        data_list.append(content)

    return data_list


def generate_summary_report(top_dir, data_list):
    """
    Generate summary report

    :param top_dir: The top result dir
    :param data_list: The list of metadata
    """
    result_num = len(data_list)
    if result_num <= 1:
        return

    # Make sure all lists have same lines.
    data_line = len(data_list[0])
    for i in range(1, result_num):
        if len(data_list[i]) != data_line:
            logging.error("Unmatched data: %s", data_list[i])

    with open(f"{top_dir}/test_summary", "w") as fp:   # pylint: disable=C0103
        for line in range(data_line):
            # Order line
            if not re.match(r"(\d+\.?\d*)\|", data_list[0][line]):
                order_list = [order for order in data_list[0][line].split("|")
                              if order]
                order_line = ""
                for order in order_list:
                    order_line += f"{utils.format_result(order)}|"
                fp.write(f"{order_line}\n")
            else:
                ori_data_list = [data for data in data_list[0][line].split("|")
                                 if data]
                if len(ori_data_list) != 6:
                    logging.error("Wrong data length")
                print(ori_data_list)
                output = ""
                band_width = iops = latency = 0.0
                band_width = float(ori_data_list[3])
                iops = float(ori_data_list[4])
                latency = float(ori_data_list[5])
                output += f"{utils.format_result(ori_data_list[0])}|"
                output += f"{utils.format_result(ori_data_list[1])}|"
                output += f"{utils.format_result(ori_data_list[2])}|"

                for i in range(1, result_num):
                    band_width += float(data_list[i][line].split("|")[3])
                    iops += float(data_list[i][line].split("|")[4])
                    latency += float(data_list[i][line].split("|")[5])
                band_width_avg = band_width / result_num
                iops_avg = iops / result_num
                latency_avg = latency / result_num
                output += f"{utils.format_result(band_width_avg)}|"
                output += f"{utils.format_result(iops_avg)}|"
                output += f"{utils.format_result(latency_avg)}|"
                fp.write(f"{output}\n")


def process_test_result(result_dir):
    """
    Process test results and generate summary report.
    """
    result_list = get_data_file_list(result_dir)
    result_data_list = get_metadata(result_list)
    generate_summary_report(result_dir, result_data_list)
