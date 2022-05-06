#!/usr/bin/python3

import argparse
import logging
import os
import re
import sys
import time
import subprocess
import utils

import process_data

logging.basicConfig(format='%(message)s', level=logging.DEBUG)


def get_fio_parameters():
    """
    Get fio parameters from default value or cmdlind input

    :return: test_scenario; test_bs; test_iodepth; thread_job
    """
    default_rt = os.path.join(sys.path[0], "result_dir")

    parser = argparse.ArgumentParser(description="Get fio parameters")
    parser.add_argument("--rt", type=str,required=False,
                        default=f"{default_rt}",
                        metavar="rt", help="Set Result dir")
    parser.add_argument("--testdir", type=str,required=True,
                        metavar="testdir", help="Set Test dir")
    parser.add_argument("--runtime", type=str,required=False,
                        metavar="runtime", help="Set Runtime (default: 1m)")
    parser.add_argument("--rw", type=str, required=False,
                        metavar="test_scenario",
                        help="Test scenario: read; write; "
                              "randread; randwrite; randrw (default: ALL)",
                        nargs="+", default=["read", "write", "randread",
                                            "randwrite", "randrw"])
    parser.add_argument("--bs", type=str, required=False, metavar="blocksize",
                        help="Test Block size (default: 4k,16k,64k,256k)",
                        nargs="+", default=["4k", "16k", "64k", "256k"])
    parser.add_argument("--iodepth", type=int, required=False,
                        metavar="iodepth", help="Iodepth (default: 1,8,64)",
                        nargs="+", default=[1, 8, 64])
    parser.add_argument("--jobs", type=int, required=False, metavar="jobs",
                        help="Thread jobs", default=16)
    parser.add_argument("--repeat", type=int, required=False, metavar="times",
                        help="Repeat times for each test scenario (default: 3)",
                        default=3)

    args = parser.parse_args()
    return (args.rt, args.testdir, args.runtime, args.rw,
            args.bs, args.iodepth, args.jobs, args.repeat)


def generate_fio_option(rw="read", bs="4k", iodepth=1,
                        runtime="1m", filename="/mnt/test/test",
                        ioengine="libaio", jobs=16, test_size="512MB",
                        output_file="/tmp/fio_output"):
    """
    Generate fio option

    :param rw: Test scenario
    :param bs: Test block size
    :param iodepth: Test iodepth
    :param runtime: Timeout setting
    :param filename: Test filename
    :param ioengine: Test backend ioengine
    :param jobs: The number of threads to run test
    :param test_size: The size of test file
    :param output_file: Output file
    """
    fio_option=(f"--rw={rw} --bs={bs} --iodepth={iodepth} "
                f"--runtime={runtime} --direct=1 "
                f"--filename={filename} --name=job1 "
                f"--ioengine={ioengine} --thread --group_reporting "
                f"--numjobs={jobs} --size={test_size} "
                f"--time_based --output={output_file}")
    return fio_option


def parse_fio_output(scenario, bs, depth, jobs, result_path):
    """
    Parse fio output and generate a summary

    :param scenario: fio rw
    :param bs: Test block size
    :param depth: Iodepth
    :param jobs: Thread jobs
    :param result_path: Fio result data path
    """
    pattern = (
        r".*[read|write].*IOPS=(\d+(?:\.\d+)?[\w|\s]),"
        r"\sBW=(\d+(?:\.\d+)?[\w|\s]*B/s)"
    )

    # Parse fio output and save data to a summary file
    with open(result_path, "a") as result_file:
        line = ""
        line += f"{utils.format_result(bs[:-1])}|"
        line += f"{utils.format_result(depth)}|"
        line += f"{utils.format_result(jobs)}|"
        fio_result = "/tmp/fio_output"

        o = subprocess.check_output(f"egrep '(read|write)' {fio_result}",
                                    shell=True)
        results = re.findall(pattern, o.decode())
        o = subprocess.check_output(f"egrep 'lat' {fio_result}", shell=True)
        latency = re.findall(
            r"\s{5}lat\s\((\wsec)\).*?avg=[\s]?(\d+(?:[\.][\d]+)?).*?",
            o.decode())
        bw = float(utils.normalize_data_size(results[0][1]))
        iops = float(utils.normalize_data_size(
            results[0][0], order_magnitude="B", factor=1000))
        lat = (float(latency[0][1]) / 1000 if latency[0][0] == "usec"
                     else float(latency[0][1]))
        # RW mode needs special processes. It has both read and write data
        if re.findall("rw", scenario):
            bw = bw + float(
                utils.normalize_data_size(results[1][1]))
            iops = iops + float(
                utils.normalize_data_size(
                    results[1][0], order_magnitude="B",
                    factor=1000))
            lat1 = float(latency[1][1]) / 1000 \
                if latency[1][0] == "usec" else float(latency[1][1])
            lat = lat + lat1

        for result in bw, iops, lat:
            line += f"{utils.format_result(result)}|"
            logging.debug("line is %s", line)
        result_file.write(f"{line}\n")


def run_fio(test_dir, runtime, scenario_list, bs_list,
            iodepth_list, thread_jobs, result_dir):
    """
    Run fio instance

    :param scenario_list: Test scenario list
    :param bs_list: Test block size list
    :param iodepth_list: Test iodepth list
    :param thread_jobs: Thread jobs
    :param result_dir: Test result dir
    """
    order_list = "Block_size Iodepth Threads BW(MB/S) IOPS Latency(ms)"
    result_path = os.path.join(result_dir, "fio_result.RHS")

    for scenario in scenario_list:
        with open(result_path, "a") as result_file:
            # Get order_list
            order_line = ""
            for order in order_list.split():
                order_line += f"{utils.format_result(order)}|"
            result_file.write(f"Category:{scenario}\n")
            result_file.write(f"{order_line.rstrip('|')}\n")
        for bs in bs_list:
            for depth in iodepth_list:
                fio_option = generate_fio_option(rw=scenario, bs=bs,
                                                 runtime=runtime,
                                                 filename=f"{test_dir}/test",
                                                 iodepth=depth)
                fio_cmd = "fio " + fio_option
                logging.debug("fio cmd is %s", fio_cmd)
                try:
                    utils.drop_cache()
                    subprocess.run(fio_cmd, shell=True, check=True)
                except:
                    logging.debug("Failed to run fio: %s", fio_cmd)
                    raise
                else:
                    parse_fio_output(scenario, bs, depth,
                                     thread_jobs, result_path)
                    # fio output file will be overwritten on the next run.
                    # Collect fio output otherwise the original will be lost.
                    fio_log = os.path.join(result_dir, "fio_log")
                    logging.debug(fio_log)
                    with open("/tmp/fio_output", "r") as source:
                        with open(fio_log, "a") as dest:
                            dest.write(f"{'-' * 100}\n")
                            dest.write(fio_cmd)
                            for line in source:
                                dest.write(line)


if __name__ == '__main__':
    (result_dir, test_dir, runtime, scenario, bs, iodepth, thread_jobs, repeat_times) = \
        get_fio_parameters()
    time_now = time.strftime("%Y%m%d-%H%M", time.localtime())
    top_test_result = os.path.join(f"{result_dir}", f"fio-{time_now}")

    for i in range(repeat_times):
        result_dir = os.path.join(top_test_result, f"fio_result-{i:02d}")
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        logging.debug(result_dir)
        run_fio(test_dir, runtime, scenario, bs, iodepth,
                thread_jobs, result_dir)
    process_data.process_test_result(top_test_result)
