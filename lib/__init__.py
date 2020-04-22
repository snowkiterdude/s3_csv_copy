#!/usr/bin/env python
""" argparse and logging """

import argparse
import multiprocessing
import os
import logging


def parse_cfg():
    """ grabs augments with argparse """

    parser = argparse.ArgumentParser(
        description="Copy objects in S3 using input from multiple CSV files.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    csv_dir_help = """
    Dir containing CSV files (CSV/TSV).
    CSV Headers must include
        --src-field-header default src
        --dst-field-header default dst
        e.g..
    src,dest
    some_src_path,some_dst_path
    ...
    """
    parser.add_argument(
        "--csv-dir",
        "-c",
        dest="csv_dir",
        help="%s" % csv_dir_help,
        type=str,
        default="csv",
    )

    no_retry_help = """
    arg:
        --no-retry, -n
    If present no check will be done. By default s3_csv_copy will skip source,destination pairs it has in
    the successful transfers file. If the --no-retry flag is set s3_csv_copy will try to transfer
    the file even if it is already in the successful transfers file.

    """
    parser.add_argument(
        "--no-retry",
        "-n",
        dest="no_retry",
        action="store_true",
        default=False,
        help="%s" % no_retry_help,
    )

    debug_mode_help = """
    arg:
        --debug, -d
    type:
        int
    Integer representing the failure present. If present debug mode is enabled. Debug mode will randomly
    return false rather than transferring objects.
    """
    parser.add_argument(
        "--debug",
        "-d",
        dest="debug_int",
        default=None,
        type=int,
        help="{}".format(debug_mode_help),
    )

    boto_extra_args_help = """
    arg:
        --boto-extra-args
    type:
        string containing json
    boto extra_args passed to boto during copy operations.
    """

    parser.add_argument(
        "--max-threads-per-process",
        "-t",
        dest="max_threads_per_process",
        type=int,
        default=12,
    )

    parser.add_argument(
        "--boto-extra-args",
        dest="boto_extra_args",
        type=str,
        default='{"ServerSideEncryption": "AES256","ACL": "bucket-owner-full-control"}',
        help="{}".format(boto_extra_args_help),
    )

    parser.add_argument(
        "--log-file", "-l", dest="log_file", type=str, default="s3_csv_copy.log"
    )

    parser.add_argument(
        "--max-cp-tries", "-mc", dest="max_cp_tries", type=int, default=3
    )

    parser.add_argument(
        "--sleep-timeout", "-s", dest="sleep_timeout", type=int, default=900
    )

    parser.add_argument(
        "--src-field-header", dest="src_field_header", type=str, default="source"
    )

    parser.add_argument(
        "--dst-field-header", dest="dst_field_header", type=str, default="destination"
    )

    cfg = parser.parse_args()
    cfg.db_dir = "db/{}".format(cfg.csv_dir)
    cfg.errors_dir = "db/{}/errors".format(cfg.csv_dir)
    cfg.succ_xfer_db_file = "db/{}/successful_transfers_db.yml".format(cfg.csv_dir)
    cfg.num_processes = int(multiprocessing.cpu_count())

    if not os.path.exists(cfg.errors_dir):
        os.makedirs(cfg.errors_dir)

    return cfg


def get_logger(log_file):
    """ Returns the main logging obj. Creates new logging obj if it does not exist. """
    log = logging.getLogger(__name__)
    if not getattr(log, "handler_set", None):
        log.setLevel(logging.DEBUG)
        lfh = logging.FileHandler(log_file)
        lfh.setLevel(logging.DEBUG)
        lsh = logging.StreamHandler()
        lsh.setLevel(logging.DEBUG)
        log_formater = logging.Formatter(
            "%(asctime)19s;%(processName)15s;%(threadName)24s;%(levelname)8s;%(message)s",
            "%Y-%m-%d_%H:%M:%S",
        )
        lfh.setFormatter(log_formater)
        lsh.setFormatter(log_formater)
        log.addHandler(lfh)
        log.addHandler(lsh)
        log.handler_set = True
    return log
