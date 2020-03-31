#!/usr/bin/env python
"""

TODOS:
* docs
  * better code documentation
  * add a readme
    * describe csv/tsv file(s) directory
    * describe copy and dst_owner recopy
    * describe auth
* Add boto copy functionality
* Use an ACL rather than two copy operations
* file db for errors and succ_xfer_db
"""
import argparse
import concurrent.futures as futures
import csv
import logging
import os
import sys
import time
import random
import yaml


RUN_ID = int(time.time())
VERSION = "v0.01"


# pylint: disable=bad-continuation
def arg_parse():
    """ grabs augments with argparse """

    run_id_help = """
    RUN_ID = int(time.time())
    Epoch time int: (seconds since Jan 1st, 1970 (midnight UTC/GMT))
    Epoch to local time stamp: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(RUN_ID))
    View the last error output: # cat errors/$(ls -r errors | head -n 1)
    """

    description = """
    ./s3_csv_copy.py
    Copy objects in S3 using input from multiple CSV files.

    {}
    """.format(
        run_id_help
    )

    parser = argparse.ArgumentParser(
        description="%s" % description, formatter_class=argparse.RawTextHelpFormatter
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

    threads_help = """
    args:
        --max-threads-files
        --max-threads-lines-per-file
    Copies CSV files in parallel and also copies CSV lines in parallel
    WARNING: Max threads will be the product of the two thread variables
        """
    parser.add_argument(
        "--max-threads-files",
        "-mf",
        dest="max_threads_files",
        help="%s" % threads_help,
        type=int,
        default=4,
    )

    parser.add_argument(
        "--max-threads-lines-per-file",
        "-mo",
        dest="max_threads_lines_per_file",
        help="%s" % threads_help,
        type=int,
        default=2,
    )

    dst_owner_help = """
    arg:
        --dst-owner, -o
    If present also copy the obj over itself using the destination owner account.
    This will change the owner of the obj to the destination owner account instead
    of the user used for the main transfer.
        """
    # Note: we should be able to do this by putting the correct obj ACL instead
    parser.add_argument(
        "--dst-owner",
        "-o",
        dest="dst_owner",
        action="store_true",
        default=False,
        help="%s" % dst_owner_help,
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

    parser.add_argument(
        "--log-file", "-l", dest="log_file", type=str, default="s3_csv_copy.log"
    )

    parser.add_argument(
        "--max-cp-tries", "-mc", dest="max_cp_tries", type=int, default=3
    )

    parser.add_argument(
        "--sleep-timeout", "-s", dest="sleep_timeout", type=int, default=0
    )

    parser.add_argument(
        "--src-field-header", dest="src_field_header", type=str, default="src"
    )

    parser.add_argument(
        "--dst-field-header", dest="dst_field_header", type=str, default="dst"
    )

    cfg = parser.parse_args()
    cfg.db_dir = "db/{}".format(cfg.csv_dir)
    cfg.errors_dir = "db/{}/errors".format(cfg.csv_dir)
    cfg.succ_xfer_db_file = "db/{}/successful_transfers_db.yml".format(cfg.csv_dir)

    if not os.path.exists(cfg.errors_dir):
        os.makedirs(cfg.errors_dir)

    return cfg


def main():
    """ main """
    LOG.info("Starting s3_csv_copy version %s", VERSION)
    LOG.debug("Using CSV directory: %s", CFG.csv_dir)
    LOG.debug("Error types: %s", ERRORS.get_error_types())

    # credentials
    if not get_env_var("AWS_ACCESS_KEY_ID") or not get_env_var("AWS_SECRET_ACCESS_KEY"):
        msg = "No AWS Credentials: Please provide the AWS_ACCESS_KEY_ID and "
        msg += "AWS_SECRET_ACCESS_KEY environment variables."
        LOG.critical(msg)
        sys.exit(1)
    if CFG.dst_owner:
        if not get_env_var("DST_OWNER_AWS_ACCESS_KEY_ID") or not get_env_var(
            "DST_OWNER_AWS_SECRET_ACCESS_KEY"
        ):
            msg = "No AWS Credentials: Please provide the DST_OWNER_AWS_ACCESS_KEY_ID and "
            msg += "DST_OWNER_AWS_SECRET_ACCESS_KEY environment variables."
            LOG.critical(msg)
            sys.exit(1)

    csv_files = get_csv_files(CFG.csv_dir)
    if not csv_files:
        LOG.critical("Could not find any CSV files in CSV directory: %s", CFG.csv_dir)
        sys.exit(1)

    with futures.ThreadPoolExecutor(max_workers=CFG.max_threads_files) as executor:
        for file in csv_files:
            executor.submit(copy_csv_file, file)


def copy_csv_file(file_path):
    """
    read a csv file from arg file_path
    loop though rows concurrently and start a copy job for each thread
    """
    if check_stop_copy():
        return False

    args = {}
    args["file_path"] = str(file_path)
    args["err_tpe"] = str(file_path) + "_errors"
    args["cp_err_list"] = "failed_files_main_transfer"
    args["cp_owner_err_list"] = "failed_files_dst_owner"
    with open(args["file_path"], newline="") as csvfile:
        try:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect=dialect)
        except csv.Error as err:
            ERRORS.add_error("{}: {}".format(args["file_path"], err))
            return False

        ERRORS.create_error_type(args["err_tpe"])
        if (
            CFG.src_field_header not in reader.fieldnames
            or CFG.dst_field_header not in reader.fieldnames
        ):
            msg = "Error bad CSV field Headers: "
            msg += "file: %s, Headers: %s" % (args["file_path"], reader.fieldnames)
            ERRORS.add_error(msg, args["err_tpe"])
            return False
        with futures.ThreadPoolExecutor(
            max_workers=CFG.max_threads_lines_per_file
        ) as executor:
            for row in reader:
                executor.submit(try_s3cp, row, reader.line_num, args)
    return True


def try_s3cp(row, line_num, args):
    """
    use the s3copy function to move an obj in s3
    on fail sleep and try again
    after third try add an error the the errors dictionary
    * args
      * row
      * line_num
      * args['file_path']
      * args['err_tpe']
    """
    if check_stop_copy():
        return False

    def sleep(try_num):
        """ sleep if copy failed before next try """
        msg = "Sleeping {} seconds: failed try {} while processing file {} line number {}".format(
            CFG.sleep_timeout, (try_num + 1), args["file_path"], line_num
        )
        LOG.debug(msg)
        time.sleep(CFG.sleep_timeout)

    def copy(usr, paswd, src, dst):
        """
         * try to copy an obj up to CFG.max_cp_tries times
         * handle exceptions and add errors to the errors dictionary if needed
         * return True/False
        """
        for try_num in range(CFG.max_cp_tries):
            try:
                s3cp(usr, paswd, src, dst)
            except S3cpBadArgException as exc:
                msg = "Bad Arguments for s3cp(): {}".format(exc)
                ERRORS.add_error(msg, str(args["err_tpe"]))
                return False
            except S3cpFailure:
                sleep(try_num)
            else:
                return True
        return False

    if not row[CFG.src_field_header] or not row[CFG.dst_field_header]:
        msg = "Bad CSV Headers. file: {}, line: {}, row: {}".format(
            args["file_path"], line_num, row
        )
        ERRORS.add_error(msg, args["err_tpe"])
        return False

    # todo: look into copying with ACL instead of copying twice
    src = row[CFG.src_field_header]
    dst = row[CFG.dst_field_header]
    if copy(
        get_env_var("AWS_ACCESS_KEY_ID"), get_env_var("AWS_SECRET_ACCESS_KEY"), src, dst
    ):
        if CFG.dst_owner:
            if copy(
                get_env_var("DST_OWNER_AWS_ACCESS_KEY_ID"),
                get_env_var("DST_OWNER_AWS_SECRET_ACCESS_KEY"),
                dst,
                dst,
            ):
                # both copies where successful
                SUCCDB.add("{},{}".format(src, dst))
                SUCCDB.add("{0},{0}".format(dst))
                return True
            # owner copy failed, but main transfer copy was successful
            SUCCDB.add("{},{}".format(src, dst))
            msg = "{0},{0}".format(dst)
            ERRORS.add_error(msg, args["err_tpe"], args["cp_owner_err_list"], False)
        else:
            # copy successful, no owner copy needed
            SUCCDB.add("{},{}".format(src, dst))
            return True
    else:
        # main transfer copy failed
        msg = "{},{}".format(src, dst)
        ERRORS.add_error(msg, args["err_tpe"], args["cp_err_list"], False)
    return False


def s3cp(usr, paswd, src, dst):
    """ doc string """

    for key, value in locals().items():
        if not key:
            raise S3cpBadArgException(key, value)

    if not CFG.no_retry:
        if SUCCDB.check_values(src, dst):
            LOG.info(
                "Transfer found in successful transfer db Skipping: %s,%s", src, dst
            )
            return True

    if CFG.debug_int is not None:
        LOG.debug("src: %s, dst: %s", src, dst)
        if CFG.debug_int >= random.randint(0, 100):
            raise S3cpFailure()
    return True
    # todo: add boto copy commands
    # raise boto / other exceptions


def get_csv_files(csv_dir):
    """ return a list if csv/tsv file paths in directory (not recursive) """
    csv_files = []
    try:
        for file_name in os.listdir(csv_dir):
            if os.path.isfile(os.path.join(csv_dir, file_name)):
                if file_name.endswith(".csv") or file_name.endswith(".tsv"):
                    csv_files.append(os.path.join(csv_dir, file_name))
                else:
                    msg = (
                        "Bad File extension for file %s. Looking for .csv or .tsv"
                        % file_name
                    )
                    ERRORS.add_error(msg)
    except FileNotFoundError:
        LOG.critical("No such file or directory: %s", csv_dir)
        sys.exit(1)
    return csv_files


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
            "%(asctime)s;%(threadName)24s;%(levelname)s;%(message)s",
            "%Y-%m-%d_%H:%M:%S",
        )
        lfh.setFormatter(log_formater)
        lsh.setFormatter(log_formater)
        log.addHandler(lfh)
        log.addHandler(lsh)
        log.handler_set = True
    return log


def check_stop_copy():
    """ check if a file called check_stop exists """
    if os.path.exists("stop_copy"):
        return True
    return False


def get_env_var(var):
    """ doc string """
    envvar = os.getenv(var)
    if not envvar:
        return None
    return envvar


class SuccXferDB:
    """ class to manage a list of successful obj transfers """

    def __init__(self, db_file):
        self.db_file = db_file
        self.lst_name = "successful_transfers"
        self.lst = self.__get_db()

    def __get_db(self):
        """ load db list or create a new one """
        lst = {self.lst_name: []}
        if os.path.exists(self.db_file):
            with open(self.db_file) as succ_xfer_file:
                loaded_yaml = yaml.safe_load(succ_xfer_file)
        try:
            if isinstance(loaded_yaml[self.lst_name], list):
                lst = loaded_yaml
            else:
                lst = {self.lst_name: []}
        except NameError:
            lst = {self.lst_name: []}
        return lst

    def add(self, add_string):
        """ doc string """
        if not self.check_value(add_string):
            self.lst[self.lst_name].append(str(add_string))

    def check_value(self, value):
        """ doc string """
        if value in self.lst[self.lst_name]:
            return True
        return False

    def check_values(self, src, dst):
        """ doc string """
        value = "{},{}".format(src, dst)
        if value in self.lst[self.lst_name]:
            return True
        return False

    def output_yaml(self):
        """ write the lst db to a file """
        yaml_out = yaml.dump(self.lst, Dumper=MyDumper, default_flow_style=False)
        with open(self.db_file, "w") as file:
            file.write(yaml_out)
        LOG.info("Output successful transfers to file: %s", self.db_file)


class ErrorsDB:
    """ manages a dictionary of lists of critical errors """

    def __init__(self, errors_dir):
        """ init """
        self.default_typ = "main_errors"
        self.errors = {}
        self.error_types = []
        self.create_error_type(self.default_typ)
        self.errors_dir = errors_dir

    def create_error_type(self, typ):
        """ create a error type list in the errors dictionary """
        if not self.error_type_exists(typ):
            self.error_types.append(str(typ))
            self.errors[str(typ)] = {}
        else:
            LOG.info(
                "Error type %s already exists. Skipping creation of error type: %s",
                typ,
                typ,
            )

    def add_error(self, msg, typ="main_errors", err_list="errors", log=True):
        """ add an error to the errors dictionary """
        if not self.error_type_exists(typ):
            log_msg = "error_type does not exists using default. type: %s" % typ
            typ = self.default_typ
            self.errors[typ]["errors"].append(log_msg)
        if err_list not in self.errors[typ]:
            self.errors[typ][err_list] = []
        self.errors[typ][err_list].append(msg)
        if log:
            LOG.error(msg)

    def output_yaml(self):
        """ doc string """
        if not os.path.exists(self.errors_dir):
            os.makedirs(self.errors_dir)
        error_file = "{}.yaml".format(RUN_ID)
        error_file_path = os.path.join(self.errors_dir, error_file)
        yaml_out = yaml.dump(self.errors, Dumper=MyDumper, default_flow_style=False)
        with open(error_file_path, "w") as errors_file:
            errors_file.write(yaml_out)
        LOG.info("Output Errors to file: %s", error_file_path)
        LOG.info("\n\n%s\n\n", yaml_out)

    def error_type_exists(self, typ):
        """ doc string """
        if str(typ) in self.error_types:
            return True
        return False

    def get_error_types(self):
        """ doc string """
        return self.error_types

    def get_errors_for_type(self, typ):
        """ log the errors for given error type column """
        if self.error_type_exists(typ):
            return self.errors[str(typ)]
        return None


class MyDumper(yaml.Dumper):  # pylint: disable=too-many-ancestors
    """ indent the lists when outputting yaml """

    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


# define Python user-defined exceptions
class CustomException(Exception):
    """ Base class for other exceptions """


class S3cpFailure(CustomException):
    """ boto copy flailed """


class S3cpBadArgException(CustomException):
    """ bad args sent to s3cp """


if __name__ == "__main__":
    CFG = arg_parse()
    LOG = get_logger(CFG.log_file)
    ERRORS = ErrorsDB(CFG.errors_dir)
    SUCCDB = SuccXferDB(CFG.succ_xfer_db_file)
    main()
    SUCCDB.output_yaml()
    ERRORS.output_yaml()
    sys.exit(0)
