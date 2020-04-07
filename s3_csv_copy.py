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
"""
import argparse
import concurrent.futures as futures
import csv
import logging
import os
import sys
import time
import random
import re
import yaml
import boto3
import botocore


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

    csv_args = {}
    csv_args["file_path"] = str(file_path)
    csv_args["err_tpe"] = str(file_path) + "_errors"
    csv_args["cp_err_list"] = "failed_files_main_transfer"
    csv_args["cp_owner_err_list"] = "failed_files_dst_owner"
    ERRORS.create_error_type(csv_args["err_tpe"])

    with open(csv_args["file_path"], newline="") as csvfile:
        try:
            dialect = csv.Sniffer().sniff(csvfile.read(1024))
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, dialect=dialect)
        except csv.Error as err:
            ERRORS.add_error("{}: {}".format(csv_args["file_path"], err))
            return False

        if (
            CFG.src_field_header not in reader.fieldnames
            or CFG.dst_field_header not in reader.fieldnames
        ):
            msg = "Error bad CSV field Headers: - file: {}, Headers: {}".format(
                csv_args["file_path"], reader.fieldnames
            )
            ERRORS.add_error(msg)
            return False

        with futures.ThreadPoolExecutor(
            max_workers=CFG.max_threads_lines_per_file
        ) as executor:
            for row in reader:
                row_args = {}
                row_args["src"] = row[CFG.src_field_header]
                row_args["dst"] = row[CFG.dst_field_header]
                row_args["line_num"] = reader.line_num
                if not row[CFG.src_field_header] or not row[CFG.dst_field_header]:
                    msg = "Bad CSV Headers. file: {}, line: {}, src: {}, dst: {}".format(
                        csv_args["file_path"],
                        row_args["line_num"],
                        row_args["src"],
                        row_args["dst"],
                    )
                    ERRORS.add_error(msg, csv_args["err_tpe"])
                    return False
                executor.submit(cp_row, csv_args, row_args)
    return True


def cp_row(csv_args, row_args):
    """
    Start copy operations needed for each row.
    Sets the user and password() for each copy operation.
                e.g. AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY
    * args
      * csv_args
        * csv_args["file_path"]
        * csv_args["err_tpe"]
        * csv_args["cp_err_list"]
        * csv_args["cp_owner_err_list"]
      * row_args
        * row_args["src"]
        * row_args["dst"]
        * row_args["line_num"]
    """
    # todo: look into copying with ACL instead of copying twice

    if check_stop_copy():
        return False

    row_args["usr"] = get_env_var("AWS_ACCESS_KEY_ID")
    row_args["paswd"] = get_env_var("AWS_SECRET_ACCESS_KEY")
    if try_s3cp(csv_args, row_args):
        if CFG.dst_owner:
            row_args["usr"] = get_env_var("DST_OWNER_AWS_ACCESS_KEY_ID")
            row_args["paswd"] = get_env_var("DST_OWNER_AWS_SECRET_ACCESS_KEY")
            if try_s3cp(csv_args, row_args):
                # both copies where successful
                SUCCDB.add("{},{}".format(row_args["src"], row_args["dst"]))
                SUCCDB.add("{0},{0}".format(row_args["dst"]))
                return True
            # owner copy failed, but main transfer copy was successful
            SUCCDB.add("{},{}".format(row_args["src"], row_args["dst"]))
            msg = "{0},{0}".format(row_args["dst"])
            ERRORS.add_error(
                msg, csv_args["err_tpe"], csv_args["cp_owner_err_list"], False
            )
        else:
            # copy successful, no owner copy needed
            SUCCDB.add("{},{}".format(row_args["src"], row_args["dst"]))
            return True
    else:
        # main transfer copy failed
        msg = "{},{}".format(row_args["src"], row_args["dst"])
        ERRORS.add_error(msg, csv_args["err_tpe"], csv_args["cp_err_list"], False)
    return False


def try_s3cp(csv_args, row_args):
    """
    Try to copy an obj up to CFG.max_cp_tries times
    On fail sleep and try again
    After third try add an error the the errors dictionary
    Return True/False
    * args
      * csv_args
        * csv_args["file_path"]
        * csv_args["err_tpe"]
        * csv_args["cp_err_list"]
        * csv_args["cp_owner_err_list"]
      * row_args
        * row_args["src"]
        * row_args["dst"]
        * row_args["line_num"]
        * row_args["usr"]
        * row_args["paswd"]
    """

    def sleep(try_num):
        """ sleep if copy failed before next try """
        LOG.debug(
            "Sleeping %s seconds: failed try %s while processing file %s line number %s",
            CFG.sleep_timeout,
            try_num,
            csv_args["file_path"],
            row_args["line_num"],
        )
        time.sleep(CFG.sleep_timeout)

    for try_num in range(CFG.max_cp_tries):
        try:
            if check_stop_copy():
                return False
            s3cp(row_args)
        except DebugS3cpSuccess:
            return True
        except AlreadyTransfered:
            return True
        except DebugS3cpRetry:
            sleep(try_num + 1)
        except S3cpBadArgException:
            return False
        except S3cpSuccess:
            return True
        # todo: if boto copy fails we need to except here and sleep
    return False


def s3cp(row_args):
    """
    Copy a object to AWS s3 using python boto3
    The source object may be a local file or an object in s3
    The destination object must always be an s3 object
    Return True/False
    * args
      * row_args
        * row_args["src"]
        * row_args["dst"]
        * row_args["line_num"]
        * row_args["usr"]
        * row_args["paswd"]
    """
    # pylint: disable=too-many-locals
    # pylint: disable=too-many-statements

    # pylint does not like s3c.Object/s3c.copy etc. looks like bug in pylint
    # pylint: disable=no-member
    s3c = boto3.resource("s3")
    print("s3cp")

    def check_s3_url(url):
        """ check if string is a valid aws s3 url """
        if re.match("^s3://([^/]+)/(.*?([^/]+))$", url) is not None:
            return True
        return False

    def check_local_file_exists(file_path):
        """ check if a local file exits """
        # cwd = os.getcwd()
        # path = cwd + os.sep + file_path
        # print("path: ", path)
        print("path string: ", repr(file_path))
        if os.path.exists(str(file_path).rstrip()):
            return True
        return False

    def check_s3_obj_exists(s3_path):
        """ Check if an s3 object exists """
        if not check_s3_url(s3_path):
            return False

        bucket = s3_path.split("/")[2]
        key = "/".join(s3_path.split("/")[3:])

        try:
            s3c.Object(bucket, key).load()
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                LOG.debug(
                    "s3cp:check_s3_obj_exists: object %s does not exists. Got 404",
                    s3_path,
                )
                return False
            LOG.debug(
                "s3cp:check_s3_obj_exists: error getting object %s: error: %s ",
                s3_path,
                exc,
            )
            return False
        else:
            return True  # The object does exist.

    def is_src_s3(src):
        """ Check if src is a valid local file or object in s3 and return 'file', 's3', or None """
        # Check if src is local file or s3 obj and set copy type boto.copy() or boto.upload_file()
        if check_s3_url(src) and check_s3_obj_exists(src):
            return True
        return False

    def is_src_file(src):
        """ doc string """
        if check_local_file_exists(src):
            return False
        return False

    def get_src_type(src):
        """ doc string """
        if is_src_s3(src):
            return "s3"
        if is_src_file(src):
            return "file"
        return None

    def check_src_dst_same(src_type, src, dst):
        """ check if the src and dst are the same size in bytes """
        print("checking src dst")
        if src_type == "s3":
            print("src s3")
            src_bucket = src.split("/")[2]
            src_key = "/".join(src.split("/")[3:])
            src_size = s3c.Object(src_bucket, src_key).content_length
            print("size: ", src_size)
        elif src_type == "file":
            print("src file")
            src_size = os.path.getsize(src)
            print("size: ", src_size)
        else:
            print("not s3 or file ERRRRRRRROR")
            return False

        dst_bucket = dst.split("/")[2]
        dst_key = "/".join(dst.split("/")[3:])
        print("checking dst exists")
        if check_s3_obj_exists(dst):
            dst_size = s3c.Object(dst_bucket, dst_key).content_length
            print("dst exists and is size: dst_size",)
        else:
            print("dst does not exists")
            return False

        print("src_size: ", src_size, "dst_size: ", dst_size)
        if src_size == dst_size:
            print("src size is dst size")
            return True
        print("src size is not dst size")
        return False

    ##############
    # debug mode #
    ##############
    if CFG.debug_int is not None:
        if not CFG.no_retry:
            if SUCCDB.check_values(row_args["src"], row_args["dst"]):
                LOG.info(
                    "Transfer found in successful transfer db Skipping: %s,%s",
                    row_args["src"],
                    row_args["dst"],
                )
                raise AlreadyTransfered()
        if CFG.debug_int >= random.randint(0, 100):
            LOG.debug(
                "Debug_copy_retry - src: %s, dst: %s", row_args["src"], row_args["dst"]
            )
            raise DebugS3cpRetry()
        LOG.debug(
            "Debug_copy_success - src: %s, dst: %s", row_args["src"], row_args["dst"]
        )
        raise DebugS3cpSuccess()

    #############
    # Copy mode #
    #############






    src_type = get_src_type(row_args["src"])
    print("src type: ", src_type, "for src: ", row_args["src"])
    if src_type is None:
        LOG.error(
            "src is not a present local file or an s3 object: %s", row_args["src"]
        )
        raise S3cpBadArgException()

    print("past src type")
    if not CFG.no_retry:
        print("not no_retry")
        if check_src_dst_same(src_type, row_args["src"], row_args["dst"]):
            LOG.info(
                "the source %s already exists in destination %s",
                row_args["src"],
                row_args["dst"],
            )
            raise AlreadyTransfered()
        print("src_dst not the same")

    try:
        src_bucket = row_args["src"].split("/")[2]
        src_key = "/".join(row_args["src"].split("/")[3:])
        dst_bucket = row_args["dst"].split("/")[2]
        dst_key = "/".join(row_args["dst"].split("/")[3:])

        copy_source = {"Bucket": str(src_bucket), "Key": str(src_key)}
        extra_args = {
            "ServerSideEncryption": "AES256",
            "ACL": "bucket-owner-full-control",
        }
        s3c.meta.client.copy(copy_source, str(dst_bucket), str(dst_key), extra_args)
    except Exception as exc:
        raise exc
    else:
        LOG.info("successfully Copied %s to %s", row_args["src"], row_args["dst"])
        SUCCDB.add("{},{}".format(row_args["src"], row_args["dst"]))
        raise S3cpSuccess()


"""
    * check if src exists
    * set src type
    * if not CFG.no_retry:
      * check if dst exists
      * check if src and dst are the same
    * do copy
"""


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


class DebugS3cpRetry(CustomException):
    """ boto copy flailed """


class DebugS3cpSuccess(CustomException):
    """ debug mode completed without failing random int % """


class S3cpSuccess(CustomException):
    """ Successfully copied file/obj to dst % """


class AlreadyTransfered(CustomException):
    """ The dst exists test found the obj. skipping % """


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
