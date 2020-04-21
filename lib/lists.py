#!/usr/bin/env python
""" manages a dictionary of named lists """

import os
import yaml
from .__init__ import get_logger


class Lists:
    """ manages a dictionary of named lists """

    def __init__(self, errors_dir, run_id, log_file):
        self.dict = {}
        self.errors_dir = errors_dir
        self.run_id = run_id
        self.log = get_logger(log_file)

    def add(self, val, lst="main_list"):
        """ Create a new list if lst is not list
            add a value to the list if it does not already exist. """
        try:
            if not isinstance(self.dict[lst], list):
                self.dict[lst] = []
        except KeyError:
            self.dict[lst] = []

        if val:
            if not self.check_value(val, lst):
                self.dict[lst].append(val)
                return True
        return False

    def remove_value(self, val, lst="main_list"):
        """ remove a value from one of the lists """
        if self.check_value(val, lst):
            self.dict[lst].remove(val)
            return True
        return False

    def check_value(self, val, lst="main_list"):
        """ check if a value is in one of the lists """
        try:
            if val in self.dict[lst]:
                return True
        except KeyError:
            return False
        return False

    def get_list(self, lst="main_list"):
        """ refturn on of the lists """
        try:
            if isinstance(self.dict[lst], list):
                return self.dict[lst]
        except KeyError:
            return None
        return None

    def output_yaml(self):
        """ doc string """
        if self.errors_dir is None or self.run_id is None:
            return False

        error_file = "{}.yaml".format(self.run_id)
        error_file_path = os.path.join(self.errors_dir, error_file)

        # the file does not exists and write privileges are given
        # pylint: disable=bad-continuation
        if not os.path.exists(self.errors_dir) and os.access(
            os.path.dirname(error_file_path), os.W_OK
        ):
            os.makedirs(self.errors_dir)

        yaml_out = yaml.dump(self.dict, Dumper=MyDumper, default_flow_style=False)
        with open(error_file_path, "w") as errors_file:
            errors_file.write(yaml_out)
        self.log.info("Output Errors to file: %s", error_file_path)
        self.log.info("\n\n%s\n\n", yaml_out)
        return True


class MyDumper(yaml.Dumper):  # pylint: disable=too-many-ancestors
    """ indent the lists when outputting yaml """

    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)
