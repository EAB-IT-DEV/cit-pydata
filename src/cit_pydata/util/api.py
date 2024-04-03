import os
import sys

import datetime

PATH_TO_ENV = os.path.join(os.getcwd(), ".env")
ON_AWS = os.getenv("AWS_EXECUTION_ENV")


def encode_base64(value: str):
    """
    Returns base64 encoded value for the input string parameter
    """
    import base64

    binary_value = value.encode("ascii")

    return base64.b64encode(binary_value)


def get_datetime_string():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def multireplace(string, replacements, ignore_case=False):
    """
    Given a string and a replacement map, it returns the replaced string.
    :param str string: string to execute replacements on
    :param dict replacements: replacement dictionary {value to find: value to replace}
    :param bool ignore_case: whether the match should be case insensitive
    :rtype: str
    """
    # Source: https://gist.github.com/bgusach/a967e0587d6e01e889fd1d776c5f3729

    import re

    if not replacements:
        # Edge case that'd produce a funny regex and cause a KeyError
        return string

    # If case insensitive, we need to normalize the old string so that later a replacement
    # can be found. For instance with {"HEY": "lol"} we should match and find a replacement for "hey",
    # "HEY", "hEy", etc.
    if ignore_case:

        def normalize_old(s: str):
            return s.lower()

        re_mode = re.IGNORECASE

    else:

        def normalize_old(s: str):
            return s

        re_mode = 0

    replacements = {normalize_old(key): val for key, val in replacements.items()}

    # Place longer ones first to keep shorter substrings from matching where the longer ones should take place
    # For instance given the replacements {'ab': 'AB', 'abc': 'ABC'} against the string 'hey abc', it should produce
    # 'hey ABC' and not 'hey ABc'
    rep_sorted = sorted(replacements, key=len, reverse=True)
    rep_escaped = map(re.escape, rep_sorted)

    # Create a big OR regex that matches any of the substrings to replace
    pattern = re.compile("|".join(rep_escaped), re_mode)

    # For each match, look up the new string in the replacements, being the key the normalized old string
    return pattern.sub(
        lambda match: replacements[normalize_old(match.group(0))], string
    )


def list_dict_to_csv(file_path, file_name, data_list, column_list=None):
    """writes a list of dictionary values to a csv
    dictionary should be column_name:value format

    """
    import csv

    if type(data_list) != list:
        return False

    if column_list is None:
        if data_list == []:
            # Empty data list
            return False
        else:
            column_list = data_list[0].keys()

    full_file_path = os.path.join(file_path, file_name)
    remove_file(full_file_path)

    # Create folder if it doesnt exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    with open(full_file_path, "w+", newline="") as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=column_list)
        csv_writer.writeheader()
        csv_writer.writerows(data_list)

    return True


def json_to_file(json_object, file_path):
    import json

    remove_file(file_path)
    with open(file_path, "w") as json_file:
        json.dump(json_object, json_file)


def remove_file(file_path):
    """removes file by first checking if it exists and then removing"""
    if os.path.exists(file_path):
        os.remove(file_path)


def get_environment_variable(
    logger=None, path_to_env_file: str = None, variable_name: str = None
):
    """Returns variable value from the env file located at path_to_env_file with the name variable_name"""
    if not logger:
        logger = get_logger()

    if ON_AWS and variable_name.startswith("aws"):
        logger.info(
            f"Detected on AWS. Skipping retrieval of AWS .env variable {variable_name}"
        )
        return None

    from dotenv import dotenv_values

    _path_to_env_file = None

    if path_to_env_file is None:
        _path_to_env_file = PATH_TO_ENV
    else:
        _path_to_env_file = path_to_env_file

    if not os.path.exists(_path_to_env_file):
        logger.error(f'environment file "{_path_to_env_file}" not found')
        return

    _envivironment_variable = variable_name.lower()

    config = dotenv_values(_path_to_env_file)
    variable = config.get(_envivironment_variable, None)

    if not variable:
        logger.error(
            f'environment variable "{_envivironment_variable}" not found in env file {_path_to_env_file}'
        )
        return None

    return variable


# def handle_logger_argument(logger_argument):
#     if isinstance(logger_argument, logging.Logger):
#         return logger_argument
#     else:
#         return get_logger(__name__, "info")


def get_logger(
    logger_name="__main__",
    log_level="info",
    log_type="console",
    file_handler_config=None,
    logging_policy=None,
    cleanup_policy=None,
):
    """
    Returns new logger from the specified options

    log_level = logging.LOG_LEVEL
    log_type = 'console'|'file'
    file_handler_config = {
        'base_file_name': 'str',
        'folder_path': os.path
    }
    logging_policy = 'daily'|'hourly'
    cleanup_policy = {'retain_last_n': {'time_unit': int}}
        below are acceptable units for the format - {"time_unit": int/float}
        days: float = ...,
        seconds: float = ...,
        microseconds: float = ...,
        milliseconds: float = ...,
        minutes: float = ...,
        hours: float = ...,
        weeks: float =
    """
    if not ON_AWS and log_type in ("console", "file"):
        import logging
        import re

        logger = logging.getLogger(logger_name)

        # Set handlers if none set from root logger
        if logger.hasHandlers() == False:
            logger.setLevel(logging.DEBUG)
            stream_handler = logging.StreamHandler()

            # Log Level Switch
            if log_level.upper() == "DEBUG":
                stream_handler.setLevel(logging.DEBUG)
            else:
                stream_handler.setLevel(logging.INFO)

            # Add Formater
            console_formatter = logging.Formatter(
                "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d,%H:%M:%S",
            )
            stream_handler.setFormatter(console_formatter)

            # Add Handler
            logger.addHandler(stream_handler)

            # Add File Handler
            if log_type == "file":
                datetime_format = ""
                file_formatter = logging.Formatter(
                    "%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                    datefmt="%Y-%m-%d,%H:%M:%S",
                )
                base_file_name = file_handler_config["base_file_name"]
                executing_file = os.path.splitext(os.path.basename(__file__))[0]
                folder_path = file_handler_config["folder_path"]
                log_path = os.path.join(folder_path, "logs")

                # Create log folder if it doesnt exist
                if not os.path.exists(log_path):
                    os.makedirs(log_path)

                # Create log file name
                if logging_policy == "daily":
                    datetime_format = "%Y%m%d"
                elif logging_policy == "hourly":
                    datetime_format = "%Y%m%d_%H"
                else:
                    datetime_format = "%Y%m%d_%H%M%S"

                time_now = datetime.datetime.now()
                today_date = time_now.strftime(datetime_format)
                log_file_name = f"{base_file_name}_{today_date}.log"

                # Get absolute path
                log_file_path = os.path.join(log_path, log_file_name)
                file_handler = logging.FileHandler(log_file_path)
                file_handler.setFormatter(file_formatter)

                if log_level == "debug":
                    file_handler.setLevel(logging.DEBUG)
                else:
                    file_handler.setLevel(logging.INFO)

                logger.addHandler(file_handler)

                # If there is a cleanup policy attached then
                # attempt to clean up logs now
                if (
                    cleanup_policy
                    and type(cleanup_policy) == dict
                    and "retain_last_n" in cleanup_policy.keys()
                ):
                    log_file_regex = re.compile(f"^{base_file_name}.*\.log$")
                    # try catch to prevent failure from affecting actual job
                    try:
                        num_files_removed = cleanup_logs(
                            log_path=log_path,
                            log_file_regex=log_file_regex,
                            retain_last_n=cleanup_policy["retain_last_n"],
                        )
                        retain_value = list(
                            dict(cleanup_policy["retain_last_n"]).values()
                        )[0]
                        retain_unit = list(
                            dict(cleanup_policy["retain_last_n"]).keys()
                        )[0]
                        if num_files_removed > 0:
                            logger.info(f"Cleaned up {num_files_removed} old log files")
                        else:
                            logger.info(
                                f"No log files that match cleaning policy: Retain last {retain_value} {retain_unit}"
                            )
                    except Exception as e:
                        logger.warning(f"Failed to cleanup logs: {e}")
    else:
        from aws_lambda_powertools import Logger

        logger = Logger(level=log_level.upper())
    return logger


def cleanup_logs(
    log_path: str,
    log_file_regex: str,
    retain_last_n: dict,
    time_now=datetime.datetime.now(),
) -> int:
    """
    Cleans up specified directory of files that match the regex and has a last modified date
    that is older than the retention policy

    Keyword arguments:
    log_path = Path to directory to cleanup
    log_file_regex = str to be converted to regex to match file names
    retain_last_n = retains last n units of files. kwargs to be passed to datetime.timedelta(**kwargs)
      below are acceptable units for the format - {"unit": int/float}
        days: float = ...,
        seconds: float = ...,
        microseconds: float = ...,
        milliseconds: float = ...,
        minutes: float = ...,
        hours: float = ...,
        weeks: float =
    time_now = overrides what the time is right now for file cleanup

    Returns:
    number of files that were attempted to be deleted: int
    """
    import re

    remove_counter = 0

    # determine cutoff_time
    cutoff_time = time_now - datetime.timedelta(**retain_last_n)

    # find all files and paths in listed directory
    for log_file_name in os.listdir(log_path):
        # only attempt delete if regex matches file name
        if re.search(log_file_regex, log_file_name):
            log_file_path = os.path.join(log_path, log_file_name)

            # get last modified date of file
            log_file_modify_date = datetime.datetime.fromtimestamp(
                os.path.getmtime(log_file_path)
            )

            # remove files that have a last modified date before cut off time
            if log_file_modify_date < cutoff_time:
                remove_file(log_file_path)

                # increment remove counter to track number or attempted removals
                remove_counter += 1
    return remove_counter


def get_log_level(path_to_env=PATH_TO_ENV):
    """Returns the logging log_level  from the .env file"""
    _log_level = "INFO"
    if os.path.exists(path_to_env):
        _log_level = get_environment_variable(
            path_to_env_file=path_to_env, variable_name="log_level"
        )
    elif ON_AWS:
        _log_level = os.getenv("POWERTOOLS_LOG_LEVEL", _log_level)
    return _log_level


def find_text_in_files(root_path, text_regex):
    """
    Searches .py files line by  line matching on regex pattern
    Files are recursively searched starting in root_path
    """
    import re
    import pprint

    os.listdir()
    file_list = []
    for dirpath, dirnames, filenames in os.walk(root_path):
        # skip pycache folders
        if not re.search("__pycache__", dirpath):
            for this_file in filenames:
                if re.search(".*\.py", this_file):
                    this_file_path = os.path.join(dirpath, this_file)
                    file_list.append(this_file_path)
        # self.logger.debug(dirnames)
        # self.logger.debug(filenames)

    result_list = []
    for file_path in file_list:
        with open(file_path) as this_file:
            # self.logger.debug(file_path)
            line_count = 0
            for line in this_file:
                line_count += 1
                result = {}

                if re.search(text_regex, line):
                    result["file"] = file_path
                    result["line"] = line
                    result["line_number"] = line_count
                    result_list.append(result)

    for item in result_list:
        # self.logger.debug(item)
        pprint.pprint(item)

    print(f'Found {len(file_list)} references with regex "{text_regex}"')


def get_core_conn(folder_path) -> dict:
    """Retrieves the connection configuration to the core systems"""
    import json

    file_path = os.path.join(folder_path, "core_conn.json")
    with open(file_path, "r") as f:
        conn_dict = json.load(f)

    return conn_dict
