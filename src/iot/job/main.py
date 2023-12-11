"""
jobの処理
"""

import argparse
import json
import logging
import time
from logging.config import fileConfig

from job_downstream import JobDownStream
from job_setup import JobSetup

fileConfig("./logging.txt", disable_existing_loggers=False)
job_logger = logging.getLogger()


def parse_args():
    """起動パラメータのパース"""
    parser = argparse.ArgumentParser(add_help=True)

    # エッジ設定
    parser.add_argument(
        "-ec", "--edge-config-filepath", dest="edge_confit_filepath", type=str,
        default="../configs/config.json",
        help="edge config filepath")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    edge_config = json.load(open(args.edge_config_filepath, "r"))

    # 溜まっているjobを処理
    job_setup = JobSetup(edge_config=edge_config)
    job_setup.main()

    # downstream
    downstream_job = JobDownStream(
        edge_config=edge_config
    )
    downstream_job.main()

    while True:
        time.sleep(1)
