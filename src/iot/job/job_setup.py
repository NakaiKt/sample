"""
セットアップ: job

queue, in_progressにjobがある場合、以下の最新のもののみ実行
    reboot: succeedステータスに移行
    app_update: 最新のもののみ実行してすべてsucceedステータスに移動
    app_start or app_stop or app_restart: 最後のjobに従い行動。すべてをsucceedに移行
        app_start / restartの場合:  アプリ開始フラグをTrue
        app_stopの場合:             アプリ開始フラグをFalse
"""

import logging
import time
import traceback

from execution.setup import SetupExecution
from awsiot import iotjobs
from defines import JobActionName
from utils.get_job import GetJob
from utils.job_status_update import JobStatusUpdate
from utils.locked_data import LockedData
from utils.mqtt_connection import connection_builder

job_logger = logging.getLogger()


class JobSetup:
    """セットアップ時のジョブ処理"""

    def __init__(self, edge_config: dict):
        """
        Args:
            edge_config (dict): エッジ固定値
        """
        self.__locked_data = LockedData()

        # job関連の通信に使うclient定義
        jobs_client = iotjobs.IotJobsClient(
            mqtt_connection=connection_builder(edge_config=edge_config)
        )

        self.__get_job = GetJob(
            thing_name=edge_config['edge_id'], jobs_client=jobs_client)

        self.__job_status_update = JobStatusUpdate(
            thing_name=edge_config['edge_id'], jobs_client=jobs_client, logger=job_logger)

        self.__setup_execution = SetupExecution()

        self.__is_pending_job_get = False
        self.__job_list = {}
        self.__complete_job_list = []

    def __callback_get_pending_jobs_accepted(self, response: iotjobs.GetPendingJobExecutionsRequest):
        """待機中のjobのsummaryを取得

        Args:
            response (iotjobs.GetPendingJobExecutionsRequest): 取得したjobリスト
        """
        with self.__locked_data.lock:
            for job in response.in_progress_jobs:
                self.__job_list[job.queued_at] = job.job_id

            for job in response.queued_jobs:
                self.__job_list[job.queued_at] = job.job_id

            # queue登録時間で昇順ソート
            self.__job_list = sorted(self.__job_list.items())
            self.__job_list = dict((x, y) for x, y in self.__job_list)
        self.__is_pending_job_get = True

    def __callback_get_pending_jobs_rejected(self, response: iotjobs.GetPendingJobExecutionsRequest):
        """待機中のjob一覧取得に失敗

        Args:
            response (iotjobs.GetPendingJobExecutionsRequest): 取得したresponse
        """
        job_logger.error(response)
        self.__is_pending_job_get = True

    def __callback_get_pending_job_detail_accepted(self, response: iotjobs.DescribeJobExecutionRequest):
        """job詳細取得成功時のcallback
        actionごとに実行関数に振り分ける

        Args:
            response (iotjobs.DescribeJobExecutionRequest): ジョブの詳細
        """
        try:
            execution = response.execution
            job_id = execution.job_id
            job_logger.info('IN_PROGRESS: (job id: %s)', job_id)

            # actionはstepsの先頭のみ対応
            action = execution.job_document['steps'][0]['action']
            job_logger.info('ACTION: %s', action)

            action_name = JobActionName(action['name'])

            if action_name == JobActionName.JOB1:
                # job1
                self.__setup_execution.setup_job1(action=action)
                self.__job_status_update.publish_succeeded(job_id=job_id)
            elif action_name == JobActionName.JOB2:
                # job2
                self.__setup_execution.setup_job2(action=action)
                self.__job_status_update.publish_succeeded(job_id=job_id)
            elif action_name == JobActionName.JOB3:
                # job3
                self.__setup_execution.setup_job3(action=action)
                self.__job_status_update.publish_succeeded(job_id=job_id)
            else:
                # 定義外action
                job_list = [i.value for i in JobActionName]
                job_logger.error(
                    "No Define Action: %s. The Job List is as Follows %s", action["name"], job_list)
                self.__job_status_update.publish_failed(job_id=job_id)

            self.__complete_job_list.append(job_id)

        except Exception:
            job_logger.error(traceback.format_exc())
            self.__job_status_update.publish_failed(job_id=job_id)

    def __callback_get_pending_job_detail_rejected(self, response: iotjobs.DescribeJobExecutionRequest):
        """job詳細取得失敗時のcallback

        Args:
            response (iotjobs.DescribeJobExecutionRequest): エラーメッセージ
        """
        try:
            job_logger.error(response)
            job_id = response.exception.job_id
            self.__job_status_update.publish_failed(job_id=job_id)
        except Exception:
            job_logger.error(traceback.format_exc())
            self.__job_status_update.publish_failed(job_id=job_id)
            raise

    def main(self) -> bool:
        """実行前に指示されていたjobを処理する

        Returns:
            bool: カメラストリームの開始可否
        """
        self.__get_job.get_pending_jobs(
            callback_accepted=self.__callback_get_pending_jobs_accepted,
            callback_rejected=self.__callback_get_pending_jobs_rejected)
        while not self.__is_pending_job_get:
            # job一覧取得が完了するまで待機
            time.sleep(1)

        for job_id in self.__job_list.values():
            # 古いjobから順に取得
            self.__job_status_update.publish_in_progress(job_id=job_id)

            # 詳細を取得して実行
            self.__get_job.get_pending_jobs_detail_by_job_id(
                job_id=job_id,
                callback_accepted=self.__callback_get_pending_job_detail_accepted,
                callback_rejected=self.__callback_get_pending_job_detail_rejected)
            while job_id not in self.__complete_job_list:
                time.sleep(1)

        self.__setup_execution.job_finish()

        job_logger.info('Complete Setup: Do Job %s', self.__complete_job_list)
