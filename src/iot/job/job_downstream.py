"""
jobの処理

queueにあるjobを取得
jobに応じて処理を実行する

jobリスト
    reboot: 端末を再起動させる
    app_update: applicationのアップデートを行いapplicationを再起動する
    app_start: applicationを開始する
    app_stop: applicationを停止する
    app_restart: applicationを再起動する
"""

import logging
import threading
import traceback

from awsiot import iotjobs
from defines import JobActionName
from execution.downstream import DownstreamExecution
from utils.get_job import GetJob
from utils.job_status_update import JobStatusUpdate
from utils.locked_data import LockedData
from utils.mqtt_connection import connection_builder, disconnection

job_logger = logging.getLogger()


class JobDownStream:
    """jobの処理"""

    def __init__(self, edge_config: dict):
        """
        Args:
            config_filepath (str): 設定ファイルパス
            edge_config (dict): エッジ固定値
            application (): アプリケーションインスタンス
            application_restart (): アプリケーション再起動関数
            is_application_start (bool): アプリ開始フラグ
        """
        self.__locked_data = LockedData()

        self.__execution = DownstreamExecution()

        self.__mqtt_connection = connection_builder(edge_config=edge_config)

        jobs_client = iotjobs.IotJobsClient(
            mqtt_connection=self.__mqtt_connection
        )
        self.__get_job = GetJob(
            thing_name=edge_config["edge_id"],
            jobs_client=jobs_client
        )
        self.__job_status_update = JobStatusUpdate(
            thing_name=edge_config["edge_id"],
            jobs_client=jobs_client,
            logger=job_logger
        )

    def __callback_next_job_summary(self, response: iotjobs.NextJobExecutionChangedSubscriptionRequest):
        """次のjobが更新されたときに概要を取得

        Args:
            response (iotjobs.NextJobExecutionChangedSubscriptionRequest): 取得したjobの概要
        """

        try:
            if response.execution:
                # 他のjobが実行中でない場合True
                can_start_job = self.__locked_data.lock_change_for_next_job_start()

                if can_start_job:
                    self.__get_job.request_next_job_for_start(
                        locked_data=self.__locked_data)

            else:
                job_logger.info(
                    "job_id: None, Waiting for further jobs...")

        except Exception:
            job_logger.error(traceback.format_exc())
            raise

    def __callback_next_job_for_start_accepted(self, response: iotjobs.StartNextPendingJobExecutionRequest):
        """次のjobの詳細を取得し、ステータスをIN_PROGRESSに移行
        actionごとに振り分ける

        Args:
            response (iotjobs.StartNextPendingJobExecutionRequest): ジョブの詳細
        """
        try:
            if response.execution:
                # NOTE: IN_PROGRESSへのupdateはsubscribe_next_job_for_startが行う
                job_thread = threading.Thread(
                    target=lambda: self.__switch_job(
                        response.execution.job_id, response.execution.job_document
                    ),
                    name="job_thread"
                )
                job_thread.start()

            else:
                job_logger.info(
                    "Request to start next job was accepted, but there are no jobs to done, Waiting for further jobs...")
                self.__done_working_on_job()

        except Exception:
            job_logger.error(traceback.format_exc())
            self.__done_working_on_job()
            raise

    def __callback_next_job_for_start_rejected(self, response: iotjobs.StartNextPendingJobExecutionRequest):
        """ジョブ詳細取得に失敗したため次のジョブを試みる

        Args:
            response (iotjobs.StartNextPendingJobExecutionRequest): エラーメッセージ
        """
        job_logger.error(response)
        self.__done_working_on_job()

    def __callback_job_status_update_accepted(self, response: iotjobs.UpdateJobExecutionResponse):
        """ステータス更新後、次のjobを実行する

        Args:
            response (iotjobs.UpdateJobExecutionResponse):
        """
        self.__done_working_on_job()

    def __callback_job_status_update_rejected(self, response: iotjobs.UpdateJobExecutionResponse):
        """ステータス更新が失敗したため、次のジョブを試みる

        Args:
            response (iotjobs.UpdateJobExecutionResponse): エラーメッセージ
        """
        job_logger.error(response)
        self.__done_working_on_job()

    def __done_working_on_job(self):
        """job完了によるlock解除を行い、次のjobがあれば詳細をrequestする
        """
        try_again = self.__locked_data.lock_change_by_complete_job()

        if try_again:
            self.__get_job.request_next_job_for_start(
                locked_data=self.__locked_data)

    def __switch_job(self, job_id: str, job_document: dict):
        """jobを受け取りaction名によって振り分ける

        Args:
            job_id (str): ジョブID
            job_document (dict): ジョブの詳細ドキュメント
        """
        try:
            self.__job_status_update.publish_in_progress(job_id=job_id)
            # actionはstepsの先頭のみ対応
            action = job_document["steps"][0]["action"]
            job_logger.info('ACTION: %s', action)

            action_name = JobActionName(action['name'])

            if action_name == JobActionName.JOB1:
                # job1
                self.__execution.downstream_job1(action=action)
            elif action_name == JobActionName.JOB2:
                # job2
                self.__execution.downstream_job2(action=action)
                self.__job_status_update.publish_succeeded(job_id=job_id)
            elif action_name == JobActionName.JOB3:
                # job3
                self.__execution.downstream_job3(action=action)
                self.__job_status_update.publish_succeeded(job_id=job_id)
            else:
                # 定義外action
                job_list = [i.value for i in JobActionName]
                job_logger.error(
                    "No Define Action: %s. The Job List is as Follows %s", action["name"], job_list)
                self.__job_status_update.publish_failed(job_id=job_id)

        except Exception:
            job_logger.error(traceback.format_exc())
            self.__job_status_update.publish_failed(job_id=job_id)

    def main(self):
        """jobの開始
        更新取得
        詳細取得
        ステータス更新
        を設定する
        """
        job_logger.info("Run Job")
        self.__get_job.subscribe_next_jobs_summary(
            callback=self.__callback_next_job_summary
        )
        self.__get_job.subscribe_next_job_for_start(
            callback_accepted=self.__callback_next_job_for_start_accepted,
            callback_rejected=self.__callback_next_job_for_start_rejected
        )
        self.__job_status_update.subscribe_job_status_update(
            callback_accept=self.__callback_job_status_update_accepted,
            callback_reject=self.__callback_job_status_update_rejected
        )

        self.__get_job.request_next_job_for_start(
            locked_data=self.__locked_data
        )
        return True

    def exit(self):
        """jobの停止
        mqtt接続を切断する
        """
        self.__locked_data.disconnect_mqtt()
        disconnection(self.__mqtt_connection)
        job_logger.info("Kill Job")
