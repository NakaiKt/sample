import logging
import traceback
from awscrt.mqtt import QoS
from awsiot import iotjobs
from awsiot.iotjobs import JobStatus

job_logger = logging.getLogger()


def publish_callback_result(future):
    """

    Args:
        future ():
    """
    try:
        job_logger.info("Publish Callback: %s", future)

    except:
        job_logger.error(traceback.format_exc())


class JobStatusUpdate:
    """サーバーにjobのステータスを報告する
    """

    def __init__(self, thing_name: str, jobs_client: iotjobs.IotJobsClient, logger=None):
        """コンストラクタ

        Args:
            thing_name (str): モノの名前
            jobs_client (iotjobs.IotJobsClient): iotjobsのクライアント
            logger (logging): ログ記録
        """
        self.__thing_name = thing_name
        self.__jobs_client = jobs_client
        self.__logger = logger

    def __status_publish(self, request: iotjobs.UpdateJobExecutionRequest):
        """リクエストをもとにサーバーにpublishする

        Args:
            request (iotjobs.UpdateJobExecutionRequest): ステータス更新リクエスト
        """
        publish_future = self.__jobs_client.publish_update_job_execution(
            request=request, qos=QoS.AT_LEAST_ONCE)
        publish_future.add_done_callback(publish_callback_result)

    def publish_in_progress(self, job_id: str):
        """jobのステータスを実行中(IN_PROGRESS)にする

        Args:
            job_id (str): ジョブID
        """
        request = iotjobs.UpdateJobExecutionRequest(
            thing_name=self.__thing_name,
            job_id=job_id,
            status=JobStatus.IN_PROGRESS
        )
        self.__status_publish(request=request)
        if self.__logger:
            self.__logger.info("IN_PROGRESS: (job id: %s)", job_id)

    def publish_succeeded(self, job_id: str):
        """jobのステータスを完了(SUCCEEDED)にする

        Args:
            job_id (str): ジョブID
        """
        request = iotjobs.UpdateJobExecutionRequest(
            thing_name=self.__thing_name,
            job_id=job_id,
            status=JobStatus.SUCCEEDED
        )
        self.__status_publish(request=request)
        if self.__logger:
            self.__logger.info("SUCCEED: (job id: %s)", job_id)

    def publish_failed(self, job_id: str):
        """jobのステータスを失敗(FAILED)にする

        Args:
            job_id (str): ジョブID
        """
        request = iotjobs.UpdateJobExecutionRequest(
            thing_name=self.__thing_name,
            job_id=job_id,
            status=JobStatus.FAILED
        )
        self.__status_publish(request=request)
        if self.__logger:
            self.__logger.info("FAILED: (job id: %s)", job_id)

    def subscribe_job_status_update(self, callback_accept=None, callback_reject=None):
        """jobのステータスアップデートを受信する

        ステータス更新が成功したとき、callback関数
        callback(response)

        Args:
            callback_accept (, optional): update成功時のcallback関数. Defaults to None.
            callback_reject (, optional): update失敗時のcallback関数. Defaults to None.
        """
        # リクエスト生成
        update_subscribe_request = iotjobs.UpdateJobExecutionSubscriptionRequest(
            thing_name=self.__thing_name,
            job_id="+"
        )

        # subscribe
        subscribe_accepted_future, _ = self.__jobs_client.subscribe_to_update_job_execution_accepted(
            request=update_subscribe_request,
            qos=QoS.AT_LEAST_ONCE,
            callback=callback_accept
        )

        subscribe_rejected_future, _ = self.__jobs_client.subscribe_to_update_job_execution_rejected(
            request=update_subscribe_request,
            qos=QoS.AT_LEAST_ONCE,
            callback=callback_reject
        )

        subscribe_accepted_future.result()
        subscribe_rejected_future.result()
