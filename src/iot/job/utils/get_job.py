"""
待機中のjob
指定したjobの詳細
次のjobの概要
次のjobの詳細

メッセージ内容、API等についての詳細は以下参照
https://docs.aws.amazon.com/ja_jp/iot/latest/developerguide/jobs-mqtt-https-api.html
"""
import logging
import traceback

from awscrt.mqtt import QoS
from awsiot import iotjobs

job_logger = logging.getLogger()


class GetJob:
    def __init__(self, thing_name: str, jobs_client: iotjobs.IotJobsClient):
        self.thing_name = thing_name
        self.jobs_client = jobs_client

    def get_pending_jobs(self, callback_accepted=None, callback_rejected=None):
        """現在のjobの一覧を取得
        成功時、callback関数は以下のようにjobを取得可能
            callback(response):
                response.queued_jobs: queueに入っているjob一覧
                response.in_progress_jobs: in_progressに入っているjob一覧

                for job in response.queue_jobs:
                    job.job_id: ジョブID
                    job.last_update_at: 最終更新日
                    job.queue_at: queueに入れられてからの時間
                    start_at: 実行が開始された時間 (queuedの場合, None)
                    etc...

        Args:
            callback_accepted (, optional): 取得成功時に呼び出すcallback関数. Defaults to None.
            callback_rejected (, optional): 取得失敗時に呼び出すcallback関数. Defaults to None.
        """
        try:
            # リクエスト生成
            get_jobs_request = iotjobs.GetPendingJobExecutionsRequest(
                thing_name=self.thing_name)

            # subscribe
            jobs_request_future_accepted, _ = self.jobs_client.subscribe_to_get_pending_job_executions_accepted(
                request=get_jobs_request,
                qos=QoS.AT_LEAST_ONCE,
                callback=callback_accepted
            )

            jobs_request_future_rejected, _ = self.jobs_client.subscribe_to_get_pending_job_executions_rejected(
                request=get_jobs_request,
                qos=QoS.AT_LEAST_ONCE,
                callback=callback_rejected
            )

            jobs_request_future_accepted.result()
            jobs_request_future_rejected.result()

            # サーバーにリクエスト
            get_jobs_request_feature = self.jobs_client.publish_get_pending_job_executions(
                request=get_jobs_request,
                qos=QoS.AT_LEAST_ONCE
            )
            get_jobs_request_feature.result()

        except:
            job_logger.error(traceback.format_exc())
            raise

    def get_pending_jobs_detail_by_job_id(self, job_id: str, callback_accepted=None, callback_rejected=None):
        """job_idで指定したjobの詳細を取得する
        取得したjobのステータスは自動的にIN_PROGRESSになる
        成功時、callback関数は以下のようにjobを取得可能
            callback(response):
                # 存在しない場合はexecution = None
                execution = response.execution

                execution.job_id: ジョブID
                execution.job_document:jobドキュメント (job発行時に指定するジョブファイルの内容)
                execution.last_update_at: 最終更新日
                etc...

        Args:
            job_id (str): ジョブID
            callback_accepted (, optional): 取得成功時のcallback関数. Defaults to None.
            callback_rejected (, optional): 取得失敗時のcallback関数. Defaults to None.
        """
        try:
            # リクエスト生成
            get_job_request = iotjobs.DescribeJobExecutionRequest(
                thing_name=self.thing_name,
                job_id=job_id
            )

            # subscribe
            job_request_feature_accepted, _ = self.jobs_client.subscribe_to_describe_job_execution_accepted(
                request=get_job_request,
                qos=QoS.AT_LEAST_ONCE,
                callback=callback_accepted
            )

            job_request_feature_rejected, _ = self.jobs_client.subscribe_to_describe_job_execution_rejected(
                request=get_job_request,
                qos=QoS.AT_LEAST_ONCE,
                callback=callback_rejected
            )

            job_request_feature_accepted.result()
            job_request_feature_rejected.result()

            # サーバーにリクエスト
            get_job_request_feature = self.jobs_client.publish_describe_job_execution(
                request=get_job_request,
                qos=QoS.AT_LEAST_ONCE
            )
            get_job_request_feature.result()

        except:
            job_logger.error(traceback.format_exc())
            raise

    def subscribe_next_jobs_summary(self, callback=None):
        """次のjobに変更があった場合に受信する
        受信とジョブの実行を行うなら下のsubscribe_next_job_for_startが良い

        callback関数は以下のようにjobを取得可能
            callback(response):
                execution = response.execution

                execution.job_id: ジョブID
                execution.job_document: jobドキュメント (job発行時に指定するジョブファイルの内容)
                execution.last_update_at: 最終更新日
                etc...

        Args:
            callback (, optional): callback関数. Defaults to None.
        """
        changed_subscribe_request = iotjobs.NextJobExecutionChangedSubscriptionRequest(
            thing_name=self.thing_name
        )

        subscribe_feature, _ = self.jobs_client.subscribe_to_next_job_execution_changed_events(
            request=changed_subscribe_request,
            qos=QoS.AT_LEAST_ONCE,
            callback=callback
        )
        subscribe_feature.result()

    def subscribe_next_job_for_start(self, callback_accepted=None, callback_rejected=None):
        """保留中のjob (QUEUED, IN_PROGRESS) を取得 (優先度: IN_PROGRESS > QUEUED)
        より古いものから順に取得される
        ステータスがQUEUED -> IN_PROGRESSに変更される (元々IN_PROGRESSの場合は変更なし)
        subscribe後のjob取得はrequest_next_job_for_startにて行う

        成功時、callback関数は以下のようにjobを取得可能
            callback(response):
                # 存在しない場合はexecution = None
                execution = response.execution

                execution.job_id: ジョブID
                execution.job_document:jobドキュメント (job発行時に指定するジョブファイルの内容)
                execution.last_update_at: 最終更新日
                etc...

        Args:
            callback_accepted (, optional): 取得成功時のcallback関数. Defaults to None.
            callback_rejected (, optional): 取得失敗時のcallback関数. Defaults to None.
        """
        # リクエスト生成
        start_subscribe_request = iotjobs.StartNextPendingJobExecutionSubscriptionRequest(
            thing_name=self.thing_name
        )

        # subscribe
        subscribe_accepted_future, _ = self.jobs_client.subscribe_to_start_next_pending_job_execution_accepted(
            request=start_subscribe_request,
            qos=QoS.AT_LEAST_ONCE,
            callback=callback_accepted
        )

        subscribe_rejected_future, _ = self.jobs_client.subscribe_to_start_next_pending_job_execution_rejected(
            request=start_subscribe_request,
            qos=QoS.AT_LEAST_ONCE,
            callback=callback_rejected
        )

        subscribe_accepted_future.result()
        subscribe_rejected_future.result()

    def request_next_job_for_start(self, locked_data):
        """次のjobの詳細取得を実行のためにrequest
        NOTE: jobのsubscribeはsubscribe_next_job_for_startにて行う

        Args:
            locked_data (LockData): threadのロックインスタンス
        """
        can_start_job = locked_data.lock_check_for_next_job_start()

        if can_start_job:
            job_logger.info("Publish: get next job request")
            request = iotjobs.StartNextPendingJobExecutionRequest(
                thing_name=self.thing_name
            )
            self.jobs_client.publish_start_next_pending_job_execution(
                request=request,
                qos=QoS.AT_LEAST_ONCE,
            )
