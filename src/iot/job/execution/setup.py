"""
セットアップ時の実行ジョブ
"""
import logging

job_logger = logging.getLogger()


class SetupExecution:
    """セットアップ時の実行ジョブクラス"""

    def setup_job1(self, action: dict):
        """job1の処理
        """
        job_logger.info('JOB SETUP EXECUTION: JOB1')
        print(action)

    def setup_job2(self, action: dict):
        """job2の処理
        """
        job_logger.info('JOB SETUP EXECUTION: JOB2')
        print(action)

    def setup_job3(self, action: dict):
        """job3の処理
        """
        job_logger.info('JOB SETUP EXECUTION: JOB3')
        print(action)
