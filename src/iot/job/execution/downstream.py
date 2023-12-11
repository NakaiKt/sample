import logging

job_logger = logging.getLogger()


class DownstreamExecution:
    def downstream_job1(self, action: dict):
        """job1の処理
        """
        job_logger.info('JOB EXECUTION: JOB1')
        print(action)

    def downstream_job2(self, action: dict):
        """job2の処理
        """
        job_logger.info('JOB EXECUTION: JOB2')
        print(action)

    def downstream_job3(self, action: dict):
        """job3の処理
        """
        job_logger.info('JOB EXECUTION: JOB3')
        print(action)
