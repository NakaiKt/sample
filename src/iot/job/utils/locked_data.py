"""job関連のスレッドロック実装"""

import logging
import threading

job_logger = logging.getLogger()


class LockedData:
    def __init__(self):
        self.lock = threading.Lock()

        # mqtt接続
        self.is_mqtt_connect = True

        # 実行中のjob
        self.is_working_on_job = False

        # 実行待機中のjob
        self.is_next_job_waiting = False

    def lock_change_for_next_job_start(self) -> bool:
        """現在のエッジ側のjobの処理状態からjobの実行可能の判断をする
        実行可能でない場合、待機中のjobがあることをis_next_job_waitingで設定

        job発行を受信したときに使用

        Returns:
            bool: jobの実行可能フラグ
        """
        # jobの実行可能フラグ
        can_start_job = False

        with self.lock:
            if self.is_working_on_job:
                # 実行中のjobがある場合、待機中のjobとして保留
                self.is_next_job_waiting = True
            else:
                # 実行中のjobがない場合
                can_start_job = True

        return can_start_job

    def lock_check_for_next_job_start(self) -> bool:
        """job実行の最終確認
        実行中のjobの存在、接続の切断がされていない場合のみTrueを返す

        Returns:
            bool: jobの実行可能フラグ
        """
        can_start_job = False

        with self.lock:
            if self.is_working_on_job:
                # 実行中のjobが存在する場合
                job_logger.info("Never mind, already working on a job")
            elif not self.is_mqtt_connect:
                # 接続が切断されている場合
                job_logger.info("Never mind, jobber connection killed")
            else:
                # 実行可能な場合
                can_start_job = True

        self.is_working_on_job = True
        self.is_next_job_waiting = False

        return can_start_job

    def lock_change_by_complete_job(self) -> bool:
        """実行中のjobが完了した事によるロック解除

        Returns:
            bool: 次のjobの有無
        """
        with self.lock:
            # job実行の完了
            self.is_working_on_job = False

        return self.is_next_job_waiting

    def disconnect_mqtt(self):
        """lockクラスの接続ステータスをFalseにする
        """
        with self.lock:
            if self.is_mqtt_connect:
                self.is_mqtt_connect = False
