""" mqtt接続確立クラス """
import logging
import traceback
from random import randint
from uuid import uuid4

from awscrt import io
from awsiot import mqtt_connection_builder
from retry import retry

job_logger = logging.getLogger()


def __make_client_id() -> str:
    """client_idの生成専用関数
    形式は
    "{edge_id}_{UUID}

    Returns:
        str: 生成したclient_id
    """
    client_id = uuid4().hex

    return client_id


@retry(tries=5, delay=randint(1, 5))
def connection_builder(
    config,
) -> mqtt_connection_builder:
    """mqtt接続の確立

    Args:
        config (dict): 証明書などの設定

    Returns:
        mqtt_connection_builder: mqtt接続
    """
    try:
        # 設定
        client_id = __make_client_id()

        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(
            event_loop_group=event_loop_group)
        client_bootstrap = io.ClientBootstrap(
            event_loop_group=event_loop_group, host_resolver=host_resolver
        )

        # 接続
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=config["iotcore_endpoint"],
            cert_filepath=config["certificate_client"],
            pri_key_filepath=config["certificate_private"],
            client_bootstrap=client_bootstrap,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=6,
        )
        # 接続確認
        connection_feature = mqtt_connection.connect()
        connection_feature.result()

        return mqtt_connection

    except:
        job_logger.error(traceback.format_exc())
        raise


def disconnection(mqtt_connection):
    """mqtt_connectionの切断"""
    disconnection_feature = mqtt_connection.disconnect()
    disconnection_feature.result()
