"""
pdf fileからembeddingを作成する
"""
import sys
sys.path.append("../")

import os
import json
import boto3
import argparse
from langchain.document_loaders import PyPDFLoader
from langchain.embeddings import BedrockEmbeddings
from langchain.indexes import VectorstoreIndexCreator
from langchain.vectorstores import FAISS

from list_invoke_model import display_invoke_model_list


def embedding(origin_file: str, save_folder: str):
    """
    埋め込みファイルを作成する
    出力形式はfaissとpkl

    Args:
        origin_file (str): 元ファイルのパス
        save_folder (str): 保存先のパス
    """

    # 元ファイルの拡張子を取得
    origin_extension = origin_file.split(".")[-1]

    if origin_extension == "pdf":
        loader = PyPDFLoader(origin_file)
    else:
        raise Exception("Not supported extension")
    
    session = boto3.Session(profile_name=os.environ["AWS_PROFILE"], region_name=os.environ["AWS_REGION"])
    bedrock_runtime = session.client(service_name="bedrock-runtime")

    embeddings = BedrockEmbeddings(
        model_id="amazon.titan-embed-text-v1",
        client=bedrock_runtime,
        region_name=os.environ["AWS_REGION"],
    )
    index_creator = VectorstoreIndexCreator(
        vectorstore_cls=FAISS,
        embedding=embeddings,
    )

    index_from_loader = index_creator.from_loaders([loader])
    index_from_loader.vectorstore.save_local(save_folder)

    return {"statusCode": 200, "body": json.dumps("Success")}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-file", dest = "origin_file", default = "./files/Microsoft サービス規約.pdf", type = str, help = "元ファイルのパス")
    parser.add_argument("--save-folder", dest = "save_folder", default="./files", type = str, help = "保存先のパス")
    parser.add_argument("--profile", dest = "profile", default="atl", type = str, help = "aws profile")
    parser.add_argument("--region", dest = "region", default="us-west-2", type = str, help = "aws region")
    args = parser.parse_args()

    display_invoke_model_list(profile = args.profile, region = args.region)

    # aws profileの設定
    os.environ["AWS_PROFILE"] = args.profile
    os.environ["AWS_REGION"] = args.region

    response = embedding(origin_file = args.origin_file, save_folder = args.save_folder)
    
    print(response)