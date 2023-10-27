import boto3


def display_invoke_model_list(profile: str, region: str):
    session = boto3.Session(profile_name=profile, region_name=region)
    bedrock = session.client('bedrock')

    print("List of modelId")
    for model in bedrock.list_foundation_models().get('modelSummaries'):
        print(model.get('modelId'))
    print("-------------------------")