import boto3
import json

import argparse

def bedrock_text_sample(bedrock: boto3.client, model_id: str = 'anthropic.claude-v2'):

    input_prompt = input("Input prompt: ")

    body = json.dumps({
        'prompt': 'Human: ' + input_prompt + 'Assistant: ',
        'max_tokens_to_sample': 100
    })


    response = bedrock.invoke_model(
        modelId=model_id,
        body=body,
    )

    if model_id == "anthropic.claude-v2":
        response_body = json.loads(response.get('body').read().decode())
        output_text = response_body.get('completion')
        print(output_text)

    elif model_id == "ai21.j2-ultra-v1":
        response_body = json.loads(response.get('body').read().decode())
        output_text = response_body.get('completions')[0].get('data').get('text')
        print(output_text)

def bedrock_chat_sample(bedrock: boto3.client):
    body = json.dumps({
        'prompt': '\n\nHuman:write an essay for living on mars in 1000 words\n\nAssistant:',
        'max_tokens_to_sample': 100
    })

    response = bedrock.invoke_model_with_response_stream(
        modelId='anthropic.claude-v2',
        body=body
    )

    stream = response.get('body')
    if stream:
        for event in stream:
            chunk = event.get('chunk')
            if chunk:
                print(json.loads(chunk.get('bytes').decode()))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bedrock API sample')
    parser.add_argument('--profile', default='atl', help='AWS profile name')
    parser.add_argument('--region', default='us-west-2', help='AWS region name')
    parser.add_argument('--prayground-mode', dest='prayground_mode', default = 'text', help='text or chat')
    args = parser.parse_args()

    profile = args.profile
    session = boto3.Session(profile_name=profile, region_name=args.region)
    bedrock = session.client('bedrock')

    print("List of modelId")
    for model in bedrock.list_foundation_models().get('modelSummaries'):
        print(model.get('modelId'))
    print("-------------------------")

    bedrock = session.client('bedrock-runtime')

    if args.prayground_mode == 'chat':
        print("Chat sample")
        bedrock_chat_sample(bedrock=bedrock)

    elif args.prayground_mode == 'text':
        print("Text sample")
        bedrock_text_sample(bedrock=bedrock)

    else:
        print("Invalid mode")