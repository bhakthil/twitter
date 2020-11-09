
import requests
import os
import json

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    #return os.environ.get("BEARER_TOKEN")
    return 'AAAAAAAAAAAAAAAAAAAAAPUMIAEAAAAAnlAGY3Dcv4MKG4FeVUgkIoNucM8%3D8OvDqDVWM5n0qE32CiCGT3mk5tWw4Z9fXcSlBzB37oVvCvQpIF'


def create_url():
    #query = "from:twitterdev -is:retweet"
    query = "conversation_id:1316019730407329798 -is:retweet"
    conversation_id = "1316019730407329798"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=conversation_id,author_id,created_at,id,public_metrics,referenced_tweets,text"
    expansions = "expansions=referenced_tweets.id,author_id"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(query, tweet_fields, expansions)
    return url


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url, headers)
    print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
