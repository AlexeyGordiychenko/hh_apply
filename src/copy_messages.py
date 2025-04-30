import argparse
from typing import Dict

import requests
from tqdm import tqdm

from settings import settings


def parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="A script to add hh messages to notion"
    )
    parser.add_argument(
        "-i",
        "--id",
        help="Negotiation ID",
    )

    args = parser.parse_args()
    return args.id


def add_messages(negotiation_id: str) -> None:
    with tqdm(total=1, desc="Getting notion page id") as pbar:
        notion_page_id = get_notion_page(negotiation_id=negotiation_id)
        pbar.update(1)
    if not notion_page_id:
        print("Can't get notion page id")
        return
    with tqdm(total=1, desc="Getting messages from HH") as pbar:
        messages = get_messages(negotiation_id=negotiation_id)
        pbar.update(1)
    for message in tqdm(messages, desc="Adding messages to notion"):
        add_message_to_notion(page_id=notion_page_id, message=message)


def get_notion_page(negotiation_id: str) -> str:
    db_filter = {
        "filter": {
            "and": [
                {
                    "property": "HH negotiation url",
                    "url": {"equals": f"/negotiations/{negotiation_id}"},
                },
            ]
        }
    }

    response = requests.post(
        url=f"{settings.notion_api_url}/databases/{settings.notion_db_id}/query",
        headers=settings.notion_headers,
        json=db_filter,
        proxies={"http": settings.notion_proxy, "https": settings.notion_proxy},
    )
    if response.status_code == 200:
        return response.json()["results"][0]["id"]
    return ""


def get_messages(negotiation_id: str):
    response = requests.get(
        url=f"{settings.negotiation_url}/{negotiation_id}/messages",
        headers=settings.hh_headers,
    )
    if response.status_code == 200:
        return response.json()["items"][1:]
    return []


def add_message_to_notion(page_id: str, message: Dict):
    response = requests.patch(
        url=f"{settings.notion_api_url}/blocks/{page_id}/children",
        headers=settings.notion_headers,
        proxies={"http": settings.notion_proxy, "https": settings.notion_proxy},
        json={
            "children": [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": message["text"],
                                },
                            }
                        ],
                        "color": "default"
                        if message["author"]["participant_type"] == "applicant"
                        else "gray_background",
                    },
                },
                {
                    "object": "block",
                    "type": "divider",
                    "divider": {},
                },
            ]
        },
    )

    if response.status_code != 200:
        print(f"Can't add message {message} to notion {response.text}")


if __name__ == "__main__":
    negotiation_id = parse_args()
    if negotiation_id:
        add_messages(negotiation_id)
