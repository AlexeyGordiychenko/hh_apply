import argparse
import asyncio
import logging

from settings import settings
import aiohttp
from asyncio import Queue, create_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="process_rejection.log",
)
logger = logging.getLogger(__name__)


def parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="A script to apply to vacancies on hh.ru"
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default="4",
        help="Number of async workers",
    )
    args = parser.parse_args()
    return args.workers


async def fill_queue(session: aiohttp.ClientSession, queue: Queue) -> None:
    db_filter = {
        "filter": {
            "and": [
                {"property": "STATUS", "status": {"equals": "Applied"}},
                {"property": "HH negotiation url", "url": {"is_not_empty": True}},
            ]
        }
    }

    response = await session.post(
        url=f"{settings.notion_api_url}/databases/{settings.notion_db_id}/query",
        headers=settings.notion_headers,
        json=db_filter,
        proxy=settings.notion_proxy,
    )
    if response.status != 200:
        logger.error(
            f"Couldn't query database: {response.status} {await response.text()}"
        )
    else:
        response_json = await response.json()
        results = response_json["results"]
        logger.info(f"Received {len(results)} results")

        for page in results:
            page_id, hh_url = (
                page["id"],
                page["properties"]["HH negotiation url"]["url"],
            )
            logger.info(f"Add page {page_id} with HH url {hh_url} to queue")
            await queue.put((page_id, hh_url))


async def process_application_status(
    session: aiohttp.ClientSession, queue: Queue
) -> None:
    while True:
        page_id, hh_url = await queue.get()
        try:
            if await application_rejected(session=session, hh_url=hh_url):
                await update_notion_status(session=session, page_id=page_id)
            else:
                logger.info(
                    f"Processing page {page_id} with HH url {hh_url} from queue: application is not rejected"
                )
        except Exception as e:
            logger.error(
                f"Processing page {page_id} with HH url {hh_url} from queue finished with error {str(e)}"
            )
        finally:
            queue.task_done()


async def application_rejected(session: aiohttp.ClientSession, hh_url: str) -> bool:
    response = await session.get(
        url=f"{settings.hh_api_url}/{hh_url.strip('/')}", headers=settings.hh_headers
    )
    if response.status != 200:
        logger.error(
            f"Couldn't fetch HH url {hh_url}: {response.status} {await response.text()}"
        )
        return False
    else:
        response_json = await response.json()
        return response_json["state"]["id"] == "discard"


async def update_notion_status(session: aiohttp.ClientSession, page_id: str) -> None:
    response = await session.patch(
        url=f"{settings.notion_api_url}/pages/{page_id}",
        headers=settings.notion_headers,
        json={"properties": {"STATUS": {"status": {"name": "Unsuccessful"}}}},
        proxy=settings.notion_proxy,
    )
    if response.status != 200:
        logger.error(
            f"Couldn't update page {page_id}: {response.status} {await response.text()}"
        )
    else:
        logger.info(f"Updated page {page_id}: status set to Unsuccessful")


async def main(workers_num: int) -> None:
    if not settings.notion_enabled:
        logger.error("Notion credentials are not provided, exiting")
        return

    queue = Queue()
    async with aiohttp.ClientSession() as session:
        await fill_queue(session=session, queue=queue)
        workers = [
            create_task(process_application_status(session=session, queue=queue))
            for _ in range(workers_num)
        ]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
    workers_num = parse_args()
    asyncio.run(main(workers_num=workers_num))
