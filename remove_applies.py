import argparse
import asyncio
import logging
from asyncio import Queue, create_task
from pathlib import Path

import aiohttp
from settings import settings

current_file = Path(__file__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-5s - %(message)s",
    filename=Path(
        current_file.parent, "logs", current_file.name.replace(".py", ".log")
    ),
)
logger = logging.getLogger(__name__)


def parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="Removes applies with status 'Wrong' from Notion and HH"
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
                {"property": "STATUS", "status": {"equals": "Wrong"}},
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


async def remove_application(session: aiohttp.ClientSession, queue: Queue) -> None:
    while True:
        page_id, hh_url = await queue.get()
        try:
            if await application_removed(session=session, hh_url=hh_url):
                await remove_application_from_notion(session=session, page_id=page_id)
        except Exception as e:
            logger.error(
                f"Processing page {page_id} with HH url {hh_url} from queue finished with error {str(e)}"
            )
        finally:
            queue.task_done()


async def application_removed(session: aiohttp.ClientSession, hh_url: str) -> bool:
    hh_url_delete = hh_url.strip("/").replace("negotiations", "negotiations/active")
    response = await session.delete(
        url=f"{settings.hh_api_url}/{hh_url_delete}",
        headers=settings.hh_headers,
    )
    if response.status != 204:
        logger.error(
            f"Couldn't fetch HH url {hh_url_delete}: {response.status} {await response.text()}"
        )
        return False
    return True


async def remove_application_from_notion(
    session: aiohttp.ClientSession, page_id: str
) -> None:
    response = await session.patch(
        url=f"{settings.notion_api_url}/pages/{page_id}",
        headers=settings.notion_headers,
        json={"archived": True},
        proxy=settings.notion_proxy,
    )
    if response.status != 200:
        logger.error(
            f"Couldn't remove page {page_id}: {response.status} {await response.text()}"
        )
    else:
        logger.info(f"Removed page {page_id}")


async def main(workers_num: int) -> None:
    if not settings.notion_enabled:
        logger.error("Notion credentials are not provided, exiting")
        return

    queue = Queue()
    async with aiohttp.ClientSession() as session:
        await fill_queue(session=session, queue=queue)
        workers = [
            create_task(remove_application(session=session, queue=queue))
            for _ in range(workers_num)
        ]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
    workers_num = parse_args()
    asyncio.run(main(workers_num=workers_num))
