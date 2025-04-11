import argparse
import asyncio
import logging
from asyncio import Queue, create_task
from datetime import datetime
from typing import List

import aiohttp
from settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=__file__.replace(".py", ".log"),
)
logger = logging.getLogger(__name__)


def parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="A script to add manual applies from hh.ru to notion."
        "Recieves a date, all applies created after this date will be added."
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default="4",
        help="Number of async workers",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test run (no applies just logging)",
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.fromisoformat(s),
        help="Date and time in ISO 8601 format (YYYY-MM-DD HH:MM:SS+Z),"
        "for example, 2025-01-01 12:00:00+05",
        required=True,
    )
    args = parser.parse_args()
    return args.workers, args.test, args.date


async def fill_queue(session: aiohttp.ClientSession, queue: Queue) -> None:
    response = await session.get(
        url=settings.negotiation_url,
        headers=settings.hh_headers,
        params={"order_by": "created_at", "order": "desc"},
    )
    if response.status != 200:
        logger.error(
            f"Error fetching {settings.vacancies_url}: {response.status}\n{await response.text()}"
        )
    else:
        response_json = await response.json()
        pages, per_page = response_json["pages"], response_json["per_page"]
        logger.info(
            f"Got {response_json['found']} negotiations, {pages} pages, {per_page} per page"
        )
        for i in range(pages):
            logger.info(f"Add block ({i},{per_page}) to queue")
            await queue.put((i, per_page))


async def fetch_negotiation_page(
    session: aiohttp.ClientSession,
    queue: Queue,
    applies_after_date: datetime,
    test_run: bool,
) -> None:
    while True:
        page, per_page = await queue.get()
        logger.info(f"Fetch block ({page},{per_page}) from queue")
        try:
            negotiations = await fetch_negotiations_from_page(
                session=session, page=page, per_page=per_page
            )
            for idx, negotiation in enumerate(negotiations):
                if (
                    datetime.strptime(negotiation["created_at"], "%Y-%m-%dT%H:%M:%S%z")
                    < applies_after_date
                ):
                    continue
                logger.info(
                    f"Page={page:02d} idx={idx:02d}: "
                    f"{negotiation['created_at']} {negotiation['id']} "
                    f"{negotiation['vacancy']['name']}"
                    f"{negotiation['vacancy']['employer']['name']} "
                )
                if not test_run:
                    await add_apply_to_notion(
                        session=session,
                        company=negotiation["vacancy"]["employer"]["name"],
                        position=negotiation["vacancy"]["name"],
                        url=negotiation["vacancy"]["alternate_url"],
                        negotiation_url=f"/negotiations/{negotiation['id']}",
                    )
        except Exception as e:
            logger.error(
                f"Fetch block ({page},{per_page}) from queue finished with error {str(e)}"
            )
        finally:
            queue.task_done()


async def fetch_negotiations_from_page(
    session: aiohttp.ClientSession, page: int, per_page: int
) -> List:
    response = await session.get(
        url=settings.negotiation_url,
        params={"page": page, "per_page": per_page},
        headers=settings.hh_headers,
    )
    if response.status != 200:
        logger.error(
            f"Error fetching {settings.vacancies_url} with page={page} per_page={per_page}: {response.status}\n{await response.text()}"
        )
        return []
    else:
        response_json = await response.json()
        logger.info(f"Page={page} got {len(response_json['items'])} vacancies")
        return response_json["items"]


async def add_apply_to_notion(
    session: aiohttp.ClientSession,
    company: str,
    position: str,
    url: str,
    negotiation_url: str,
) -> None:
    if not settings.notion_enabled:
        return

    new_page_props = {
        "COMPANY": {"title": [{"text": {"content": company}}]},
        "POSITION": {"rich_text": [{"type": "text", "text": {"content": position}}]},
        "APPLICATION DATE": {"date": {"start": settings.notion_apply_date}},
        "JOB POST": {"url": url},
        "STATUS": {"status": {"name": "Applied"}},
        "HH negotiation url": {"url": negotiation_url},
        "RESUME USED": {"relation": [{"id": settings.notion_resume_id}]},
    }
    response = await session.post(
        url=f"{settings.notion_api_url}/pages",
        headers=settings.notion_headers,
        json={
            "parent": {"database_id": settings.notion_db_id},
            "properties": new_page_props,
        },
        proxy=settings.notion_proxy,
    )
    if response.status != 200:
        logger.error(
            f"NOTION: Could not create a page for {url}: {response.status} {await response.text()}"
        )
    else:
        response_json = await response.json()
        logger.info(f"NOTION: Page created with id: {response_json['id']}")


async def main(workers_num: int, applies_after_date: datetime, test_run: bool) -> None:
    if not settings.notion_enabled:
        logger.info("NOTION: Notion is disabled")

    queue = Queue()
    async with aiohttp.ClientSession() as session:
        await fill_queue(session, queue)
        workers = [
            create_task(
                fetch_negotiation_page(
                    session=session,
                    queue=queue,
                    applies_after_date=applies_after_date,
                    test_run=test_run,
                )
            )
            for _ in range(workers_num)
        ]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
    workers_num, test_run, applies_after_date = parse_args()
    asyncio.run(
        main(
            workers_num=workers_num,
            applies_after_date=applies_after_date,
            test_run=test_run,
        )
    )
