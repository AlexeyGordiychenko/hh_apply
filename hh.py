import argparse
import asyncio
import logging
from typing import List, Tuple

from settings import settings
import aiohttp
from asyncio import Queue, create_task


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="hh.log",
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
    response = await session.get(
        url=settings.vacancies_url, headers=settings.hh_headers
    )
    if response.status != 200:
        logger.error(
            f"Error fetching {settings.vacancies_url}: {response.status}\n{await response.text()}"
        )
    else:
        response_json = await response.json()
        pages, per_page = response_json["pages"], response_json["per_page"]
        logger.info(
            f"Got {response_json['found']} vacancies, {pages} pages, {per_page} per page"
        )
        for i in range(pages):
            logger.info(f"Add block ({i},{per_page}) to queue")
            await queue.put((i, per_page))


async def fetch_vacancy_page(session: aiohttp.ClientSession, queue: Queue) -> None:
    while True:
        page, per_page = await queue.get()
        logger.info(f"Fetch block ({page},{per_page}) from queue")
        try:
            vacancies = await fetch_vacancies_from_page(
                session=session, page=page, per_page=per_page
            )
            for idx, vacancy in enumerate(vacancies):
                status, negotiation_url, text = await apply_to_vacancy(
                    session=session, vacancy_id=vacancy["id"]
                )
                logger.info(
                    f"Page={page} idx={idx}: {vacancy['id']} {vacancy['name']} {vacancy['employer']['name']} "
                    f"APPLIED with status {status} got negotiation url: {negotiation_url} and text: {text}"
                )
                if status == 201:
                    await add_apply_to_notion(
                        session=session,
                        company=vacancy["employer"]["name"],
                        position=vacancy["name"],
                        url=vacancy["alternate_url"],
                        negotiation_url=negotiation_url,
                    )
        except Exception as e:
            logger.error(
                f"Fetch block ({page},{per_page}) from queue finished with error {str(e)}"
            )
        finally:
            queue.task_done()


async def fetch_vacancies_from_page(
    session: aiohttp.ClientSession, page: int, per_page: int
) -> List:
    response = await session.get(
        url=settings.vacancies_url,
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


async def apply_to_vacancy(
    session: aiohttp.ClientSession, vacancy_id: int
) -> Tuple[int, str, str]:
    response = await session.post(
        url=settings.negotiation_url,
        headers=settings.hh_headers,
        data={
            "vacancy_id": vacancy_id,
            "resume_id": settings.resume_id,
            "message": settings.cover_letter,
        },
    )
    return response.status, response.headers.get("Location", ""), await response.text()


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


async def main(workers_num: int) -> None:
    if not settings.notion_enabled:
        logger.info("NOTION: Notion is disabled")

    queue = Queue()
    async with aiohttp.ClientSession() as session:
        await fill_queue(session, queue)
        workers = [
            create_task(fetch_vacancy_page(session, queue)) for _ in range(workers_num)
        ]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
    workers_num = parse_args()
    asyncio.run(main(workers_num=workers_num))
