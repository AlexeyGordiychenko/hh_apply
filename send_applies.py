import argparse
import asyncio
import logging
from asyncio import Queue, create_task
from enum import Enum
from pathlib import Path
from typing import List, Optional

import aiohttp
from exceptions import HH_Limit_Exceeded_Error
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


class SearchType(Enum):
    SIMILAR = "similar"
    QUERY = "query"


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
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test run (no applies just logging)",
    )
    parser.add_argument(
        "-s",
        "--search",
        type=SearchType,
        choices=list(SearchType),
        required=True,
        help="Search similar or query",
    )
    args = parser.parse_args()
    return args.workers, args.test, args.search


async def get_vacancies_response(
    session: aiohttp.ClientSession, search: SearchType, page: int = 0
) -> Optional[dict]:
    if search == SearchType.SIMILAR:
        response = await session.get(
            url=settings.vacancies_url,
            params={"page": page},
            headers=settings.hh_headers,
        )
    elif search == SearchType.QUERY:
        # TODO: use .yml file for this
        params = [
            ("text", "python"),
            ("professional_role", 96),
            ("search_field", "name"),
            (
                "excluded_text",
                "senior,сеньор,lead,преподаватель,автор,наставник,руководитель,репетитор,старший,ведущий,главный,techlead",
            ),
            ("work_format", "REMOTE"),
            ("page", page),
        ]
        response = await session.get(
            url=f"{settings.hh_api_url}/vacancies",
            params=params,
            headers=settings.hh_headers,
        )
    else:
        return None
    if response.status != 200:
        logger.error(
            f"Error fetching page {page} with {response.url}: {response.status}\n{await response.text()}"
        )
        return None
    else:
        response_json = await response.json()
        return response_json


async def fill_queue(queue: Queue, start_page: int, end_page: int) -> None:
    for i in range(start_page, end_page):
        logger.info(f"Add page {i} to queue")
        await queue.put(i)


async def fetch_vacancy_page(
    session: aiohttp.ClientSession, queue: Queue, test_run: bool, search: SearchType
) -> None:
    while True:
        page = await queue.get()
        response_json = await get_vacancies_response(
            session=session, page=page, search=search
        )
        vacancies = await process_vacancies_response(
            response_json=response_json, queue=queue, page=page
        )
        for idx, vacancy in enumerate(vacancies):
            logger_basic_message = f"Page={page:02d} idx={idx:02d}: {vacancy['id']} {vacancy['name']} {vacancy['employer']['name']}"
            if await vacancy_blacklisted_by_words(
                " ".join((vacancy["name"], vacancy["employer"]["name"]))
            ):
                logger.info(f"{logger_basic_message} SKIPPED due to blacklist words")
                continue
            elif await vacancy_blacklisted_by_ids(vacancy["id"]):
                logger.info(f"{logger_basic_message} SKIPPED due to blacklist ID")
                continue
            if test_run:
                logger.info(f"{logger_basic_message} TEST RUN")
                continue

            try:
                negotiation_url = await apply_to_vacancy(
                    session=session,
                    vacancy_id=vacancy["id"],
                    logger_msg=logger_basic_message,
                )
            except HH_Limit_Exceeded_Error:
                queue.shutdown(immediate=True)
                break
            if negotiation_url:
                await add_apply_to_notion(
                    session=session,
                    company=vacancy["employer"]["name"],
                    position=vacancy["name"],
                    url=vacancy["alternate_url"],
                    negotiation_url=negotiation_url,
                    logger_msg=logger_basic_message,
                )
        queue.task_done()


async def process_vacancies_response(
    response_json: Optional[dict], queue: Queue, page: int = 0
) -> List:
    if response_json:
        if page == 0:
            logger.info(
                f"Got {response_json['found']} vacancies, {response_json['pages']} pages"
            )
            await fill_queue(
                queue=queue, start_page=page + 1, end_page=response_json["pages"]
            )
        logger.info(f"Page={page} got {len(response_json['items'])} vacancies")
        return response_json["items"]
    return []


async def apply_to_vacancy(
    session: aiohttp.ClientSession, vacancy_id: int, logger_msg: str
) -> Optional[str]:
    response = await session.post(
        url=settings.negotiation_url,
        headers=settings.hh_headers,
        data={
            "vacancy_id": vacancy_id,
            "resume_id": settings.resume_id,
            "message": settings.cover_letter,
        },
        allow_redirects=False,
    )
    if response.status == 201:
        logger.info(
            f"{logger_msg} APPLIED successfully, GOT negotiation url: {response.headers.get('Location', '')}"
        )
        return response.headers.get("Location", "")
    else:
        error_msg = ""
        if response.status == 403 or response.status == 400:
            response_json = await response.json()
            if any(
                error["value"] == "limit_exceeded" for error in response_json["errors"]
            ):
                logger.error(f"{logger_msg} LIMIT EXCEEDED. Stopping...")
                raise HH_Limit_Exceeded_Error
            else:
                error_msg = response_json["description"]
        elif response.status == 303:
            error_msg = (
                f"External apply required on {response.headers.get('Location', '')}"
            )
        else:
            error_msg = f"Unknown error: {response.status} {await response.text()}"
        logger.error(f"{logger_msg} apply FAILED with error: {error_msg}")


async def add_apply_to_notion(
    session: aiohttp.ClientSession,
    company: str,
    position: str,
    url: str,
    negotiation_url: str,
    logger_msg: str,
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
            f"{logger_msg} NOTION: Could not create a page: {response.status} {await response.text()}"
        )
    else:
        response_json = await response.json()
        logger.info(f"{logger_msg} NOTION: Page created with id: {response_json['id']}")


async def vacancy_blacklisted_by_words(vacancy_text: str) -> bool:
    return any(
        word in settings.blacklist_words
        for word in settings.blacklist_regex.findall(vacancy_text.lower())
    )


async def vacancy_blacklisted_by_ids(vacancy_id: str) -> bool:
    return vacancy_id in settings.blacklist_ids


async def main(workers_num: int, test_run: bool, search: SearchType) -> None:
    if not settings.notion_enabled:
        logger.info("NOTION: Notion is disabled")

    queue = Queue()
    async with aiohttp.ClientSession() as session:
        await queue.put(0)
        workers = [
            create_task(
                fetch_vacancy_page(
                    session=session, queue=queue, test_run=test_run, search=search
                )
            )
            for _ in range(workers_num)
        ]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    logger.info(f"{'-' * 60}Done")


if __name__ == "__main__":
    workers_num, test_run, search = parse_args()
    asyncio.run(main(workers_num=workers_num, test_run=test_run, search=search))
