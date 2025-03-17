import asyncio
import logging
from settings import settings
import aiohttp
from asyncio import Queue, create_task


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="hh.log",
)
logger = logging.getLogger(__name__)

VACANCY_URL = (
    f"{settings.api_url.rstrip('/')}/resumes/{settings.resume_id}/similar_vacancies"
)
HEADERS = {"Authorization": f"Bearer {settings.token}"}


async def fill_queue(session, queue):
    response = await session.get(url=VACANCY_URL, headers=HEADERS)
    if response.status != 200:
        logger.error(
            f"Error fetching {VACANCY_URL}: {response.status}\n{await response.text()}"
        )
        return False
    else:
        response_json = await response.json()
        pages, per_page = response_json["pages"], response_json["per_page"]
        logger.info(
            f"Got {response_json['found']} vacancies, {pages} pages, {per_page} per page"
        )
        for i in range(pages):
            logger.info(f"Add block ({i},{per_page}) to queue")
            await queue.put((i, per_page))


async def fetch_vacancy_page(session, queue):
    while True:
        page, per_page = await queue.get()
        logger.info(f"Fetch block ({page},{per_page}) from queue")
        try:
            response = await session.get(
                url=VACANCY_URL,
                params={"page": page, "per_page": per_page},
                headers=HEADERS,
            )
            if response.status != 200:
                logger.error(
                    f"Error fetching {VACANCY_URL} with page={page} per_page={per_page}: {response.status}\n{await response.text()}"
                )
            else:
                response_json = await response.json()
                logger.info(f"Page={page} got {len(response_json['items'])} vacancies")
                for idx, vacancy in enumerate(response_json["items"]):
                    logger.info(
                        f"Page={page} idx={idx}: {vacancy['id']} {vacancy['name']} {vacancy['employer']['name']}"
                    )
        except Exception as e:
            logger.error(
                f"Fetch block ({page},{per_page}) from queue finished with error {str(e)}"
            )
        finally:
            queue.task_done()


async def main():
    queue = Queue()
    async with aiohttp.ClientSession() as session:
        await fill_queue(session, queue)
        workers = [create_task(fetch_vacancy_page(session, queue)) for _ in range(2)]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


asyncio.run(main())
