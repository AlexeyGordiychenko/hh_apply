import argparse
import aiohttp
from send_applies import SearchType, add_apply_to_notion, apply_to_vacancy, get_vacancies_response

def parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="A script to apply to one vacancy on hh.ru for testing the script"
    )
    parser.add_argument(
        "-s",
        "--search",
        type=SearchType,
        choices=list(SearchType),
        default=SearchType.SIMILAR,
        required=True,
        help="Search similar or query",
    )
    args = parser.parse_args()
    return args.search

async def send(search):
    async with aiohttp.ClientSession() as session:
        response_json = await get_vacancies_response(session=session, page=0, search=search)
        vacancy = response_json["items"][0]
        negotiation_url = await apply_to_vacancy(session=session, vacancy_id=vacancy["id"], logger_msg="")
        if negotiation_url:
            await add_apply_to_notion(
                session=session,
                company=vacancy["employer"]["name"],
                position=vacancy["name"],
                url=vacancy["alternate_url"],
                negotiation_url=negotiation_url,
                logger_msg="",
            )

if __name__ == "__main__":
    import asyncio
    search = parse_args()
    asyncio.run(send(search))