import asyncio
import datetime
import aiohttp
from more_itertools import chunked

from models import Base, Session, SwapiPeople, engine

MAX_CHUNK_SIZE = 5

async def get_json(url_x):
    session = aiohttp.ClientSession()
    response = await session.get(url_x)
    json_data = await response.json()
    await session.close()
    if "detail" in json_data:
        print(f"ой!>>>>>>>>>>>>>>>>>>>>>> {url_x} похоже, база пустая - {json_data}")
    else:
        if "name" in json_data:
            print(json_data["name"])
    return json_data


async def get_in(list_in, name_in):
    r = []
    for x_in in list_in:
        json_x = await get_json(x_in)
        r.append(json_x[name_in])
    return ", ".join(r) if r else "None"


async def insert_to_db(people_json_list):
    async with Session() as session:
        swapi_people_list = [
            SwapiPeople(
                json=json_data,
                name=json_data["name"],
                height=json_data["height"],
                mass=json_data["mass"],
                hair_color=json_data["hair_color"],
                skin_color=json_data["skin_color"],
                eye_color=json_data["eye_color"],
                birth_year=json_data["birth_year"],
                gender=json_data["gender"],
                homeworld=await get_in([json_data["homeworld"]], "name"),
                films=await get_in(json_data["films"], "title"),
                species=await get_in(json_data["species"], "name"),
                vehicles=await get_in(json_data["vehicles"], "name"),
                starships=await get_in(json_data["starships"], "name"),
            )
            for json_data in people_json_list
            if "name" in json_data
        ]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    await engine.dispose()

    for ids_chunk in chunked(range(1, 91), MAX_CHUNK_SIZE):
        print(ids_chunk)
        get_people_coros = [
            get_json(f"https://swapi.dev/api/people/{people_id}")
            for people_id in ids_chunk
        ]
        people_json_list = await asyncio.gather(*get_people_coros)
        asyncio.create_task(insert_to_db(people_json_list))

    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)

    await asyncio.gather(*tasks_sets)


start = datetime.datetime.now()
asyncio.run(main())
print("work!")
print(datetime.datetime.now() - start)
