import math
from concurrent import futures
from typing import Any, Callable, Iterable, Iterator, Union, Set

import requests as reqs
from bs4 import BeautifulSoup as BSoup
from bs4.element import Tag as BSTag

from app.globals import *
from app.typesystem import WebEntity


def extract_id_from_url(url: str) -> str:
    result = re.search(id_from_url_pattern, url)

    return result if result is None else result.group(5)


def get_full_cast_url_by_movie_soup(movie_soup: BSoup, base_url) -> str:
    return base_url + movie_soup.find("a", {"class": "ipc-metadata-list-item__icon-link",
                                            "aria-label": "See full cast and crew"})['href']


def get_actor_filmography_by_actor_soup(actor_page_soup: BSoup):
    filmography = actor_page_soup.find("div", attrs={"id": "filmography"})
    if filmography is not None:
        return filmography.find_all('div', attrs={"id": re.compile(r"^act(or|ress)")})

    return None


def is_a_real_movie(f: BSTag) -> bool:
    may_be_year_string = f.find("span", attrs={"class": "year_column"}).text.strip()

    return len(may_be_year_string) > 0 and re.search(real_movie_pattern, f.text) is None


def get_soup_by_url(url: str) -> BSoup:
    resp = reqs.get(url)

    return BSoup(resp.content, "html.parser")


def web_entities_to_ids(web_entities: Iterable[WebEntity]) -> Iterator[Union[None, str]]:
    _, urls = zip(*web_entities)

    return map(lambda url: extract_id_from_url(url), urls)


def actors_to_actor_ids(actors: Iterable[WebEntity]) -> Iterator[Union[None, str]]:
    return web_entities_to_ids(actors)


def movies_to_movie_ids(movies: Iterable[WebEntity]) -> Iterator[Union[None, str]]:
    return web_entities_to_ids(movies)


def get_those_of_these(
        these: Set[Any],
        get_those: Callable[[Any, Union[int, None]], Iterable[Any]],
        those_known: Set[Any],
        those_limit, parallelism):
    those_of_these = set()

    with futures.ThreadPoolExecutor(max_workers=parallelism, thread_name_prefix="IMDB crawler") as executor:
        future_data_results = {executor.submit(get_those, this, those_limit): this for this in these}

    for future in futures.as_completed(future_data_results):
        this = future_data_results[future]
        try:
            those = set(future.result())
        except Exception as ex:
            raise Exception(f'Request for those of {this} generated an exception: {ex}')
        else:
            those = set(those).difference(those_known)
            those_known = those_known.union(those)
            those_of_these = those_of_these.union(those)

    return those_of_these, those_known


def get_distance(
        left: Set[Any],
        right: Set[Any],
        get_these: Callable[[Any, Union[int, None]], Iterable[Any]],
        get_those: Callable[[Any, Union[int, None]], Iterable[Any]],
        these_known: Set[Any],
        those_known: Set[Any],
        these_limit,
        those_limit,
        distance,
        parallelism):
    if distance > 5:
        return math.inf

    if len(left.intersection(right).intersection(these_known)) > 0:
        return distance

    try:
        those_of_left, those_known = get_those_of_these(left, get_those, those_known, those_limit, parallelism)
        those_of_right, those_known = get_those_of_these(right, get_those, those_known, those_limit, parallelism)

    except Exception as ex:
        raise Exception(f"Something went wrong: {ex}")

    return get_distance(those_of_left,
                        those_of_right,
                        get_those,
                        get_these,
                        those_known,
                        these_known,
                        those_limit,
                        these_limit,
                        distance + 1, parallelism)
