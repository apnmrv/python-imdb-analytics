# define helper functions if needed
# and put them in `imdb_helper_functions` module.
# you can import them and use here like that:
import csv
import logging
from itertools import combinations

from app.imdb_helper_functions import *
from app.typesystem import *
from app.globals import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_actors_by_movie_soup(cast_page_soup: BSoup, num_of_actors_limit: int = None) -> MovieActors:
    """
    :param cast_page_soup: BSoup
    :param num_of_actors_limit: int
    :return:
    """
    cast_anchors = cast_page_soup.select('table.cast_list a[href^="/name/nm"]')
    movie_actors = []

    for a in cast_anchors:
        if a.string is not None:
            name = a.string.strip()
            url = base_url + a['href']
            actor = WebEntity((name, url))
            movie_actors.append(actor)

    return movie_actors if num_of_actors_limit is None or len(movie_actors) < num_of_actors_limit \
        else movie_actors[:num_of_actors_limit]


def get_movies_by_actor_soup(actor_page_soup: BSoup, num_of_movies_limit: int = None) -> ActorMovies:
    """
    :param actor_page_soup: BSoup
    :param num_of_movies_limit: int
    :return: ActorMovies
    """
    filmography = get_actor_filmography_by_actor_soup(actor_page_soup)
    if filmography is None:
        logger.warning(f"Can't find filmography")

    actor_movies = []

    for f in filmography:
        if is_a_real_movie(f):
            a = f.select("a[href^='/title/']")[0]
            movie = WebEntity((a.text, base_url + a['href']))
            actor_movies.append(movie)

    return actor_movies if num_of_movies_limit is None or len(actor_movies) < num_of_movies_limit else actor_movies[
                                                                                                       :num_of_movies_limit]


def get_movie_distance(actor_start_url: str, actor_end_url: str,
                       num_of_actors_limit=None, num_of_movies_limit=None, parallelism=1) -> int:
    src_id = extract_id_from_url(actor_start_url)
    trg_id = extract_id_from_url(actor_end_url)

    distance = get_distance({src_id},
                            {trg_id},
                            get_actors_ids_by_movie_id,
                            get_movies_ids_by_actor_id,
                            {src_id, trg_id},
                            set(),
                            num_of_movies_limit,
                            num_of_actors_limit, 0, parallelism)

    return distance

def get_movie_descriptions_by_actor_soup(actor_page_soup) -> dict:
    movies = get_movies_by_actor_soup(actor_page_soup)

    descriptions = {}

    for title, url in movies:
        logger.info(f"Getting description of {title}")
        id = extract_id_from_url(url)
        movie_soup = get_soup_by_url(url)
        try:
            description = movie_soup.select(movie_description_selector)[0].text.strip()
            descriptions[id] = {"title": title, "description": description}
        except Exception as ex:
            logger.warning(f"Can't find description for {title}: {ex}")

    return descriptions


def get_actors_by_movie_url(movie_url: str,
                            postproc: Callable[[Iterable[WebEntity]], Iterator[Any]],
                            num_of_actors_limit: int = None) -> Iterator[Any]:
    movie_soup: BSoup = get_soup_by_url(movie_url)
    full_cast_url = get_full_cast_url_by_movie_soup(movie_soup, base_url)
    full_cast_soup = get_soup_by_url(full_cast_url)

    actors = get_actors_by_movie_soup(full_cast_soup, num_of_actors_limit)

    return postproc(actors)


def get_movies_by_actor_url(actor_url: str,
                            postproc: Callable[[Iterable[WebEntity]], Iterator[Any]],
                            num_of_movies_limit: int = None) -> Iterator[str]:
    actor_soup: BSoup = get_soup_by_url(actor_url)
    movies = get_movies_by_actor_soup(actor_soup, num_of_movies_limit)

    return postproc(movies)


def get_movies_ids_by_actor_id(actor_id: str, num_of_movies_limit: int = None):
    url = f"{actor_base_url}/{actor_id}/"
    return get_movies_by_actor_url(url, movies_to_movie_ids, num_of_movies_limit)


def get_actors_ids_by_movie_id(movie_id: str, num_of_actors_limit: int = None):
    url = f"{movie_base_url}/{movie_id}/"
    return get_actors_by_movie_url(url, actors_to_actor_ids, num_of_actors_limit)


def persist_distances(distances):
    fieldnames = ['actor_from', 'actor_to', 'distance']
    try:
        with open('movie_distances.csv', 'w', encoding='UTF8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(distances)
    except Exception as ex:
        logger.error(f"Can't write distances to a file: {ex}")


def run():
    distances = []

    actor_ids = map(lambda a: a[0], actors.items())
    actor_id_list = list(actor_ids)
    actor_id_pairs = combinations(actor_id_list, 2)
    parallelism = 4
    with futures.ProcessPoolExecutor(max_workers=5) as executor:
        future_data_results = {executor.submit(get_movie_distance, f"{actor_base_url}/{l_id}", f"{actor_base_url}/{r_id}", parallelism, parallelism, parallelism): (l_id, r_id) for l_id, r_id in actor_id_pairs}

    for future in futures.as_completed(future_data_results):
        l_id, r_id = future_data_results[future]
        try:
            distance = future.result()
        except Exception as ex:
            raise Exception(f'Exception thrown while calculating the distance from {l_id} to {r_id}: {ex}')
        else:
            distances.append({"from_actor": l_id, "to_actor": r_id, "distance": distance})

    persist_distances(distances)


if __name__ == '__main__':
    run()