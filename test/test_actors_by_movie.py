import re
from typing import Tuple

import requests as reqs
from bs4 import BeautifulSoup as BSoup
from app.imdb_code import get_actors_by_movie_soup as func_to_test

movie_url = "https://www.imdb.com/title/tt3480822/fullcredits?ref_=tt_ov_st_sm#cast"
should_be_actors_number = 98
should_be_output_length = 10

resp = reqs.get(movie_url)
movie_soup = BSoup(resp.content, "html.parser")
output = func_to_test(movie_soup)
limited_output = func_to_test(movie_soup, should_be_output_length)

url_regexp = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"


def test_should_return_all_actors():
    assert(len(output) == should_be_actors_number)


def test_should_return_list_of_string_pairs():
    result = True
    for p in output:
        if not (isinstance(p, Tuple) and isinstance(p[0], str) and isinstance(p[1], str)):
            result = False
            break
    assert result


def test_second_part_of_pair_should_be_url():
    result = True
    for p in output:
        if re.fullmatch(url_regexp, p[1]) is None:
            result = False
            break
    assert result


def test_should_return_number_of_pairs_specified():
    assert len(limited_output) == should_be_output_length


if __name__ == "__main__":
    test_should_return_all_actors()
    test_should_return_list_of_string_pairs()
    test_second_part_of_pair_should_be_url()
    test_should_return_number_of_pairs_specified()
    print("Everything passed")