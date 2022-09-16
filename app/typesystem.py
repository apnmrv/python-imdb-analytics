from typing import NewType, Tuple, List
from bs4 import BeautifulSoup as BSoup

Url = NewType('Url', str)
EntityName = NewType('EntityName', str)
WebEntity = NewType('WebEntity', Tuple[EntityName, Url])

ActorName = NewType('ActorName', EntityName)
MovieTitle = NewType('MovieTitle', EntityName)
Soup = NewType('Soup', BSoup)
MovieActors = List[WebEntity]
ActorMovies = List[WebEntity]
