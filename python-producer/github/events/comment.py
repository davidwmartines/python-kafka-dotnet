from dataclasses import dataclass


@dataclass
class Comment:
    author: str
    body: str
    time: int
