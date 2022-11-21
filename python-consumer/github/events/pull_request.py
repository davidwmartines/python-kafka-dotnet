from typing import List, Optional
from dataclasses import dataclass, field
from github.events.comment import Comment
from github.events.status import Status


@dataclass
class PullRequest:
    """Represents the state of a Pull Request."""

    id: int
    url: str
    title: str
    author: str
    opened_on: int
    closed_on: Optional[int] = None
    status: Optional[Status] = None
    comments: List[Comment] = field(default_factory=list)
