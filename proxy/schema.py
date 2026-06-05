from dataclasses import dataclass, field


@dataclass
class FetchRequest:
    id: str
    type: str  # http | search | github | arxiv
    url: str = ""
    method: str = "GET"
    headers: dict = field(default_factory=dict)
    body: str | None = None
    timeout: int = 30
    created_at: float = 0.0


@dataclass
class FetchResponse:
    id: str
    status: int = 0
    body: str = ""
    headers: dict = field(default_factory=dict)
    error: str | None = None
    fetched_at: float = 0.0
    truncated: bool = False
