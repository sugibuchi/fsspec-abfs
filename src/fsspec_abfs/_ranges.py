"""The range a ``_cat_file`` asks, from fsspec's ``start`` and ``end``, sizing the file only when it must.

fsspec slices as Python does: either end may be negative, counted from the end of the file. A storage's ranged GET
takes absolute offsets, or a suffix, the last N bytes, as HTTP's ``bytes=-N``. So a read needs the file's size, and
an ``_info`` first, only for an end counted from the end, or for a suffix the storage cannot ask.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


@dataclass(frozen=True)
class Range:
    """What to download: ``start`` to ``end``, exclusive, None for the end of the file.

    A negative ``start`` with no ``end`` is a suffix, the last ``-start`` bytes. ``empty`` asks nothing.
    """

    start: int = 0
    end: int | None = None
    empty: bool = False

    @property
    def whole(self) -> bool:
        return self.start == 0 and self.end is None

    def http(self) -> str | None:
        """The HTTP Range header's value, or None for the whole file."""
        if self.whole:
            return None
        if self.start < 0:
            return f"bytes={self.start}"
        return f"bytes={self.start}-" + ("" if self.end is None else str(self.end - 1))


async def plan(
    start: int | None,
    end: int | None,
    size: Callable[[], Awaitable[int]],
    *,
    suffix: bool = True,
) -> Range:
    """The range to download, calling ``size`` only for an end counted from the end of the file.

    ``suffix`` says whether the storage asks the last N bytes itself, as HTTP's ``bytes=-N``.
    """
    start = start or 0
    if (end is not None and end < 0) or (start < 0 and (end is not None or not suffix)):
        n = await size()
        start = max(0, start + n) if start < 0 else start
        end = None if end is None else max(0, end + n) if end < 0 else end
    if end is not None and end <= max(start, 0):
        return Range(empty=True)
    return Range(start, end)
