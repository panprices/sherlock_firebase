from typing import Any, List
from itertools import islice


def chunkify(lst: List[Any], size: int) -> List[list]:
    """
    Split a list into `size`-sized chunks.

    Example:

    ```python
    chunks([1, 2, 3, 4, 5], 2) -> [[1, 2], [3, 4], [5]]
    ```
    """

    # This will get the start points for all chunks.
    # Ex if you have a 100 long list and size is 24:
    # range(0, 100, 24) -> [0, 24, 48, 72, 96]
    indexes = range(0, len(lst), size)

    return [lst[start : start + size] for start in indexes]
