from dataclasses import dataclass
from enum import Enum
from typing import Optional

from fasthtml.common import Div


class SEMANTIC_COLOR(Enum):
    """
    An enumeration for semantic color coding of badges.
    """

    NEUTRAL = "neutral"
    PRIMARY = "primary"
    SECONDARY = "secondary"
    ACCENT = "accent"
    SUCCESS = "success"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class Badge:
    """
    A class to represent a badge with a semantic color.

    Attributes:
        name (str): The display name of the badge.
        semantic_color (SEMANTIC_COLOR): The color theme of the badge.
    """

    name: str
    semantic_color: Optional[SEMANTIC_COLOR] = SEMANTIC_COLOR.NEUTRAL

    def __ft__(self) -> Div:
        """
        Renders the Badge component.

        :return: A Div element styled as a badge with the specified semantic color.
        """
        return Div(
            self.name,
            cls=f"badge badge-{self.semantic_color.value} badge-sm text-black inline",
        )
