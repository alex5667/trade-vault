from enum import StrEnum


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"

    def to_side(self) -> "Side":
        return Side.BUY if self == Direction.LONG else Side.SELL

    def to_side_int(self) -> int:
        return 1 if self == Direction.LONG else -1

class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    def to_direction(self) -> "Direction":
        return Direction.LONG if self == Side.BUY else Direction.SHORT

    def to_side_int(self) -> int:
        return 1 if self == Side.BUY else -1
