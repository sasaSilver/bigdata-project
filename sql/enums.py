from enum import StrEnum

class ResultClass(StrEnum):
    """Game outcome class - primary ML target"""
    WHITE_WIN = "white_win"
    BLACK_WIN = "black_win"
    DRAW = "draw"


class TimeClass(StrEnum):
    """Chess.com time control categories"""
    BULLET = "bullet"
    BLITZ = "blitz"
    RAPID = "rapid"
    DAILY = "daily"


class SideToMove(StrEnum):
    """Side currently to move (human-readable)"""
    WHITE = "white"
    BLACK = "black"


class PieceType(StrEnum):
    """Standard chess piece types"""
    PAWN = "pawn"
    KNIGHT = "knight"
    BISHOP = "bishop"
    ROOK = "rook"
    QUEEN = "queen"
    KING = "king"


class PromotionPiece(StrEnum):
    """Possible promotion pieces"""
    QUEEN = "queen"
    ROOK = "rook"
    BISHOP = "bishop"
    KNIGHT = "knight"


class Weekday(StrEnum):
    """Day of week for game end time"""
    MONDAY = "Monday"
    TUESDAY = "Tuesday"
    WEDNESDAY = "Wednesday"
    THURSDAY = "Thursday"
    FRIDAY = "Friday"
    SATURDAY = "Saturday"
    SUNDAY = "Sunday"