from datetime import datetime

from sqlalchemy import DateTime, String, BigInteger, Index, Enum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func
from .enums import (
    Weekday,
    ResultClass,
    TimeClass,
    SideToMove,
    PieceType,
    PromotionPiece,
)


class Base(DeclarativeBase): ...


class MLMetric(Base):
    __tablename__ = "ml_metrics"

    model: Mapped[str] = mapped_column(String(50), primary_key=True)
    accuracy: Mapped[float]
    f1: Mapped[float]


class ChessMove(Base):
    """Chess move-level dataset table for BigData/Spark final project.

    Each row represents one ply (half-move) from one Chess.com game.
    Primary task: multiclass classification predicting final game outcome.
    """

    __tablename__ = "chess_moves"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # ========================================
    # Metadata / identifier columns
    # ========================================
    game_uuid: Mapped[str | None] = mapped_column(String(36), index=True)
    game_url: Mapped[str | None]

    # ========================================
    # Archive and datetime columns
    # ========================================
    archive_month: Mapped[str | None] = mapped_column(
        String(7), index=True
    )  # Format: YYYY-MM
    game_year: Mapped[int | None] = mapped_column(index=True)
    game_month: Mapped[int | None]
    game_end_timestamp: Mapped[int | None] = mapped_column(BigInteger)  # Unix timestamp
    game_end_datetime_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )  # Timezone-aware
    end_hour_utc: Mapped[int | None]
    end_weekday_utc: Mapped[Weekday | None] = mapped_column(
        Enum(Weekday, name="weekday_enum")
    )

    # ========================================
    # Game metadata
    # ========================================
    rated: Mapped[bool | None]
    rules: Mapped[str | None] = mapped_column(String(50))
    time_class: Mapped[TimeClass | None] = mapped_column(
        Enum(TimeClass, name="time_class_enum", create_constraint=True), index=True
    )  # bullet, blitz, rapid, daily
    time_control_raw: Mapped[str | None] = mapped_column(String(50))
    time_control_base_seconds: Mapped[int | None]
    time_control_increment_seconds: Mapped[int | None]

    # ========================================
    # Opening/ECO information
    # ========================================
    eco_url: Mapped[str | None]
    eco_code: Mapped[str | None] = mapped_column(String(5), index=True)  # ECO code
    opening_name: Mapped[str | None] = mapped_column(String(200))  # Opening name

    # ========================================
    # Player information
    # ========================================
    white_username: Mapped[str | None] = mapped_column(String(100))
    black_username: Mapped[str | None] = mapped_column(String(100))
    white_rating: Mapped[int | None] = mapped_column(index=True)
    black_rating: Mapped[int | None] = mapped_column(index=True)
    rating_diff: Mapped[int | None]  # white_rating - black_rating
    white_accuracy: Mapped[float | None]
    black_accuracy: Mapped[float | None]

    # ========================================
    # Move context
    # ========================================
    ply_index: Mapped[int | None] = mapped_column(index=True)
    fullmove_number: Mapped[int | None]
    side_to_move: Mapped[str | None] = mapped_column(String(1))  # 'w' or 'b'
    side_to_move_name: Mapped[SideToMove | None] = mapped_column(
        Enum(SideToMove, name="side_to_move_enum", create_constraint=True)
    )
    san_move: Mapped[str | None] = mapped_column(
        String(10)
    )  # Standard algebraic notation
    uci_move: Mapped[str | None] = mapped_column(String(10))  # UCI notation
    piece_moved: Mapped[PieceType | None] = mapped_column(
        Enum(PieceType, name="piece_type_enum", create_constraint=True)
    )
    from_square: Mapped[str | None] = mapped_column(String(2))  # Algebraic notation
    to_square: Mapped[str | None] = mapped_column(String(2))  # Algebraic notation

    # ========================================
    # Move flags
    # ========================================
    is_capture: Mapped[bool | None]
    is_check: Mapped[bool | None]
    is_checkmate: Mapped[bool | None]
    is_castling: Mapped[bool | None]
    is_promotion: Mapped[bool | None]
    promotion_piece: Mapped[PromotionPiece | None] = mapped_column(
        Enum(PromotionPiece, name="promotion_piece_enum", create_constraint=True),
        nullable=True,
    )

    # ========================================
    # Board state (FEN)
    # ========================================
    board_fen_before: Mapped[str | None]
    board_fen_after: Mapped[str | None]
    active_color_before: Mapped[str | None] = mapped_column(String(1))  # 'w' or 'b'
    castling_rights_before: Mapped[str | None] = mapped_column(
        String(4)
    )  # e.g., 'KQkq'
    en_passant_square_before: Mapped[str | None] = mapped_column(
        String(2)
    )  # e.g., 'e3'
    halfmove_clock_before: Mapped[int | None]
    fullmove_number_before: Mapped[int | None]
    legal_moves_count_before: Mapped[int | None]
    total_piece_count_before: Mapped[int | None]

    # ========================================
    # Material counts
    # ========================================
    material_white_before: Mapped[int | None]
    material_black_before: Mapped[int | None]
    material_diff_before: Mapped[int | None]
    non_pawn_material_white_before: Mapped[int | None]
    non_pawn_material_black_before: Mapped[int | None]
    non_pawn_material_diff_before: Mapped[int | None]

    # Piece counts by type for each side
    white_pawns_before: Mapped[int | None]
    white_knights_before: Mapped[int | None]
    white_bishops_before: Mapped[int | None]
    white_rooks_before: Mapped[int | None]
    white_queens_before: Mapped[int | None]
    black_pawns_before: Mapped[int | None]
    black_knights_before: Mapped[int | None]
    black_bishops_before: Mapped[int | None]
    black_rooks_before: Mapped[int | None]
    black_queens_before: Mapped[int | None]

    # ========================================
    # Pawn structure analysis
    # ========================================
    white_doubled_pawns_before: Mapped[int | None]
    black_doubled_pawns_before: Mapped[int | None]
    doubled_pawns_diff_before: Mapped[int | None]
    white_isolated_pawns_before: Mapped[int | None]
    black_isolated_pawns_before: Mapped[int | None]
    isolated_pawns_diff_before: Mapped[int | None]
    white_passed_pawns_before: Mapped[int | None]
    black_passed_pawns_before: Mapped[int | None]
    passed_pawns_diff_before: Mapped[int | None]

    # Pawn shield scores
    white_pawn_shield_score_before: Mapped[float | None]
    black_pawn_shield_score_before: Mapped[float | None]
    pawn_shield_diff_before: Mapped[float | None]

    # ========================================
    # King safety analysis
    # ========================================
    white_king_tropism_before: Mapped[float | None]
    black_king_tropism_before: Mapped[float | None]
    king_tropism_diff_before: Mapped[float | None]

    # Castling rights
    white_can_castle_kingside_before: Mapped[bool | None]
    white_can_castle_queenside_before: Mapped[bool | None]
    black_can_castle_kingside_before: Mapped[bool | None]
    black_can_castle_queenside_before: Mapped[bool | None]
    in_check_before: Mapped[bool | None]

    # Bishop pair
    bishops_pair_white_before: Mapped[bool | None]
    bishops_pair_black_before: Mapped[bool | None]

    # ========================================
    # Game phase classification
    # ========================================
    is_opening_phase: Mapped[bool | None]
    is_middlegame_phase: Mapped[bool | None]
    is_endgame_phase: Mapped[bool | None]
    ply_bucket: Mapped[str | None] = mapped_column(String(11))  # Format: 0001-0010

    # ========================================
    # Clock/time data
    # ========================================
    white_clock_seconds_after: Mapped[float | None]
    black_clock_seconds_after: Mapped[float | None]
    clock_seconds_after_for_side_to_move: Mapped[float | None]
    side_to_move_clock_before: Mapped[float | None]
    time_spent_on_move_seconds: Mapped[float | None]
    clock_remaining_pct: Mapped[float | None]
    avg_time_spent_per_move_so_far: Mapped[float | None]
    is_in_time_trouble_30s: Mapped[bool | None]

    # ========================================
    # Player rating context
    # ========================================
    side_to_move_rating: Mapped[int | None]
    opponent_rating: Mapped[int | None]
    side_to_move_rating_diff: Mapped[int | None]

    # ========================================
    # Audit-only columns (not for modeling)
    # ========================================
    result_raw: Mapped[str | None] = mapped_column(String(100))
    termination: Mapped[str | None] = mapped_column(String(50))

    # ========================================
    # Target columns
    # ========================================
    final_result_class: Mapped[ResultClass | None] = mapped_column(
        Enum(ResultClass, name="result_class_enum", create_constraint=True), index=True
    )  # white_win, black_win, draw
    white_won_flag: Mapped[int | None]
    black_won_flag: Mapped[int | None]
    draw_flag: Mapped[int | None]

    # ========================================
    # Metadata
    # ========================================
    created_at: Mapped[DateTime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[DateTime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # ========================================
    # Composite indexes for optimized querying
    # ========================================
    __table_args__ = (
        # Game-level queries: retrieve all moves for a specific game in order
        Index("ix_chess_moves_game_ply", "game_uuid", "ply_index"),
        # Rating-based win rate analysis
        Index(
            "ix_chess_moves_white_rating_result", "white_rating", "final_result_class"
        ),
        Index(
            "ix_chess_moves_black_rating_result", "black_rating", "final_result_class"
        ),
        Index(
            "ix_chess_moves_side_rating_diff",
            "side_to_move_rating",
            "side_to_move_rating_diff",
        ),
        # Time control performance analysis
        Index("ix_chess_moves_time_class_result", "time_class", "final_result_class"),
        Index(
            "ix_chess_moves_time_control_base_result",
            "time_control_base_seconds",
            "final_result_class",
        ),
        # Opening/ECO performance analysis
        Index("ix_chess_moves_eco_code_result", "eco_code", "final_result_class"),
        Index("ix_chess_moves_eco_ply", "eco_code", "ply_index"),
        # Temporal analysis
        Index(
            "ix_chess_moves_year_month_result",
            "game_year",
            "game_month",
            "final_result_class",
        ),
        Index("ix_chess_moves_timestamp", "game_end_timestamp"),
        # Material advantage analysis
        Index(
            "ix_chess_moves_material_diff_result",
            "material_diff_before",
            "final_result_class",
        ),
        Index(
            "ix_chess_moves_material_counts",
            "material_white_before",
            "material_black_before",
            "final_result_class",
        ),
        # Game phase analysis
        Index(
            "ix_chess_moves_phase_result",
            "is_opening_phase",
            "is_middlegame_phase",
            "is_endgame_phase",
        ),
        Index("ix_chess_moves_ply_bucket_result", "ply_bucket", "final_result_class"),
        # Position-specific analysis
        Index("ix_chess_moves_check_result", "is_check", "final_result_class"),
        Index("ix_chess_moves_capture_result", "is_capture", "final_result_class"),
        # Time trouble analysis
        Index(
            "ix_chess_moves_time_trouble_result",
            "is_in_time_trouble_30s",
            "final_result_class",
        ),
        # Rating matchup analysis
        Index(
            "ix_chess_moves_ratings_matchup",
            "white_rating",
            "black_rating",
            "final_result_class",
        ),
        # Piece count analysis
        Index(
            "ix_chess_moves_piece_count_result",
            "total_piece_count_before",
            "final_result_class",
        ),
        # Castling analysis
        Index(
            "ix_chess_moves_castling_result",
            "white_can_castle_kingside_before",
            "black_can_castle_kingside_before",
            "final_result_class",
        ),
        # Bishop pair analysis
        Index(
            "ix_chess_moves_bishop_pair_result",
            "bishops_pair_white_before",
            "bishops_pair_black_before",
            "final_result_class",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"side_to_move='{self.side_to_move}', result='{self.final_result_class}')>"
        )
