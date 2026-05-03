import uuid

from sqlalchemy import DateTime, String, BigInteger, text, Index, Enum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func
from .enums import Weekday, ResultClass, TimeClass, SideToMove, PieceType, PromotionPiece

class Base(DeclarativeBase):
    ...



class ChessMove(Base):
    """Chess move-level dataset table for BigData/Spark final project.
    
    Each row represents one ply (half-move) from one Chess.com game.
    Primary task: multiclass classification predicting final game outcome.
    """
    __tablename__ = 'chess_moves'

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=text('gen_random_uuid()')
    )

    # ========================================
    # Metadata / identifier columns
    # ========================================
    game_uuid: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    game_url: Mapped[str]

    # ========================================
    # Archive and datetime columns
    # ========================================
    archive_month: Mapped[str] = mapped_column(String(7), index=True)  # Format: YYYY-MM
    game_year: Mapped[int] = mapped_column(index=True)
    game_month: Mapped[int]
    game_end_timestamp: Mapped[int] = mapped_column(BigInteger)  # Unix timestamp
    game_end_datetime_utc: Mapped[DateTime] = mapped_column(DateTime(timezone=True))  # Timezone-aware
    end_hour_utc: Mapped[int]
    end_weekday_utc: Mapped[Weekday] = mapped_column(Enum(Weekday, name="weekday_enum"))

    # ========================================
    # Game metadata
    # ========================================
    rated: Mapped[bool]
    rules: Mapped[str] = mapped_column(String(50))
    time_class: Mapped[TimeClass] = mapped_column(
        Enum(TimeClass, name="time_class_enum", create_constraint=True), index=True
    )  # bullet, blitz, rapid, daily
    time_control_raw: Mapped[str] = mapped_column(String(50))
    time_control_base_seconds: Mapped[int]
    time_control_increment_seconds: Mapped[int]

    # ========================================
    # Opening/ECO information
    # ========================================
    eco_url: Mapped[str]
    eco_code: Mapped[str] = mapped_column(String(5), index=True)  # ECO code
    opening_name: Mapped[str] = mapped_column(String(200))  # Opening name

    # ========================================
    # Player information
    # ========================================
    white_username: Mapped[str] = mapped_column(String(100))
    black_username: Mapped[str] = mapped_column(String(100))
    white_rating: Mapped[int] = mapped_column(index=True)
    black_rating: Mapped[int] = mapped_column(index=True)
    rating_diff: Mapped[int]  # white_rating - black_rating
    white_accuracy: Mapped[float]
    black_accuracy: Mapped[float]

    # ========================================
    # Move context
    # ========================================
    ply_index: Mapped[int] = mapped_column(index=True)
    fullmove_number: Mapped[int]
    side_to_move: Mapped[str] = mapped_column(String(1))  # 'w' or 'b'
    side_to_move_name: Mapped[SideToMove] = mapped_column(
        Enum(SideToMove, name="side_to_move_enum", create_constraint=True)
    )
    san_move: Mapped[str] = mapped_column(String(10))  # Standard algebraic notation
    uci_move: Mapped[str] = mapped_column(String(4))  # UCI notation
    piece_moved: Mapped[PieceType] = mapped_column(
        Enum(PieceType, name="piece_type_enum", create_constraint=True)
    )
    from_square: Mapped[str] = mapped_column(String(2))  # Algebraic notation
    to_square: Mapped[str] = mapped_column(String(2))  # Algebraic notation

    # ========================================
    # Move flags
    # ========================================
    is_capture: Mapped[bool]
    is_check: Mapped[bool]
    is_checkmate: Mapped[bool]
    is_castling: Mapped[bool]
    is_promotion: Mapped[bool]
    promotion_piece: Mapped[PromotionPiece] = mapped_column(
        Enum(PromotionPiece, name="promotion_piece_enum", create_constraint=True), nullable=True
    )

    # ========================================
    # Board state (FEN)
    # ========================================
    board_fen_before: Mapped[str]
    board_fen_after: Mapped[str]
    active_color_before: Mapped[str] = mapped_column(String(1))  # 'w' or 'b'
    castling_rights_before: Mapped[str] = mapped_column(String(4))  # e.g., 'KQkq'
    en_passant_square_before: Mapped[str] = mapped_column(String(2))  # e.g., 'e3'
    halfmove_clock_before: Mapped[int]
    fullmove_number_before: Mapped[int]
    legal_moves_count_before: Mapped[int]
    total_piece_count_before: Mapped[int]

    # ========================================
    # Material counts
    # ========================================
    material_white_before: Mapped[int]
    material_black_before: Mapped[int]
    material_diff_before: Mapped[int]
    non_pawn_material_white_before: Mapped[int]
    non_pawn_material_black_before: Mapped[int]
    non_pawn_material_diff_before: Mapped[int]

    # Piece counts by type for each side
    white_pawns_before: Mapped[int]
    white_knights_before: Mapped[int]
    white_bishops_before: Mapped[int]
    white_rooks_before: Mapped[int]
    white_queens_before: Mapped[int]
    black_pawns_before: Mapped[int]
    black_knights_before: Mapped[int]
    black_bishops_before: Mapped[int]
    black_rooks_before: Mapped[int]
    black_queens_before: Mapped[int]

    # ========================================
    # Pawn structure analysis
    # ========================================
    white_doubled_pawns_before: Mapped[int]
    black_doubled_pawns_before: Mapped[int]
    doubled_pawns_diff_before: Mapped[int]
    white_isolated_pawns_before: Mapped[int]
    black_isolated_pawns_before: Mapped[int]
    isolated_pawns_diff_before: Mapped[int]
    white_passed_pawns_before: Mapped[int]
    black_passed_pawns_before: Mapped[int]
    passed_pawns_diff_before: Mapped[int]

    # Pawn shield scores
    white_pawn_shield_score_before: Mapped[float]
    black_pawn_shield_score_before: Mapped[float]
    pawn_shield_diff_before: Mapped[float]

    # ========================================
    # King safety analysis
    # ========================================
    white_king_tropism_before: Mapped[float]
    black_king_tropism_before: Mapped[float]
    king_tropism_diff_before: Mapped[float]

    # Castling rights
    white_can_castle_kingside_before: Mapped[bool]
    white_can_castle_queenside_before: Mapped[bool]
    black_can_castle_kingside_before: Mapped[bool]
    black_can_castle_queenside_before: Mapped[bool]
    in_check_before: Mapped[bool]

    # Bishop pair
    bishops_pair_white_before: Mapped[bool]
    bishops_pair_black_before: Mapped[bool]

    # ========================================
    # Game phase classification
    # ========================================
    is_opening_phase: Mapped[bool]
    is_middlegame_phase: Mapped[bool]
    is_endgame_phase: Mapped[bool]
    ply_bucket: Mapped[str] = mapped_column(String(11))  # Format: 0001-0010

    # ========================================
    # Clock/time data
    # ========================================
    white_clock_seconds_after: Mapped[float]
    black_clock_seconds_after: Mapped[float]
    clock_seconds_after_for_side_to_move: Mapped[float]
    side_to_move_clock_before: Mapped[float]
    time_spent_on_move_seconds: Mapped[float]
    clock_remaining_pct: Mapped[float]
    avg_time_spent_per_move_so_far: Mapped[float]
    is_in_time_trouble_30s: Mapped[bool]

    # ========================================
    # Player rating context
    # ========================================
    side_to_move_rating: Mapped[int]
    opponent_rating: Mapped[int]
    side_to_move_rating_diff: Mapped[int]

    # ========================================
    # Audit-only columns (not for modeling)
    # ========================================
    result_raw: Mapped[str] = mapped_column(String(100))
    termination: Mapped[str] = mapped_column(String(50))

    # ========================================
    # Target columns
    # ========================================
    final_result_class: Mapped[ResultClass] = mapped_column(
        Enum(ResultClass, name="result_class_enum", create_constraint=True), index=True
    )  # white_win, black_win, draw
    white_won_flag: Mapped[int]
    black_won_flag: Mapped[int]
    draw_flag: Mapped[int]

    # ========================================
    # Metadata
    # ========================================
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # ========================================
    # Composite indexes for optimized querying
    # ========================================
    __table_args__ = (
        # Game-level queries: retrieve all moves for a specific game in order
        Index('ix_chess_moves_game_ply', 'game_uuid', 'ply_index'),

        # Rating-based win rate analysis
        Index('ix_chess_moves_white_rating_result', 'white_rating', 'final_result_class'),
        Index('ix_chess_moves_black_rating_result', 'black_rating', 'final_result_class'),
        Index('ix_chess_moves_side_rating_diff', 'side_to_move_rating', 'side_to_move_rating_diff'),

        # Time control performance analysis
        Index('ix_chess_moves_time_class_result', 'time_class', 'final_result_class'),
        Index('ix_chess_moves_time_control_base_result', 'time_control_base_seconds', 'final_result_class'),

        # Opening/ECO performance analysis
        Index('ix_chess_moves_eco_code_result', 'eco_code', 'final_result_class'),
        Index('ix_chess_moves_eco_ply', 'eco_code', 'ply_index'),

        # Temporal analysis
        Index('ix_chess_moves_year_month_result', 'game_year', 'game_month', 'final_result_class'),
        Index('ix_chess_moves_timestamp', 'game_end_timestamp'),

        # Material advantage analysis
        Index('ix_chess_moves_material_diff_result', 'material_diff_before', 'final_result_class'),
        Index('ix_chess_moves_material_counts', 'material_white_before', 'material_black_before', 'final_result_class'),

        # Game phase analysis
        Index('ix_chess_moves_phase_result', 'is_opening_phase', 'is_middlegame_phase', 'is_endgame_phase'),
        Index('ix_chess_moves_ply_bucket_result', 'ply_bucket', 'final_result_class'),

        # Position-specific analysis
        Index('ix_chess_moves_check_result', 'is_check', 'final_result_class'),
        Index('ix_chess_moves_capture_result', 'is_capture', 'final_result_class'),

        # Time trouble analysis
        Index('ix_chess_moves_time_trouble_result', 'is_in_time_trouble_30s', 'final_result_class'),

        # Rating matchup analysis
        Index('ix_chess_moves_ratings_matchup', 'white_rating', 'black_rating', 'final_result_class'),

        # Piece count analysis
        Index('ix_chess_moves_piece_count_result', 'total_piece_count_before', 'final_result_class'),

        # Castling analysis
        Index('ix_chess_moves_castling_result', 'white_can_castle_kingside_before', 'black_can_castle_kingside_before', 'final_result_class'),

        # Bishop pair analysis
        Index('ix_chess_moves_bishop_pair_result', 'bishops_pair_white_before', 'bishops_pair_black_before', 'final_result_class'),
    )

    def __repr__(self) -> str:
        return (
            f"<ChessMove(game_uuid='{self.game_uuid}', ply_index={self.ply_index}, "
            f"side_to_move='{self.side_to_move}', result='{self.final_result_class}')>"
        )