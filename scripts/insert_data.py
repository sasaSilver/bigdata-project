"""
This script inserts the .csv dataset into the postgres database using COPY.
"""

import psycopg2 as pg

from settings import settings

if __name__ == "__main__":
    with pg.connect(settings.pg.conn_dsn) as conn:
        with conn.cursor() as cur:
            with open("data/chess_moves_dataset.csv", "r", encoding="utf-8") as f:
                cur.copy_expert("""
COPY chess_moves (
    game_uuid, game_url, archive_month, game_year, game_month,
    game_end_timestamp, game_end_datetime_utc, rated, rules, time_class,
    time_control_raw, time_control_base_seconds, time_control_increment_seconds,
    eco_url, eco_code, opening_name, white_username, black_username,
    white_rating, black_rating, rating_diff, white_accuracy, black_accuracy,
    result_raw, termination, ply_index, fullmove_number, side_to_move,
    side_to_move_name, san_move, uci_move, is_capture, is_check, is_checkmate,
    is_castling, is_promotion, promotion_piece, from_square, to_square,
    piece_moved, board_fen_before, board_fen_after, active_color_before,
    castling_rights_before, en_passant_square_before, halfmove_clock_before,
    fullmove_number_before, legal_moves_count_before, material_white_before,
    material_black_before, material_diff_before, white_clock_seconds_after,
    black_clock_seconds_after, clock_seconds_after_for_side_to_move,
    side_to_move_clock_before, time_spent_on_move_seconds, clock_remaining_pct,
    avg_time_spent_per_move_so_far, is_in_time_trouble_30s, end_hour_utc,
    end_weekday_utc, white_pawns_before, white_knights_before, white_bishops_before,
    white_rooks_before, white_queens_before, black_pawns_before, black_knights_before,
    black_bishops_before, black_rooks_before, black_queens_before,
    white_doubled_pawns_before, black_doubled_pawns_before, doubled_pawns_diff_before,
    white_isolated_pawns_before, black_isolated_pawns_before, isolated_pawns_diff_before,
    white_passed_pawns_before, black_passed_pawns_before, passed_pawns_diff_before,
    white_pawn_shield_score_before, black_pawn_shield_score_before, pawn_shield_diff_before,
    white_king_tropism_before, black_king_tropism_before, king_tropism_diff_before,
    white_can_castle_kingside_before, white_can_castle_queenside_before,
    black_can_castle_kingside_before, black_can_castle_queenside_before,
    in_check_before, is_opening_phase, is_middlegame_phase, is_endgame_phase,
    ply_bucket, total_piece_count_before, non_pawn_material_white_before,
    non_pawn_material_black_before, non_pawn_material_diff_before,
    bishops_pair_white_before, bishops_pair_black_before, side_to_move_rating,
    opponent_rating, side_to_move_rating_diff, final_result_class,
    white_won_flag, black_won_flag, draw_flag
) FROM STDIN WITH (
    FORMAT CSV,
    HEADER,
    DELIMITER ',',
    NULL '',
    FORCE_NULL (promotion_piece)
)
""",
                    f,
                )
            cur.execute("SELECT * FROM emps LIMIT 2;")
