# # Chess.com Move-Level Dataset Generation Pipeline
#
# This notebook builds a reproducible data engineering pipeline for a Big Data / Spark project.
#
# **Pipeline outputs**
# - `chess_moves_dataset.csv`
# - `dataset_description.txt`
# - `data_quality_report.txt`
# - `archive_manifest.csv`
# - `schema_example.csv`
# - `generation_log.txt`
#
# **Unit of observation**
# - One row equals one ply (half-move).
#
# **Primary ML task**
# - Multiclass classification of the final game outcome from the current move state.
#

# ## Package Installation Notes
#
# Run this once in the notebook environment if the packages are not already installed:
#
# ```bash
# python -m pip install requests python-chess tqdm
# ```
#
# The notebook is designed for Python 3.11+ and can be executed top-to-bottom from a fresh kernel.
#

# ## Execution Flow
#
# 1. Configure the Chess.com username and output paths.
# 2. Discover monthly archive URLs from the Chess.com public API.
# 3. Cache raw monthly JSON archives locally for reproducibility.
# 4. Parse PGNs with `python-chess`.
# 5. Emit one CSV row per ply with board-state, metadata, clock, and target columns.
# 6. Validate the generated dataset and write supporting text reports.
#

# ## Imports
#
# Import only the packages needed for fetching, parsing, feature engineering, and streaming output writes.

# In[1]:


from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import chess
import chess.pgn
import requests
from tqdm.auto import tqdm


# ## Configuration and Paths
#
# Edit the values in the next cells before running the notebook end-to-end.

# In[ ]:


USERNAME = "Tirex300"
OUTPUT_DIR = Path("data")
RAW_CACHE_DIR = OUTPUT_DIR / "raw_archives"
FORCE_REFETCH = False
REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": "chess-move-dataset-pipeline/1.0 (+academic project; contact: n8627617@gmail.com)",
    "Accept": "application/json",
}
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.5
SCHEMA_SAMPLE_LIMIT = 100


# In[3]:


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV_PATH = OUTPUT_DIR / "chess_moves_dataset.csv"
OUTPUT_CSV_TMP_PATH = OUTPUT_DIR / "chess_moves_dataset.csv.tmp"
DATASET_DESCRIPTION_PATH = OUTPUT_DIR / "dataset_description.txt"
DATA_QUALITY_REPORT_PATH = OUTPUT_DIR / "data_quality_report.txt"
ARCHIVE_MANIFEST_PATH = OUTPUT_DIR / "archive_manifest.csv"
SCHEMA_EXAMPLE_PATH = OUTPUT_DIR / "schema_example.csv"
GENERATION_LOG_PATH = OUTPUT_DIR / "generation_log.txt"


# In[4]:


logger = logging.getLogger("chess_moves_dataset")
logger.handlers.clear()
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

file_handler = logging.FileHandler(GENERATION_LOG_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

logger.info("Notebook initialized for username=%s", USERNAME)


# ## API Constants and Headers
#
# Define public API endpoints, request headers, and regular expressions used later in the pipeline.

# In[5]:


ARCHIVES_ENDPOINT_TEMPLATE = (
    "https://api.chess.com/pub/player/{username}/games/archives"
)
ARCHIVE_MONTH_PATTERN = re.compile(r"/games/(?P<year>\d{4})/(?P<month>\d{2})/?$")
CLOCK_PATTERN = re.compile(r"\[%clk\s+([0-9]+:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?)\]")


# ## Result Normalization and Schema
#
# Set up label normalization rules, output column order, and column-level documentation used in the text reports.

# In[6]:


PGN_RESULT_TO_CLASS = {
    "1-0": "white_win",
    "0-1": "black_win",
    "1/2-1/2": "draw",
}

DRAW_RESULT_CODES = {
    "agreed",
    "repetition",
    "stalemate",
    "insufficient",
    "50move",
    "timevsinsufficient",
    "draw",
}

DRAW_TERMINATION_SNIPPETS = {
    "draw",
    "stalemate",
    "repetition",
    "insufficient",
    "50-move",
    "50 move",
    "time vs insufficient",
}

PIECE_VALUE_MAP = {
    chess.PAWN: 1,
    chess.KNIGHT: 3,
    chess.BISHOP: 3,
    chess.ROOK: 5,
    chess.QUEEN: 9,
}

PIECE_NAME_MAP = {
    chess.PAWN: "pawn",
    chess.KNIGHT: "knight",
    chess.BISHOP: "bishop",
    chess.ROOK: "rook",
    chess.QUEEN: "queen",
    chess.KING: "king",
}


# In[7]:


METADATA_COLUMNS = [
    "game_uuid",
    "game_url",
    "archive_month",
    "game_year",
    "game_month",
    "game_end_timestamp",
    "game_end_datetime_utc",
    "rated",
    "rules",
    "time_class",
    "time_control_raw",
    "time_control_base_seconds",
    "time_control_increment_seconds",
    "eco_url",
    "eco_code",
    "opening_name",
    "white_username",
    "black_username",
    "white_rating",
    "black_rating",
    "rating_diff",
    "white_accuracy",
    "black_accuracy",
    "result_raw",
    "termination",
]

FEATURE_COLUMNS = [
    "ply_index",
    "fullmove_number",
    "side_to_move",
    "side_to_move_name",
    "san_move",
    "uci_move",
    "is_capture",
    "is_check",
    "is_checkmate",
    "is_castling",
    "is_promotion",
    "promotion_piece",
    "from_square",
    "to_square",
    "piece_moved",
    "board_fen_before",
    "board_fen_after",
    "active_color_before",
    "castling_rights_before",
    "en_passant_square_before",
    "halfmove_clock_before",
    "fullmove_number_before",
    "legal_moves_count_before",
    "material_white_before",
    "material_black_before",
    "material_diff_before",
    "white_clock_seconds_after",
    "black_clock_seconds_after",
    "clock_seconds_after_for_side_to_move",
    "side_to_move_clock_before",
    "time_spent_on_move_seconds",
    "clock_remaining_pct",
    "avg_time_spent_per_move_so_far",
    "is_in_time_trouble_30s",
    "end_hour_utc",
    "end_weekday_utc",
    "white_pawns_before",
    "white_knights_before",
    "white_bishops_before",
    "white_rooks_before",
    "white_queens_before",
    "black_pawns_before",
    "black_knights_before",
    "black_bishops_before",
    "black_rooks_before",
    "black_queens_before",
    "white_doubled_pawns_before",
    "black_doubled_pawns_before",
    "doubled_pawns_diff_before",
    "white_isolated_pawns_before",
    "black_isolated_pawns_before",
    "isolated_pawns_diff_before",
    "white_passed_pawns_before",
    "black_passed_pawns_before",
    "passed_pawns_diff_before",
    "white_pawn_shield_score_before",
    "black_pawn_shield_score_before",
    "pawn_shield_diff_before",
    "white_king_tropism_before",
    "black_king_tropism_before",
    "king_tropism_diff_before",
    "white_can_castle_kingside_before",
    "white_can_castle_queenside_before",
    "black_can_castle_kingside_before",
    "black_can_castle_queenside_before",
    "in_check_before",
    "is_opening_phase",
    "is_middlegame_phase",
    "is_endgame_phase",
    "ply_bucket",
    "total_piece_count_before",
    "non_pawn_material_white_before",
    "non_pawn_material_black_before",
    "non_pawn_material_diff_before",
    "bishops_pair_white_before",
    "bishops_pair_black_before",
    "side_to_move_rating",
    "opponent_rating",
    "side_to_move_rating_diff",
]

TARGET_COLUMNS = [
    "final_result_class",
    "white_won_flag",
    "black_won_flag",
    "draw_flag",
]

OUTPUT_COLUMNS = METADATA_COLUMNS + FEATURE_COLUMNS + TARGET_COLUMNS
IDENTIFIER_COLUMNS = ["game_uuid", "game_url"]
AUDIT_ONLY_COLUMNS = ["result_raw", "termination"]
MODELING_FEATURE_COLUMNS = [
    column
    for column in OUTPUT_COLUMNS
    if column not in set(IDENTIFIER_COLUMNS + TARGET_COLUMNS + AUDIT_ONLY_COLUMNS)
]


# In[8]:


COLUMN_DESCRIPTIONS = {
    "game_uuid": "Stable game identifier from the Chess.com payload. Used to uniquely identify one game across all plies.",
    "game_url": "Public Chess.com game URL for auditability and raw-record traceability.",
    "archive_month": "Archive partition in YYYY-MM format derived from the monthly archive endpoint URL.",
    "game_year": "UTC calendar year of the game end timestamp. Useful for temporal filtering and partitioning.",
    "game_month": "UTC calendar month of the game end timestamp. Useful for seasonality and partitioning.",
    "game_end_timestamp": "Unix timestamp in seconds for the game end time in UTC.",
    "game_end_datetime_utc": "ISO-8601 UTC timestamp string for the game end time.",
    "rated": "Whether the game was rated on Chess.com.",
    "rules": "Ruleset reported by Chess.com, such as standard chess.",
    "time_class": "Chess.com time bucket such as bullet, blitz, rapid, or daily.",
    "time_control_raw": "Original Chess.com time-control string exactly as reported by the API.",
    "time_control_base_seconds": "Parsed base time in seconds from the raw time-control string when available.",
    "time_control_increment_seconds": "Parsed increment in seconds from the raw time-control string when available.",
    "eco_url": "Opening reference URL from Chess.com when present.",
    "eco_code": "ECO opening classification code, such as C45 or B07, when available.",
    "opening_name": "Human-readable opening name extracted from the Chess.com ECO URL slug.",
    "white_username": "White player's Chess.com username as reported by the API.",
    "black_username": "Black player's Chess.com username as reported by the API.",
    "white_rating": "White player's rating at the time of the game.",
    "black_rating": "Black player's rating at the time of the game.",
    "rating_diff": "White rating minus black rating, giving a signed pre-game rating gap.",
    "white_accuracy": "Post-game white accuracy metric from Chess.com when available.",
    "black_accuracy": "Post-game black accuracy metric from Chess.com when available.",
    "result_raw": "Raw result signals combined from PGN and API fields for auditing only. Do not use as a training feature.",
    "termination": "Raw termination text from Chess.com, such as resignation or checkmate. Audit-only, not a modeling feature.",
    "ply_index": "1-based ply number within the game. Every dataset row represents exactly one ply.",
    "fullmove_number": "Human-readable move number associated with the ply. White and black plies from the same turn share the same value.",
    "side_to_move": "Compact side-to-move indicator before the ply: w for white or b for black.",
    "side_to_move_name": "Readable side-to-move label before the ply: white or black.",
    "san_move": "Move in standard algebraic notation generated from the board state before the ply.",
    "uci_move": "Move in UCI notation for machine-friendly parsing.",
    "is_capture": "Boolean flag indicating whether the ply captures an opponent piece.",
    "is_check": "Boolean flag indicating whether the position after the ply places the opponent king in check.",
    "is_checkmate": "Boolean flag indicating whether the ply ends the game state in checkmate.",
    "is_castling": "Boolean flag indicating whether the ply is a castling move.",
    "is_promotion": "Boolean flag indicating whether the ply promotes a pawn.",
    "promotion_piece": "Name of the promoted piece when the ply is a promotion; null otherwise.",
    "from_square": "Origin square of the move in algebraic board coordinates.",
    "to_square": "Destination square of the move in algebraic board coordinates.",
    "piece_moved": "Piece type moved on this ply from a neutral perspective, such as pawn or knight.",
    "board_fen_before": "Full FEN string for the board state immediately before the ply.",
    "board_fen_after": "Full FEN string for the board state immediately after the ply.",
    "active_color_before": "Active color field from the pre-move FEN. This duplicates side_to_move in raw FEN-compatible form.",
    "castling_rights_before": "Castling-rights segment from the pre-move FEN, such as KQkq or -.",
    "en_passant_square_before": "En passant target square from the pre-move FEN, or - when not available.",
    "halfmove_clock_before": "Halfmove clock from the pre-move FEN, useful for draw-rule context.",
    "fullmove_number_before": "Fullmove counter from the pre-move FEN.",
    "legal_moves_count_before": "Number of legal moves available before the ply.",
    "material_white_before": "White material count before the ply using standard piece weights excluding kings.",
    "material_black_before": "Black material count before the ply using standard piece weights excluding kings.",
    "material_diff_before": "White material minus black material before the ply.",
    "white_clock_seconds_after": "White clock in seconds immediately after the ply when a PGN clock annotation is available.",
    "black_clock_seconds_after": "Black clock in seconds immediately after the ply when a PGN clock annotation is available.",
    "clock_seconds_after_for_side_to_move": "Clock in seconds after the ply for the player who just moved.",
    "end_hour_utc": "UTC hour when the game ended. This is a datetime-derived explanatory variable.",
    "end_weekday_utc": "UTC weekday name when the game ended. This is a datetime-derived explanatory variable.",
    "in_check_before": "Boolean flag showing whether the side to move was already in check before making the ply.",
    "is_opening_phase": "Boolean flag derived from ply index and material heuristics to indicate an opening-phase position.",
    "is_middlegame_phase": "Boolean flag derived from board-state heuristics to indicate a middlegame position.",
    "is_endgame_phase": "Boolean flag derived from board-state heuristics to indicate an endgame position.",
    "ply_bucket": "Coarse ply-range bucket in blocks of ten plies for easy aggregation.",
    "total_piece_count_before": "Total number of pieces on the board before the ply, including kings.",
    "non_pawn_material_white_before": "White non-pawn material count before the ply using standard piece weights.",
    "non_pawn_material_black_before": "Black non-pawn material count before the ply using standard piece weights.",
    "non_pawn_material_diff_before": "White non-pawn material minus black non-pawn material before the ply.",
    "bishops_pair_white_before": "Boolean flag indicating whether white still has both bishops before the ply.",
    "bishops_pair_black_before": "Boolean flag indicating whether black still has both bishops before the ply.",
    "side_to_move_rating": "Rating of the player who is about to move.",
    "opponent_rating": "Rating of the player waiting to respond.",
    "side_to_move_rating_diff": "Side-to-move rating minus opponent rating.",
    "final_result_class": "Multiclass target column with exactly three labels: white_win, black_win, or draw.",
    "white_won_flag": "Binary target flag equal to 1 when the final result is a white win.",
    "black_won_flag": "Binary target flag equal to 1 when the final result is a black win.",
    "draw_flag": "Binary target flag equal to 1 when the final result is a draw.",
}

for color in ("white", "black"):
    for piece_name in ("pawns", "knights", "bishops", "rooks", "queens"):
        column_name = f"{color}_{piece_name}_before"
        human_piece = piece_name[:-1] if piece_name.endswith("s") else piece_name
        COLUMN_DESCRIPTIONS[column_name] = (
            f"Number of {color} {human_piece} pieces on the board before the ply."
        )

    COLUMN_DESCRIPTIONS[f"{color}_can_castle_kingside_before"] = (
        f"Boolean flag indicating whether {color} still has kingside castling rights before the ply."
    )
    COLUMN_DESCRIPTIONS[f"{color}_can_castle_queenside_before"] = (
        f"Boolean flag indicating whether {color} still has queenside castling rights before the ply."
    )

COLUMN_DESCRIPTIONS.update(
    {
        "white_doubled_pawns_before": "Number of white pawns that sit on files containing at least two white pawns before the ply.",
        "black_doubled_pawns_before": "Number of black pawns that sit on files containing at least two black pawns before the ply.",
        "doubled_pawns_diff_before": "White doubled-pawn count minus black doubled-pawn count before the ply.",
        "white_isolated_pawns_before": "Number of white pawns with no supporting white pawn on an adjacent file before the ply.",
        "black_isolated_pawns_before": "Number of black pawns with no supporting black pawn on an adjacent file before the ply.",
        "isolated_pawns_diff_before": "White isolated-pawn count minus black isolated-pawn count before the ply.",
        "white_passed_pawns_before": "Number of white passed pawns before the ply, using same-file and adjacent-file blockers ahead of the pawn.",
        "black_passed_pawns_before": "Number of black passed pawns before the ply, using same-file and adjacent-file blockers ahead of the pawn.",
        "passed_pawns_diff_before": "White passed-pawn count minus black passed-pawn count before the ply.",
        "white_pawn_shield_score_before": "Friendly pawn-shield score in front of the white king before the ply, using king file and adjacent files over the next two ranks.",
        "black_pawn_shield_score_before": "Friendly pawn-shield score in front of the black king before the ply, using king file and adjacent files over the next two ranks.",
        "pawn_shield_diff_before": "White pawn-shield score minus black pawn-shield score before the ply.",
        "white_king_tropism_before": "Sum of enemy non-pawn piece closeness scores around the white king before the ply using Chebyshev distance.",
        "black_king_tropism_before": "Sum of enemy non-pawn piece closeness scores around the black king before the ply using Chebyshev distance.",
        "king_tropism_diff_before": "White king-tropism score minus black king-tropism score before the ply.",
        "side_to_move_clock_before": "Clock in seconds for the player about to move, reconstructed from sequential per-game clock state before the ply.",
        "time_spent_on_move_seconds": "Estimated seconds spent on the current move using reconstructed pre-move clock, post-move clock, and increment.",
        "clock_remaining_pct": "Fraction of starting base time still remaining for the mover before the ply.",
        "avg_time_spent_per_move_so_far": "Mean time spent on this player's previous moves in the same game, excluding the current move.",
        "is_in_time_trouble_30s": "Nullable flag indicating whether the mover had fewer than 30 seconds before the ply.",
    }
)


# ## Utility Parsing Functions
#
# Small helpers keep the downstream cells readable and make edge-case handling explicit.

# In[9]:


def safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_archive_month_from_url(url: str) -> str:
    match = ARCHIVE_MONTH_PATTERN.search(url)
    if not match:
        raise ValueError(f"Could not parse archive month from URL: {url}")
    return f"{match.group('year')}-{match.group('month')}"


def archive_year_from_url(url: str) -> int:
    return int(parse_archive_month_from_url(url).split("-")[0])


def epoch_to_utc_datetime(epoch_seconds: int | None) -> datetime | None:
    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def slug_to_opening_name(url: str | None) -> str | None:
    if not url:
        return None
    slug = Path(urlparse(url).path).name
    if not slug:
        return None
    cleaned = unquote(slug).replace("-", " ")
    return cleaned.strip() or None


def parse_time_control(time_control_raw: str | None) -> tuple[int | None, int | None]:
    if not time_control_raw:
        return (None, None)
    if "+" in time_control_raw:
        base_part, increment_part = time_control_raw.split("+", maxsplit=1)
        return (safe_int(base_part), safe_int(increment_part))
    if "/" in time_control_raw:
        return (None, None)
    return (safe_int(time_control_raw), 0)


def parse_clock_string_to_seconds(clock_string: str | None) -> float | None:
    if not clock_string:
        return None
    try:
        hours_text, minutes_text, seconds_text = clock_string.split(":")
        return int(hours_text) * 3600 + int(minutes_text) * 60 + float(seconds_text)
    except (TypeError, ValueError):
        return None


def extract_clock_seconds(comment_text: str | None) -> float | None:
    if not comment_text:
        return None
    match = CLOCK_PATTERN.search(comment_text)
    if not match:
        return None
    return parse_clock_string_to_seconds(match.group(1))


def make_ply_bucket(ply_index: int, bucket_size: int = 10) -> str:
    start_value = ((ply_index - 1) // bucket_size) * bucket_size + 1
    end_value = start_value + bucket_size - 1
    return f"{start_value:04d}-{end_value:04d}"


def bytes_to_megabytes(byte_count: int) -> float:
    return byte_count / 1_000_000


def weekday_name_from_datetime(dt: datetime | None) -> str | None:
    return dt.strftime("%A") if dt else None


# ## Chess.com API Fetching Helpers
#
# These functions handle archive discovery and resilient JSON retrieval with retries and exponential backoff.

# In[10]:


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    return session


def fetch_json_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    max_retries: int,
    backoff_base_seconds: float,
    context: str,
) -> dict[str, Any]:
    last_exception: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt == max_retries:
                break
            sleep_seconds = backoff_base_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Fetch failed for %s on attempt %s/%s; retrying in %.1f seconds: %s",
                context,
                attempt,
                max_retries,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"Failed to fetch {context} after {max_retries} attempts"
    ) from last_exception


def discover_archive_urls(session: requests.Session, username: str) -> list[str]:
    archives_endpoint = ARCHIVES_ENDPOINT_TEMPLATE.format(username=username.lower())
    payload = fetch_json_with_retries(
        session,
        archives_endpoint,
        timeout=REQUEST_TIMEOUT,
        max_retries=MAX_RETRIES,
        backoff_base_seconds=BACKOFF_BASE_SECONDS,
        context="archives index",
    )
    archive_urls = payload.get("archives")
    if not isinstance(archive_urls, list):
        raise RuntimeError("Archives response did not include a valid 'archives' list")
    return sorted(archive_urls)


# ## Archive Caching and Manifest Writing
#
# Each monthly archive is cached to local JSON for reproducibility and logged into an archive manifest.

# In[11]:


@dataclass(slots=True)
class ArchiveFetchRecord:
    archive_month: str
    archive_url: str
    cache_path: Path
    status: str
    cache_used: bool
    game_count: int
    error_message: str | None = None


def archive_cache_path(archive_url: str) -> Path:
    archive_month = parse_archive_month_from_url(archive_url)
    return RAW_CACHE_DIR / f"{archive_month}.json"


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def ensure_archive_cached(
    session: requests.Session,
    archive_url: str,
    *,
    force_refetch: bool,
) -> ArchiveFetchRecord:
    archive_month = parse_archive_month_from_url(archive_url)
    cache_path = archive_cache_path(archive_url)

    if cache_path.exists() and not force_refetch:
        try:
            payload = load_json_file(cache_path)
            games = payload.get("games", [])
            return ArchiveFetchRecord(
                archive_month=archive_month,
                archive_url=archive_url,
                cache_path=cache_path,
                status="cached",
                cache_used=True,
                game_count=len(games),
            )
        except Exception as exc:
            logger.warning(
                "Corrupted cache detected for %s; refetching. Error: %s",
                archive_month,
                exc,
            )

    try:
        payload = fetch_json_with_retries(
            session,
            archive_url,
            timeout=REQUEST_TIMEOUT,
            max_retries=MAX_RETRIES,
            backoff_base_seconds=BACKOFF_BASE_SECONDS,
            context=f"archive {archive_month}",
        )
        write_json_file(cache_path, payload)
        games = payload.get("games", [])
        return ArchiveFetchRecord(
            archive_month=archive_month,
            archive_url=archive_url,
            cache_path=cache_path,
            status="fetched",
            cache_used=False,
            game_count=len(games),
        )
    except Exception as exc:
        return ArchiveFetchRecord(
            archive_month=archive_month,
            archive_url=archive_url,
            cache_path=cache_path,
            status="failed",
            cache_used=False,
            game_count=0,
            error_message=str(exc),
        )


def write_archive_manifest(records: list[ArchiveFetchRecord]) -> None:
    with ARCHIVE_MANIFEST_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "archive_month",
                "archive_url",
                "cache_path",
                "status",
                "cache_used",
                "game_count",
                "error_message",
            ],
        )
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "archive_month": record.archive_month,
                    "archive_url": record.archive_url,
                    "cache_path": str(record.cache_path),
                    "status": record.status,
                    "cache_used": record.cache_used,
                    "game_count": record.game_count,
                    "error_message": record.error_message or "",
                }
            )


# ## PGN and Result Parsing Helpers
#
# This section resolves end timestamps, combines raw result signals, and maps them safely into the three target classes.

# In[12]:


def parse_pgn_game(pgn_text: str) -> chess.pgn.Game | None:
    return chess.pgn.read_game(io.StringIO(pgn_text))


def resolve_end_datetime(
    game_payload: dict[str, Any],
    pgn_headers: dict[str, Any],
) -> tuple[int | None, datetime | None]:
    end_timestamp = safe_int(game_payload.get("end_time"))
    if end_timestamp is not None:
        return end_timestamp, epoch_to_utc_datetime(end_timestamp)

    end_date = pgn_headers.get("EndDate") or pgn_headers.get("UTCDate")
    end_time = pgn_headers.get("EndTime") or pgn_headers.get("UTCTime")
    if end_date and end_time:
        try:
            dt = datetime.strptime(
                f"{end_date} {end_time}", "%Y.%m.%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)
            return int(dt.timestamp()), dt
        except ValueError:
            return None, None

    return None, None


def extract_result_raw(
    game_payload: dict[str, Any], pgn_headers: dict[str, Any]
) -> str:
    parts = []
    pgn_result = pgn_headers.get("Result")
    white_result = game_payload.get("white", {}).get("result")
    black_result = game_payload.get("black", {}).get("result")

    if pgn_result:
        parts.append(f"pgn:{pgn_result}")
    if white_result:
        parts.append(f"white:{white_result}")
    if black_result:
        parts.append(f"black:{black_result}")

    return " | ".join(parts)


def normalize_final_result_class(
    game_payload: dict[str, Any],
    pgn_headers: dict[str, Any],
) -> str | None:
    candidates: set[str] = set()

    pgn_result = pgn_headers.get("Result")
    if pgn_result in PGN_RESULT_TO_CLASS:
        candidates.add(PGN_RESULT_TO_CLASS[pgn_result])

    white_result = str(game_payload.get("white", {}).get("result", "")).strip().lower()
    black_result = str(game_payload.get("black", {}).get("result", "")).strip().lower()
    termination_text = (
        str(game_payload.get("termination") or pgn_headers.get("Termination") or "")
        .strip()
        .lower()
    )

    if white_result == "win" and black_result != "win":
        candidates.add("white_win")
    if black_result == "win" and white_result != "win":
        candidates.add("black_win")
    if white_result in DRAW_RESULT_CODES and black_result in DRAW_RESULT_CODES:
        candidates.add("draw")
    if any(snippet in termination_text for snippet in DRAW_TERMINATION_SNIPPETS):
        candidates.add("draw")

    if len(candidates) == 1:
        return next(iter(candidates))
    return None


# ## Board-State and Phase Features
#
# Compute material, piece counts, castling rights, and coarse opening or middlegame or endgame labels before each ply.

# In[13]:


def compute_piece_counts(board: chess.Board, color: chess.Color) -> dict[str, int]:
    return {
        "pawns": len(board.pieces(chess.PAWN, color)),
        "knights": len(board.pieces(chess.KNIGHT, color)),
        "bishops": len(board.pieces(chess.BISHOP, color)),
        "rooks": len(board.pieces(chess.ROOK, color)),
        "queens": len(board.pieces(chess.QUEEN, color)),
    }


def material_score_from_counts(counts: dict[str, int]) -> int:
    return (
        counts["pawns"] * PIECE_VALUE_MAP[chess.PAWN]
        + counts["knights"] * PIECE_VALUE_MAP[chess.KNIGHT]
        + counts["bishops"] * PIECE_VALUE_MAP[chess.BISHOP]
        + counts["rooks"] * PIECE_VALUE_MAP[chess.ROOK]
        + counts["queens"] * PIECE_VALUE_MAP[chess.QUEEN]
    )


def non_pawn_material_score_from_counts(counts: dict[str, int]) -> int:
    return (
        counts["knights"] * PIECE_VALUE_MAP[chess.KNIGHT]
        + counts["bishops"] * PIECE_VALUE_MAP[chess.BISHOP]
        + counts["rooks"] * PIECE_VALUE_MAP[chess.ROOK]
        + counts["queens"] * PIECE_VALUE_MAP[chess.QUEEN]
    )


def count_doubled_pawns(pawn_squares: list[chess.Square]) -> int:
    file_counts = Counter(chess.square_file(square) for square in pawn_squares)
    return sum(count for count in file_counts.values() if count >= 2)


def count_isolated_pawns(pawn_squares: list[chess.Square]) -> int:
    pawn_files = {chess.square_file(square) for square in pawn_squares}
    isolated_pawn_count = 0

    for square in pawn_squares:
        file_index = chess.square_file(square)
        if (file_index - 1 not in pawn_files) and (file_index + 1 not in pawn_files):
            isolated_pawn_count += 1

    return isolated_pawn_count


# A pawn is passed when no opposing pawn exists ahead on the same or adjacent files.
def count_passed_pawns(
    pawn_squares: list[chess.Square],
    opposing_pawn_squares: list[chess.Square],
    color: chess.Color,
) -> int:
    passed_pawn_count = 0

    for square in pawn_squares:
        file_index = chess.square_file(square)
        rank_index = chess.square_rank(square)
        is_passed = True

        for opposing_square in opposing_pawn_squares:
            opposing_file_index = chess.square_file(opposing_square)
            if abs(opposing_file_index - file_index) > 1:
                continue

            opposing_rank_index = chess.square_rank(opposing_square)
            if color == chess.WHITE and opposing_rank_index > rank_index:
                is_passed = False
                break
            if color == chess.BLACK and opposing_rank_index < rank_index:
                is_passed = False
                break

        if is_passed:
            passed_pawn_count += 1

    return passed_pawn_count


def compute_feature_diff(
    left_value: int | float | None, right_value: int | float | None
) -> int | float | None:
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def compute_pawn_shield_score(board: chess.Board, color: chess.Color) -> int | None:
    king_square = board.king(color)
    if king_square is None:
        return None

    king_file = chess.square_file(king_square)
    king_rank = chess.square_rank(king_square)
    valid_files = set(range(max(0, king_file - 1), min(7, king_file + 1) + 1))

    if color == chess.WHITE:
        candidate_ranks = {king_rank + 1, king_rank + 2}
    else:
        candidate_ranks = {king_rank - 1, king_rank - 2}

    valid_ranks = {rank_index for rank_index in candidate_ranks if 0 <= rank_index <= 7}
    shield_score = 0

    for pawn_square in board.pieces(chess.PAWN, color):
        if (
            chess.square_file(pawn_square) in valid_files
            and chess.square_rank(pawn_square) in valid_ranks
        ):
            shield_score += 1

    return shield_score


# King tropism sums simple enemy-piece proximity scores around the king square.
def compute_king_tropism_score(board: chess.Board, color: chess.Color) -> float | None:
    king_square = board.king(color)
    if king_square is None:
        return None

    king_file = chess.square_file(king_square)
    king_rank = chess.square_rank(king_square)
    enemy_color = not color
    tropism_score = 0.0

    for piece_type in (chess.KNIGHT, chess.BISHOP, chess.ROOK, chess.QUEEN):
        for piece_square in board.pieces(piece_type, enemy_color):
            file_distance = abs(chess.square_file(piece_square) - king_file)
            rank_distance = abs(chess.square_rank(piece_square) - king_rank)
            chebyshev_distance = max(file_distance, rank_distance)
            tropism_score += 1 / max(chebyshev_distance, 1)

    return tropism_score


def compute_board_state_features(board: chess.Board) -> dict[str, Any]:
    white_counts = compute_piece_counts(board, chess.WHITE)
    black_counts = compute_piece_counts(board, chess.BLACK)
    white_pawn_squares = list(board.pieces(chess.PAWN, chess.WHITE))
    black_pawn_squares = list(board.pieces(chess.PAWN, chess.BLACK))

    material_white = material_score_from_counts(white_counts)
    material_black = material_score_from_counts(black_counts)
    non_pawn_material_white = non_pawn_material_score_from_counts(white_counts)
    non_pawn_material_black = non_pawn_material_score_from_counts(black_counts)

    # Pawn-structure features are derived from the pre-move board only.
    white_doubled_pawns = count_doubled_pawns(white_pawn_squares)
    black_doubled_pawns = count_doubled_pawns(black_pawn_squares)
    white_isolated_pawns = count_isolated_pawns(white_pawn_squares)
    black_isolated_pawns = count_isolated_pawns(black_pawn_squares)
    white_passed_pawns = count_passed_pawns(
        white_pawn_squares, black_pawn_squares, chess.WHITE
    )
    black_passed_pawns = count_passed_pawns(
        black_pawn_squares, white_pawn_squares, chess.BLACK
    )
    white_pawn_shield_score = compute_pawn_shield_score(board, chess.WHITE)
    black_pawn_shield_score = compute_pawn_shield_score(board, chess.BLACK)
    white_king_tropism = compute_king_tropism_score(board, chess.WHITE)
    black_king_tropism = compute_king_tropism_score(board, chess.BLACK)

    return {
        "white_pawns_before": white_counts["pawns"],
        "white_knights_before": white_counts["knights"],
        "white_bishops_before": white_counts["bishops"],
        "white_rooks_before": white_counts["rooks"],
        "white_queens_before": white_counts["queens"],
        "black_pawns_before": black_counts["pawns"],
        "black_knights_before": black_counts["knights"],
        "black_bishops_before": black_counts["bishops"],
        "black_rooks_before": black_counts["rooks"],
        "black_queens_before": black_counts["queens"],
        "white_doubled_pawns_before": white_doubled_pawns,
        "black_doubled_pawns_before": black_doubled_pawns,
        "doubled_pawns_diff_before": white_doubled_pawns - black_doubled_pawns,
        "white_isolated_pawns_before": white_isolated_pawns,
        "black_isolated_pawns_before": black_isolated_pawns,
        "isolated_pawns_diff_before": white_isolated_pawns - black_isolated_pawns,
        "white_passed_pawns_before": white_passed_pawns,
        "black_passed_pawns_before": black_passed_pawns,
        "passed_pawns_diff_before": white_passed_pawns - black_passed_pawns,
        "white_pawn_shield_score_before": white_pawn_shield_score,
        "black_pawn_shield_score_before": black_pawn_shield_score,
        "pawn_shield_diff_before": compute_feature_diff(
            white_pawn_shield_score, black_pawn_shield_score
        ),
        "white_king_tropism_before": white_king_tropism,
        "black_king_tropism_before": black_king_tropism,
        "king_tropism_diff_before": compute_feature_diff(
            white_king_tropism, black_king_tropism
        ),
        "white_can_castle_kingside_before": board.has_kingside_castling_rights(
            chess.WHITE
        ),
        "white_can_castle_queenside_before": board.has_queenside_castling_rights(
            chess.WHITE
        ),
        "black_can_castle_kingside_before": board.has_kingside_castling_rights(
            chess.BLACK
        ),
        "black_can_castle_queenside_before": board.has_queenside_castling_rights(
            chess.BLACK
        ),
        "in_check_before": board.is_check(),
        "legal_moves_count_before": board.legal_moves.count(),
        "material_white_before": material_white,
        "material_black_before": material_black,
        "material_diff_before": material_white - material_black,
        "total_piece_count_before": len(board.piece_map()),
        "non_pawn_material_white_before": non_pawn_material_white,
        "non_pawn_material_black_before": non_pawn_material_black,
        "non_pawn_material_diff_before": non_pawn_material_white
        - non_pawn_material_black,
        "bishops_pair_white_before": white_counts["bishops"] >= 2,
        "bishops_pair_black_before": black_counts["bishops"] >= 2,
    }


def infer_phase_flags(
    board_features: dict[str, Any], ply_index: int
) -> tuple[bool, bool, bool]:
    total_piece_count = board_features["total_piece_count_before"]
    total_queens = (
        board_features["white_queens_before"] + board_features["black_queens_before"]
    )
    total_non_pawn_material = (
        board_features["non_pawn_material_white_before"]
        + board_features["non_pawn_material_black_before"]
    )

    is_opening = ply_index <= 20 and total_piece_count >= 24
    is_endgame = total_piece_count <= 12 or (
        total_queens <= 1 and total_non_pawn_material <= 20
    )
    is_middlegame = not is_opening and not is_endgame

    return is_opening, is_middlegame, is_endgame


# ## Per-Ply Row Extraction
#
# Transform one Chess.com game into a list of ply-level dataset rows using `python-chess`.

# In[14]:


class GameSkipError(Exception):
    def __init__(
        self, reason: str, *, game_uuid: str | None = None, game_url: str | None = None
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.game_uuid = game_uuid
        self.game_url = game_url


def compute_time_spent_on_move(
    side_to_move_clock_before: float | int | None,
    mover_clock_after: float | None,
    time_control_increment_seconds: int | None,
) -> float | None:
    if side_to_move_clock_before is None or mover_clock_after is None:
        return None

    time_spent_seconds = (
        float(side_to_move_clock_before)
        - float(mover_clock_after)
        + float(time_control_increment_seconds or 0)
    )
    return max(time_spent_seconds, 0.0)


def mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def build_game_rows(
    game_payload: dict[str, Any], archive_month: str
) -> list[dict[str, Any]]:
    game_url = game_payload.get("url")
    game_uuid = game_payload.get("uuid") or (
        str(game_url).rstrip("/").split("/")[-1] if game_url else None
    )

    if not game_uuid:
        raise GameSkipError("missing_game_identifier", game_url=game_url)

    pgn_text = game_payload.get("pgn")
    if not pgn_text:
        raise GameSkipError("missing_pgn", game_uuid=game_uuid, game_url=game_url)

    parsed_game = parse_pgn_game(pgn_text)
    if parsed_game is None:
        raise GameSkipError("malformed_pgn", game_uuid=game_uuid, game_url=game_url)

    headers = dict(parsed_game.headers)
    final_result_class = normalize_final_result_class(game_payload, headers)
    if final_result_class is None:
        raise GameSkipError(
            "unmapped_or_contradictory_result", game_uuid=game_uuid, game_url=game_url
        )

    end_timestamp, end_datetime = resolve_end_datetime(game_payload, headers)
    game_year = (
        end_datetime.year if end_datetime else safe_int(archive_month.split("-")[0])
    )
    game_month = (
        end_datetime.month if end_datetime else safe_int(archive_month.split("-")[1])
    )
    end_hour_utc = end_datetime.hour if end_datetime else None
    end_weekday_utc = weekday_name_from_datetime(end_datetime)

    time_control_raw = game_payload.get("time_control") or headers.get("TimeControl")
    time_control_base_seconds, time_control_increment_seconds = parse_time_control(
        time_control_raw
    )

    eco_url = (
        game_payload.get("eco") or game_payload.get("eco_url") or headers.get("ECOUrl")
    )
    eco_code = headers.get("ECO")
    opening_name = slug_to_opening_name(eco_url)

    white_rating = safe_int(
        game_payload.get("white", {}).get("rating") or headers.get("WhiteElo")
    )
    black_rating = safe_int(
        game_payload.get("black", {}).get("rating") or headers.get("BlackElo")
    )
    white_accuracy = safe_float(game_payload.get("accuracies", {}).get("white"))
    black_accuracy = safe_float(game_payload.get("accuracies", {}).get("black"))

    base_row = {
        "game_uuid": game_uuid,
        "game_url": game_url,
        "archive_month": archive_month,
        "game_year": game_year,
        "game_month": game_month,
        "game_end_timestamp": end_timestamp,
        "game_end_datetime_utc": end_datetime.isoformat() if end_datetime else None,
        "rated": game_payload.get("rated"),
        "rules": game_payload.get("rules") or "chess",
        "time_class": game_payload.get("time_class"),
        "time_control_raw": time_control_raw,
        "time_control_base_seconds": time_control_base_seconds,
        "time_control_increment_seconds": time_control_increment_seconds,
        "eco_url": eco_url,
        "eco_code": eco_code,
        "opening_name": opening_name,
        "white_username": game_payload.get("white", {}).get("username")
        or headers.get("White"),
        "black_username": game_payload.get("black", {}).get("username")
        or headers.get("Black"),
        "white_rating": white_rating,
        "black_rating": black_rating,
        "rating_diff": (
            white_rating - black_rating
            if white_rating is not None and black_rating is not None
            else None
        ),
        "white_accuracy": white_accuracy,
        "black_accuracy": black_accuracy,
        "result_raw": extract_result_raw(game_payload, headers),
        "termination": game_payload.get("termination") or headers.get("Termination"),
        "end_hour_utc": end_hour_utc,
        "end_weekday_utc": end_weekday_utc,
        "final_result_class": final_result_class,
        "white_won_flag": int(final_result_class == "white_win"),
        "black_won_flag": int(final_result_class == "black_win"),
        "draw_flag": int(final_result_class == "draw"),
    }

    rows: list[dict[str, Any]] = []
    known_clock_after = {"white": None, "black": None}
    initial_clock_seconds = (
        float(time_control_base_seconds)
        if time_control_base_seconds is not None and time_control_base_seconds > 0
        else None
    )
    # Track only clock information known up to the current ply.
    side_clock_state = {"white": initial_clock_seconds, "black": initial_clock_seconds}
    move_time_history = {"white": [], "black": []}

    for ply_index, node in enumerate(parsed_game.mainline(), start=1):
        parent = node.parent
        if parent is None:
            raise GameSkipError(
                "missing_parent_node", game_uuid=game_uuid, game_url=game_url
            )

        board_before = parent.board()
        move = node.move
        san_move = board_before.san(move)
        board_after = board_before.copy(stack=False)
        board_after.push(move)

        side_to_move_name = "white" if board_before.turn == chess.WHITE else "black"
        side_to_move = "w" if board_before.turn == chess.WHITE else "b"
        move_clock_seconds = extract_clock_seconds(node.comment)
        side_to_move_clock_before = side_clock_state[side_to_move_name]
        avg_time_spent_per_move_so_far = mean_or_none(
            move_time_history[side_to_move_name]
        )
        time_spent_on_move_seconds = compute_time_spent_on_move(
            side_to_move_clock_before,
            move_clock_seconds,
            time_control_increment_seconds,
        )
        clock_remaining_pct = (
            side_to_move_clock_before / time_control_base_seconds
            if side_to_move_clock_before is not None
            and time_control_base_seconds is not None
            and time_control_base_seconds > 0
            else None
        )
        is_in_time_trouble_30s = (
            side_to_move_clock_before < 30
            if side_to_move_clock_before is not None
            else None
        )

        if side_to_move_name == "white" and move_clock_seconds is not None:
            known_clock_after["white"] = move_clock_seconds
        if side_to_move_name == "black" and move_clock_seconds is not None:
            known_clock_after["black"] = move_clock_seconds

        piece = board_before.piece_at(move.from_square)
        board_features = compute_board_state_features(board_before)
        is_opening_phase, is_middlegame_phase, is_endgame_phase = infer_phase_flags(
            board_features, ply_index
        )

        side_to_move_rating = (
            white_rating if side_to_move_name == "white" else black_rating
        )
        opponent_rating = black_rating if side_to_move_name == "white" else white_rating

        row = dict(base_row)
        row.update(board_features)
        row.update(
            {
                "ply_index": ply_index,
                "fullmove_number": board_before.fullmove_number,
                "side_to_move": side_to_move,
                "side_to_move_name": side_to_move_name,
                "san_move": san_move,
                "uci_move": move.uci(),
                "is_capture": board_before.is_capture(move),
                "is_check": board_after.is_check(),
                "is_checkmate": board_after.is_checkmate(),
                "is_castling": board_before.is_castling(move),
                "is_promotion": move.promotion is not None,
                "promotion_piece": PIECE_NAME_MAP.get(move.promotion)
                if move.promotion
                else None,
                "from_square": chess.square_name(move.from_square),
                "to_square": chess.square_name(move.to_square),
                "piece_moved": PIECE_NAME_MAP.get(piece.piece_type) if piece else None,
                "board_fen_before": board_before.fen(),
                "board_fen_after": board_after.fen(),
                "active_color_before": side_to_move,
                "castling_rights_before": board_before.castling_xfen(),
                "en_passant_square_before": (
                    chess.square_name(board_before.ep_square)
                    if board_before.ep_square is not None
                    else "-"
                ),
                "halfmove_clock_before": board_before.halfmove_clock,
                "fullmove_number_before": board_before.fullmove_number,
                "white_clock_seconds_after": known_clock_after["white"],
                "black_clock_seconds_after": known_clock_after["black"],
                "clock_seconds_after_for_side_to_move": known_clock_after[
                    side_to_move_name
                ],
                "side_to_move_clock_before": side_to_move_clock_before,
                "time_spent_on_move_seconds": time_spent_on_move_seconds,
                "clock_remaining_pct": clock_remaining_pct,
                "avg_time_spent_per_move_so_far": avg_time_spent_per_move_so_far,
                "is_in_time_trouble_30s": is_in_time_trouble_30s,
                "end_hour_utc": end_hour_utc,
                "end_weekday_utc": end_weekday_utc,
                "is_opening_phase": is_opening_phase,
                "is_middlegame_phase": is_middlegame_phase,
                "is_endgame_phase": is_endgame_phase,
                "ply_bucket": make_ply_bucket(ply_index),
                "side_to_move_rating": side_to_move_rating,
                "opponent_rating": opponent_rating,
                "side_to_move_rating_diff": (
                    side_to_move_rating - opponent_rating
                    if side_to_move_rating is not None and opponent_rating is not None
                    else None
                ),
            }
        )

        rows.append(row)

        if time_spent_on_move_seconds is not None:
            move_time_history[side_to_move_name].append(time_spent_on_move_seconds)
        side_clock_state[side_to_move_name] = (
            move_clock_seconds if move_clock_seconds is not None else None
        )

    if not rows:
        raise GameSkipError(
            "game_without_plies", game_uuid=game_uuid, game_url=game_url
        )

    return rows


# ## Streaming Validation Helpers
#
# Track data quality, null counts, duplicates, distributions, and schema samples while rows are being written.

# In[15]:


SELECTED_UNIQUE_COLUMNS = [
    "archive_month",
    "time_class",
    "rules",
    "eco_code",
    "opening_name",
    "white_username",
    "black_username",
    "final_result_class",
]


@dataclass
class GenerationStats:
    archive_months_discovered: int = 0
    archive_months_successful: int = 0
    archive_months_failed: int = 0
    total_games_fetched: int = 0
    total_games_retained: int = 0
    total_games_processed: int = 0
    total_games_skipped: int = 0
    total_plies_written: int = 0
    rows_written: int = 0
    malformed_pgn_games: int = 0
    duplicate_game_uuid_count: int = 0
    duplicate_row_count: int = 0
    rows_with_missing_clock_values: int = 0
    rows_with_missing_accuracy_values: int = 0
    cache_hit_count: int = 0
    cache_miss_count: int = 0
    min_rating: int | None = None
    max_rating: int | None = None
    min_end_timestamp: int | None = None
    max_end_timestamp: int | None = None
    first_archive_month_processed: str | None = None
    last_archive_month_processed: str | None = None
    null_counts: Counter = field(default_factory=Counter)
    skip_reason_counts: Counter = field(default_factory=Counter)
    time_class_counts: Counter = field(default_factory=Counter)
    final_result_counts: Counter = field(default_factory=Counter)
    unique_values: dict[str, set[Any]] = field(
        default_factory=lambda: {column: set() for column in SELECTED_UNIQUE_COLUMNS}
    )
    seen_game_uuids: set[str] = field(default_factory=set)
    seen_row_keys: set[tuple[str, int]] = field(default_factory=set)
    plies_per_game: list[int] = field(default_factory=list)
    skipped_game_examples: list[dict[str, Any]] = field(default_factory=list)
    failed_archives: list[dict[str, Any]] = field(default_factory=list)
    schema_sample_rows: list[dict[str, Any]] = field(default_factory=list)

    def observe_archive_record(self, record: ArchiveFetchRecord) -> None:
        if record.status == "failed":
            self.archive_months_failed += 1
            self.failed_archives.append(
                {
                    "archive_month": record.archive_month,
                    "archive_url": record.archive_url,
                    "error_message": record.error_message,
                }
            )
            return

        self.archive_months_successful += 1
        if self.first_archive_month_processed is None:
            self.first_archive_month_processed = record.archive_month
        self.last_archive_month_processed = record.archive_month

        if record.cache_used:
            self.cache_hit_count += 1
        else:
            self.cache_miss_count += 1

    def register_duplicate_game_uuid(self, game_uuid: str) -> bool:
        if game_uuid in self.seen_game_uuids:
            self.duplicate_game_uuid_count += 1
            return True
        self.seen_game_uuids.add(game_uuid)
        return False

    def observe_processed_game(self, game_uuid: str, plies_written: int) -> None:
        self.total_games_retained += 1
        self.total_games_processed += 1
        self.total_plies_written += plies_written
        self.plies_per_game.append(plies_written)

    def observe_skipped_game(
        self, reason: str, *, game_uuid: str | None, game_url: str | None
    ) -> None:
        self.total_games_skipped += 1
        self.skip_reason_counts[reason] += 1
        if reason == "malformed_pgn":
            self.malformed_pgn_games += 1
        if len(self.skipped_game_examples) < 25:
            self.skipped_game_examples.append(
                {
                    "reason": reason,
                    "game_uuid": game_uuid,
                    "game_url": game_url,
                }
            )

    def observe_row(self, row: dict[str, Any]) -> None:
        self.rows_written += 1

        row_key = (row["game_uuid"], row["ply_index"])
        if row_key in self.seen_row_keys:
            self.duplicate_row_count += 1
        else:
            self.seen_row_keys.add(row_key)

        for column in OUTPUT_COLUMNS:
            value = row.get(column)
            if value is None or value == "":
                self.null_counts[column] += 1

        for column in SELECTED_UNIQUE_COLUMNS:
            value = row.get(column)
            if value not in (None, ""):
                self.unique_values[column].add(value)

        time_class = row.get("time_class")
        if time_class not in (None, ""):
            self.time_class_counts[time_class] += 1

        final_result_class = row.get("final_result_class")
        if final_result_class not in (None, ""):
            self.final_result_counts[final_result_class] += 1

        for rating_column in ("white_rating", "black_rating"):
            rating_value = row.get(rating_column)
            if isinstance(rating_value, int):
                self.min_rating = (
                    rating_value
                    if self.min_rating is None
                    else min(self.min_rating, rating_value)
                )
                self.max_rating = (
                    rating_value
                    if self.max_rating is None
                    else max(self.max_rating, rating_value)
                )

        end_timestamp = row.get("game_end_timestamp")
        if isinstance(end_timestamp, int):
            self.min_end_timestamp = (
                end_timestamp
                if self.min_end_timestamp is None
                else min(self.min_end_timestamp, end_timestamp)
            )
            self.max_end_timestamp = (
                end_timestamp
                if self.max_end_timestamp is None
                else max(self.max_end_timestamp, end_timestamp)
            )

        if (
            row.get("white_clock_seconds_after") is None
            or row.get("black_clock_seconds_after") is None
            or row.get("clock_seconds_after_for_side_to_move") is None
        ):
            self.rows_with_missing_clock_values += 1

        if row.get("white_accuracy") is None or row.get("black_accuracy") is None:
            self.rows_with_missing_accuracy_values += 1

        if len(self.schema_sample_rows) < SCHEMA_SAMPLE_LIMIT:
            self.schema_sample_rows.append(
                {column: row.get(column) for column in OUTPUT_COLUMNS}
            )


# ## Archive Discovery and Caching Run
#
# Discover all monthly archives for the configured account, filter by year range, and ensure the cache is populated.

# In[16]:


session = build_session()

all_archive_urls = discover_archive_urls(session, USERNAME)

stats = GenerationStats()
stats.archive_months_discovered = len(all_archive_urls)

logger.info("Discovered %s archive months", stats.archive_months_discovered)

archive_fetch_records: list[ArchiveFetchRecord] = []

for archive_url in tqdm(all_archive_urls, desc="Caching archive months"):
    record = ensure_archive_cached(session, archive_url, force_refetch=FORCE_REFETCH)
    archive_fetch_records.append(record)
    stats.observe_archive_record(record)
    if record.status == "failed":
        logger.error(
            "Failed archive %s: %s", record.archive_month, record.error_message
        )
    else:
        logger.info(
            "Archive %s ready via %s with %s games",
            record.archive_month,
            record.status,
            record.game_count,
        )

write_archive_manifest(archive_fetch_records)
successful_archive_records = [
    record for record in archive_fetch_records if record.status != "failed"
]

print(
    {
        "archive_months_discovered": stats.archive_months_discovered,
        "archive_months_successful": stats.archive_months_successful,
        "archive_months_failed": stats.archive_months_failed,
        "cache_hit_count": stats.cache_hit_count,
        "cache_miss_count": stats.cache_miss_count,
        "first_archive_month_processed": stats.first_archive_month_processed,
        "last_archive_month_processed": stats.last_archive_month_processed,
    }
)


# ## Incremental CSV Generation
#
# Read cached monthly archives, parse games safely, write CSV rows incrementally, and keep the entire output out of memory.

# In[17]:


if OUTPUT_CSV_TMP_PATH.exists():
    OUTPUT_CSV_TMP_PATH.unlink()

with OUTPUT_CSV_TMP_PATH.open("w", encoding="utf-8", newline="") as csv_handle:
    writer = csv.DictWriter(csv_handle, fieldnames=OUTPUT_COLUMNS)
    writer.writeheader()

    for archive_record in tqdm(successful_archive_records, desc="Generating ply rows"):
        archive_payload = load_json_file(archive_record.cache_path)
        games = archive_payload.get("games", [])
        stats.total_games_fetched += len(games)

        for game_payload in games:
            candidate_game_uuid = (
                game_payload.get("uuid")
                or str(game_payload.get("url", "")).rstrip("/").split("/")[-1]
            )
            if candidate_game_uuid and stats.register_duplicate_game_uuid(
                candidate_game_uuid
            ):
                stats.observe_skipped_game(
                    "duplicate_game_uuid",
                    game_uuid=candidate_game_uuid,
                    game_url=game_payload.get("url"),
                )
                logger.warning("Skipping duplicate game_uuid=%s", candidate_game_uuid)
                continue

            try:
                rows = build_game_rows(game_payload, archive_record.archive_month)
            except GameSkipError as exc:
                stats.observe_skipped_game(
                    exc.reason, game_uuid=exc.game_uuid, game_url=exc.game_url
                )
                logger.warning(
                    "Skipping game %s because of %s",
                    exc.game_uuid or exc.game_url or "<unknown>",
                    exc.reason,
                )
                continue
            except Exception as exc:
                game_uuid = game_payload.get("uuid")
                stats.observe_skipped_game(
                    "unexpected_processing_error",
                    game_uuid=game_uuid,
                    game_url=game_payload.get("url"),
                )
                logger.exception(
                    "Unexpected error while processing game %s: %s", game_uuid, exc
                )
                continue

            for row in rows:
                writer.writerow(row)
                stats.observe_row(row)

            stats.observe_processed_game(rows[0]["game_uuid"], len(rows))

OUTPUT_CSV_TMP_PATH.replace(OUTPUT_CSV_PATH)

if stats.schema_sample_rows:
    with SCHEMA_EXAMPLE_PATH.open("w", encoding="utf-8", newline="") as handle:
        sample_writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        sample_writer.writeheader()
        sample_writer.writerows(stats.schema_sample_rows)

logger.info("Dataset generation complete: %s rows written", stats.rows_written)
print(
    {
        "total_games_fetched": stats.total_games_fetched,
        "total_games_processed": stats.total_games_processed,
        "total_games_skipped": stats.total_games_skipped,
        "total_plies_written": stats.total_plies_written,
        "rows_written": stats.rows_written,
    }
)


# ## Validation and Profiling
#
# Summarize file size, row count, distributions, timestamp ranges, null counts, and duplicate checks after generation.

# In[18]:


output_file_size_bytes = (
    OUTPUT_CSV_PATH.stat().st_size if OUTPUT_CSV_PATH.exists() else 0
)
output_file_size_mb = bytes_to_megabytes(output_file_size_bytes)

average_plies_per_game = (
    stats.total_plies_written / stats.total_games_processed
    if stats.total_games_processed
    else 0.0
)

validation_summary = {
    "archive_months_discovered": stats.archive_months_discovered,
    "archive_months_successful": stats.archive_months_successful,
    "archive_months_failed": stats.archive_months_failed,
    "total_games_fetched": stats.total_games_fetched,
    "total_games_retained": stats.total_games_retained,
    "total_games_processed": stats.total_games_processed,
    "total_games_skipped": stats.total_games_skipped,
    "total_plies_written": stats.total_plies_written,
    "total_csv_rows": stats.rows_written,
    "csv_file_size_bytes": output_file_size_bytes,
    "csv_file_size_mb": round(output_file_size_mb, 2),
    "number_of_columns": len(OUTPUT_COLUMNS),
    "null_counts_per_column": {
        column: stats.null_counts.get(column, 0) for column in OUTPUT_COLUMNS
    },
    "unique_counts_selected_columns": {
        column: len(values) for column, values in stats.unique_values.items()
    },
    "min_rating": stats.min_rating,
    "max_rating": stats.max_rating,
    "min_timestamp": stats.min_end_timestamp,
    "max_timestamp": stats.max_end_timestamp,
    "min_datetime_utc": (
        epoch_to_utc_datetime(stats.min_end_timestamp).isoformat()
        if stats.min_end_timestamp is not None
        else None
    ),
    "max_datetime_utc": (
        epoch_to_utc_datetime(stats.max_end_timestamp).isoformat()
        if stats.max_end_timestamp is not None
        else None
    ),
    "time_class_distribution": dict(stats.time_class_counts),
    "final_result_class_distribution": dict(stats.final_result_counts),
    "average_plies_per_game": round(average_plies_per_game, 2),
    "duplicate_game_uuid_count": stats.duplicate_game_uuid_count,
    "duplicate_row_count": stats.duplicate_row_count,
    "malformed_pgn_games": stats.malformed_pgn_games,
    "rows_with_missing_clock_values": stats.rows_with_missing_clock_values,
    "rows_with_missing_accuracy_values": stats.rows_with_missing_accuracy_values,
    "cache_was_used": stats.cache_hit_count > 0,
    "first_archive_month_processed": stats.first_archive_month_processed,
    "last_archive_month_processed": stats.last_archive_month_processed,
    "threshold_checks": {
        "rows_at_least_500k": stats.rows_written >= 500_000,
        "file_size_at_least_500mb": output_file_size_bytes >= 500 * 1000 * 1000,
        "at_least_six_explanatory_variables": len(MODELING_FEATURE_COLUMNS) >= 6,
        "datetime_present": "game_end_datetime_utc" in OUTPUT_COLUMNS
        and "end_hour_utc" in OUTPUT_COLUMNS,
    },
}

print(json.dumps(validation_summary, indent=2))


# ## Dataset Description Output
#
# Write a plain-text description of the dataset, feature list, targets, and requirement checks.

# In[19]:


def build_dataset_description_text(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("Chess Move-Level Dataset Description")
    lines.append("=" * 40)
    lines.append("")
    lines.append("1. Brief project overview")
    lines.append(
        "This dataset was generated from the Chess.com public API for a Big Data / Spark final project. "
        "Each row represents one ply from one game, making the dataset suitable for large-scale sequence-to-tabular modeling."
    )
    lines.append("")
    lines.append("2. Source description")
    lines.append(
        f"Source account: {USERNAME}. Source system: Chess.com public archives API backed by monthly game archive endpoints."
    )
    lines.append("")
    lines.append("3. Data acquisition process from Chess.com API")
    lines.append(
        "The pipeline first requests the player archives index, filters archive months to the configured year range, "
        "fetches each monthly archive with retries and backoff, caches raw JSON locally, and then parses each game PGN."
    )
    lines.append("")
    lines.append("4. Unit of observation")
    lines.append("One row equals exactly one ply (half-move) from one Chess.com game.")
    lines.append("")
    lines.append("5. Primary ML task")
    lines.append(
        "Multiclass classification: predict the final game outcome from the current move state at a given ply."
    )
    lines.append("")
    lines.append("6. Target definition for final_result_class")
    lines.append(
        "The target is standardized into exactly three classes: white_win, black_win, and draw. "
        "Games with missing or contradictory result signals are skipped."
    )
    lines.append("")
    lines.append("7. Feature list")
    lines.append("Metadata / identifier columns")
    for column in IDENTIFIER_COLUMNS:
        lines.append(f"- {column}: {COLUMN_DESCRIPTIONS[column]}")
    lines.append("")
    lines.append("Modeling feature columns")
    for column in MODELING_FEATURE_COLUMNS:
        lines.append(f"- {column}: {COLUMN_DESCRIPTIONS[column]}")
    lines.append("")
    lines.append("Audit-only columns")
    for column in AUDIT_ONLY_COLUMNS:
        lines.append(f"- {column}: {COLUMN_DESCRIPTIONS[column]}")
    lines.append("")
    lines.append("Target columns")
    for column in TARGET_COLUMNS:
        lines.append(f"- {column}: {COLUMN_DESCRIPTIONS[column]}")
    lines.append("")
    lines.append("8. Datetime note")
    lines.append(
        "Datetime fields are included explicitly through game_end_timestamp, game_end_datetime_utc, end_hour_utc, and end_weekday_utc."
    )
    lines.append("")
    lines.append("9. Exact row count")
    lines.append(str(summary["total_csv_rows"]))
    lines.append("")
    lines.append("10. Exact file size")
    lines.append(
        f"{summary['csv_file_size_bytes']} bytes ({summary['csv_file_size_mb']} MB)"
    )
    lines.append("")
    lines.append("11. Dataset requirement checks")
    lines.append(
        f"- >= 500,000 rows: {summary['threshold_checks']['rows_at_least_500k']}"
    )
    lines.append(
        f"- >= 500 MB: {summary['threshold_checks']['file_size_at_least_500mb']}"
    )
    lines.append(
        f"- >= 6 explanatory variables: {summary['threshold_checks']['at_least_six_explanatory_variables']}"
    )
    lines.append(
        f"- datetime present: {summary['threshold_checks']['datetime_present']}"
    )
    lines.append("")
    lines.append("Feature vs target vs audit-only separation")
    lines.append(
        "Modeling feature columns include the board-state, move-context, temporal, opening, time-control, and rating fields."
    )
    lines.append(
        "Target columns are final_result_class, white_won_flag, black_won_flag, and draw_flag."
    )
    lines.append(
        "Audit-only columns are result_raw and termination because they directly reveal the final outcome."
    )
    return "\n".join(lines)


dataset_description_text = build_dataset_description_text(validation_summary)
DATASET_DESCRIPTION_PATH.write_text(dataset_description_text, encoding="utf-8")

print(DATASET_DESCRIPTION_PATH)


# ## Data Quality Report Output
#
# Write a plain-text report covering fetch failures, malformed inputs, skips, null-heavy fields, and anomalies.

# In[20]:


def build_data_quality_report_text(
    summary: dict[str, Any], current_stats: GenerationStats
) -> str:
    lines: list[str] = []
    lines.append("Chess Move-Level Dataset Quality Report")
    lines.append("=" * 40)
    lines.append("")
    lines.append("API fetch failures")
    if current_stats.failed_archives:
        for failed_archive in current_stats.failed_archives:
            lines.append(
                f"- {failed_archive['archive_month']}: {failed_archive['error_message']}"
            )
    else:
        lines.append("- None")
    lines.append("")
    lines.append(f"Malformed input count: {summary['malformed_pgn_games']}")
    lines.append(f"Skipped game count: {summary['total_games_skipped']}")
    lines.append(
        f"Rows with missing clock values: {summary['rows_with_missing_clock_values']}"
    )
    lines.append(
        f"Rows with missing accuracy values: {summary['rows_with_missing_accuracy_values']}"
    )
    lines.append(f"Duplicate game_uuid count: {summary['duplicate_game_uuid_count']}")
    lines.append(
        f"Duplicate row count using (game_uuid, ply_index): {summary['duplicate_row_count']}"
    )
    lines.append("")
    lines.append("Skipped game reasons")
    for reason, count in current_stats.skip_reason_counts.most_common():
        lines.append(f"- {reason}: {count}")
    if not current_stats.skip_reason_counts:
        lines.append("- None")
    lines.append("")
    lines.append("Counts by final_result_class")
    for result_class, count in sorted(
        summary["final_result_class_distribution"].items()
    ):
        lines.append(f"- {result_class}: {count}")
    lines.append("")
    lines.append("Null counts per column")
    for column in OUTPUT_COLUMNS:
        lines.append(f"- {column}: {summary['null_counts_per_column'][column]}")
    lines.append("")
    lines.append("Anomalies or suspicious issues")
    anomalies: list[str] = []
    if current_stats.failed_archives:
        anomalies.append(
            "One or more monthly archives could not be fetched successfully."
        )
    if summary["duplicate_game_uuid_count"] > 0:
        anomalies.append("Duplicate game UUIDs were present and skipped.")
    if summary["duplicate_row_count"] > 0:
        anomalies.append("Duplicate row keys were observed in the generated CSV.")
    if summary["rows_with_missing_clock_values"] > 0:
        anomalies.append(
            "Some rows do not contain full clock information because PGN clock annotations are not guaranteed for every move."
        )
    if summary["rows_with_missing_accuracy_values"] > 0:
        anomalies.append(
            "Some games do not expose Chess.com accuracy metrics, so those row fields remain null."
        )
    if not anomalies:
        anomalies.append(
            "No major data quality anomalies were detected beyond expected nullable fields."
        )
    for anomaly in anomalies:
        lines.append(f"- {anomaly}")
    lines.append("")
    lines.append("Example skipped games")
    if current_stats.skipped_game_examples:
        for example in current_stats.skipped_game_examples:
            lines.append(
                f"- reason={example['reason']}, game_uuid={example['game_uuid']}, game_url={example['game_url']}"
            )
    else:
        lines.append("- None")
    return "\n".join(lines)


data_quality_report_text = build_data_quality_report_text(validation_summary, stats)
DATA_QUALITY_REPORT_PATH.write_text(data_quality_report_text, encoding="utf-8")

print(DATA_QUALITY_REPORT_PATH)


# ## Final Summary
#
# Print the main output paths and a compact summary of the generated dataset.

# In[21]:


final_summary = {
    "output_csv": str(OUTPUT_CSV_PATH),
    "dataset_description": str(DATASET_DESCRIPTION_PATH),
    "data_quality_report": str(DATA_QUALITY_REPORT_PATH),
    "archive_manifest": str(ARCHIVE_MANIFEST_PATH),
    "schema_example": str(SCHEMA_EXAMPLE_PATH),
    "generation_log": str(GENERATION_LOG_PATH),
    "rows_written": validation_summary["total_csv_rows"],
    "games_processed": validation_summary["total_games_processed"],
    "games_skipped": validation_summary["total_games_skipped"],
    "csv_size_mb": validation_summary["csv_file_size_mb"],
    "final_result_distribution": validation_summary["final_result_class_distribution"],
}

print(json.dumps(final_summary, indent=2))
