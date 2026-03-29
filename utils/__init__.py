"""
Utils Package
"""

from utils.team_normalizer import (
    normalize_team_name,
    normalize_team_names_pair,
    TeamNameNormalizer,
    normalize,
    are_teams_equal,
    SPANISH_TO_ENGLISH_CHARS,
    REMOVE_WORDS
)

from utils.time_utils import (
    utc_now_timestamp,
    datetime_to_utc_timestamp,
    utc_timestamp_to_datetime,
    parse_date_string,
    get_timezone_offset,
    get_date_range_timestamps,
    is_within_time_window,
    format_timestamp,
    parse_livesoccertv_date,
    TimeMatcher
)

from utils.match_aligner import (
    MatchResult,
    MatchCandidate,
    MatchAlignment,
    MatchAligner,
    align_matches
)

from utils.proxy_manager import (
    get_proxy_manager,
    is_proxy_enabled,
    get_proxy_url,
    get_proxy_for_chromium,
    get_proxy_for_requests,
    get_proxy_for_aiohttp,
    ProxyManager,
    ProxyConfig
)

from utils.proxy_api_client import (
    get_911_api_client,
    Proxy911APIClient
)

__all__ = [
    # Team normalizer
    'normalize_team_name',
    'normalize_team_names_pair',
    'TeamNameNormalizer',
    'normalize',
    'are_teams_equal',
    'SPANISH_TO_ENGLISH_CHARS',
    'REMOVE_WORDS',
    
    # Time utils
    'utc_now_timestamp',
    'datetime_to_utc_timestamp',
    'utc_timestamp_to_datetime',
    'parse_date_string',
    'get_timezone_offset',
    'get_date_range_timestamps',
    'is_within_time_window',
    'format_timestamp',
    'parse_livesoccertv_date',
    'TimeMatcher',
    
    # Match aligner
    'MatchResult',
    'MatchCandidate',
    'MatchAlignment',
    'MatchAligner',
    'align_matches',
    
    # Proxy manager
    'get_proxy_manager',
    'is_proxy_enabled',
    'get_proxy_url',
    'get_proxy_for_chromium',
    'get_proxy_for_requests',
    'get_proxy_for_aiohttp',
    'ProxyManager',
    'ProxyConfig',
    
    # Proxy API client
    'get_911_api_client',
    'Proxy911APIClient',
]
