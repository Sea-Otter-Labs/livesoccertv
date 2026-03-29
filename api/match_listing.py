from typing import Any, Optional


DEFAULT_MATCH_LIST_LEAGUE_ID = 140


def parse_bool_arg(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None

    normalized = value.strip().lower()
    if normalized in {'true', '1', 'yes'}:
        return True
    if normalized in {'false', '0', 'no'}:
        return False

    raise ValueError(f"Invalid boolean value: {value}")


def build_list_matches_filters(args) -> dict[str, Any]:
    return {
        'league_id': args.get('league_id', DEFAULT_MATCH_LIST_LEAGUE_ID, type=int),
        'season': args.get('season', type=int),
        'date_from': args.get('date_from', type=int),
        'date_to': args.get('date_to', type=int),
        'team_id': args.get('team_id', type=int),
        'status': args.get('status'),
        'has_channels': parse_bool_arg(args.get('has_channels')),
        'broadcast_status': args.get('broadcast_status'),
        'limit': args.get('limit', type=int),
        'offset': args.get('offset', 0, type=int),
    }


def serialize_match_list_item(match) -> dict[str, Any]:
    channels = match.channels or []
    channel_names = ','.join(
        channel.get('name') for channel in channels
        if isinstance(channel, dict) and channel.get('name')
    )

    return {
        'fixture_id': match.fixture_id,
        'league_id': match.league_id,
        'season': match.season,
        'match_timestamp_utc': match.match_timestamp_utc,
        'match_date': match.match_date.isoformat() if match.match_date else None,
        'home_team_id': match.home_team_id,
        'home_team': match.home_team_name,
        'away_team_id': match.away_team_id,
        'away_team': match.away_team_name,
        'status': match.match_status,
        'home_score': match.home_score,
        'away_score': match.away_score,
        'broadcast_match_status': match.broadcast_match_status.value if match.broadcast_match_status else None,
        'matched_confidence': float(match.matched_confidence) if match.matched_confidence is not None else None,
        'web_crawl_raw_id': match.web_crawl_raw_id,
        'channels_count': len(channels),
        'channel_names': channel_names,
        'channels': channels,
        'last_verified_at': match.last_verified_at.isoformat() if match.last_verified_at else None,
    }
