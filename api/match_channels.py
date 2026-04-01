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


def build_list_matches_filters(args: dict) -> dict[str, Any]:
    return {
        'league_id': args.get('league_id', DEFAULT_MATCH_LIST_LEAGUE_ID),
        'season': args.get('season'),
        'start_timestamp': args.get('start_timestamp'),
        'end_timestamp': args.get('end_timestamp'),
        'team_id': args.get('team_id'),
        'status': args.get('status'),
        'has_channels': parse_bool_arg(args.get('has_channels')),
        'broadcast_status': args.get('broadcast_status', 'matched'),
        'channel_country': args.get('channel_country', 'Spain'),
        'limit': args.get('limit'),
        'offset': args.get('offset', 0),
    }


def serialize_match_list_item(match, channel_country: Optional[str] = None) -> dict[str, Any]:
    channels_data = match.channels or {}

    # 如果指定了国家，只返回该国家的频道列表
    if channel_country:
        channels = channels_data.get(channel_country, [])
    else:
        # 未指定国家，返回原始字典
        channels = channels_data

    return {
        'fixture_id': match.fixture_id,
        'channels': channels,
    }
