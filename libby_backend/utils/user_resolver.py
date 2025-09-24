# =============================================================================
# USER ID RESOLVER UTILITY
# =============================================================================
# Centralized utility to handle Clerk user ID to integer conversion
# Used across all routes that accept user_id parameter

import hashlib
import logging
import os
import json
import threading

logger = logging.getLogger(__name__)

# Persistent map filename (stored alongside this module)
_MAP_FILENAME = os.path.join(os.path.dirname(__file__), ".clerk_user_id_map.json")
# Lock for in-process safety when reading/writing the map
_map_lock = threading.Lock()

# 32-bit signed integer bounds
_INT32_MIN = -2147483648
_INT32_MAX = 2147483647

# Start assigning Clerk-derived IDs from this offset to reduce accidental overlap
_START_ID = 1000000000  # fits well within 32-bit signed range

def _load_map() -> dict:
    try:
        if os.path.exists(_MAP_FILENAME):
            with open(_MAP_FILENAME, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        logger.exception("Failed to load clerk id map file")
    return {}

def _save_map(m: dict) -> None:
    try:
        with open(_MAP_FILENAME, "w", encoding="utf-8") as f:
            json.dump(m, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("Failed to save clerk id map file")


def resolve_user_id(user_id_input: str) -> int:
    """
    Convert Clerk user ID (string) to stable integer for database storage.
    
    Args:
        user_id_input (str): Clerk user ID like 'user_32Pey87dYeO7PXje4quqbRO79ih'
        
    Returns:
        int: Stable hashed integer for database use
        
    Examples:
        resolve_user_id('user_32Pey87dYeO7PXje4quqbRO79ih') -> 2734103894
        resolve_user_id('123') -> 123 (if already an integer string)
    """
    # Validate input
    if user_id_input is None:
        raise ValueError("user_id_input is required")

    # If it's already a numeric string, convert directly and ensure it's in 32-bit signed range
    if isinstance(user_id_input, str) and user_id_input.isdigit():
        val = int(user_id_input)
        if val < _INT32_MIN or val > _INT32_MAX:
            raise ValueError(f"Numeric user_id out of 32-bit range: {val}")
        return val

    # For Clerk-style IDs (non-numeric), maintain a persistent mapping to guarantee uniqueness
    with _map_lock:
        m = _load_map()
        # If already mapped, return existing value
        if user_id_input in m:
            return int(m[user_id_input])

        # Otherwise assign a new unique id
        # Compute next candidate starting from either the max existing or the start offset
        existing_vals = [int(v) for v in m.values() if isinstance(v, int) or (isinstance(v, str) and v.isdigit())]
        next_id = _START_ID
        if existing_vals:
            candidate = max(existing_vals) + 1
            if candidate <= _INT32_MAX:
                next_id = candidate
            else:
                # wrap-around find first free slot between START_ID and INT32_MAX
                used = set(existing_vals)
                for cid in range(_START_ID, _INT32_MAX + 1):
                    if cid not in used:
                        next_id = cid
                        break
                else:
                    raise RuntimeError("No available 32-bit IDs remaining for Clerk IDs")

        # Save mapping
        m[user_id_input] = next_id
        _save_map(m)
        logger.debug(f"Assigned Clerk ID mapping: {user_id_input} -> {next_id}")
        return next_id

def validate_user_id(user_id_input: str) -> tuple[bool, int | None, str | None]:
    """
    Validate and resolve user ID with error details.
    
    Args:
        user_id_input (str): User ID to validate and resolve
        
    Returns:
        tuple: (is_valid, resolved_id, error_message)
    """
    if not user_id_input:
        return False, None, "User ID is required"
    
    if not isinstance(user_id_input, str):
        return False, None, "User ID must be a string"
    
    try:
        resolved_id = resolve_user_id(user_id_input)
        return True, resolved_id, None
    except Exception as e:
        return False, None, f"Invalid user ID format: {e}"

# Decorator for routes that need user_id resolution
def with_resolved_user_id(f):
    """
    Decorator that automatically resolves user_id parameter in route functions.
    Replaces the user_id parameter with resolved integer version.
    """
    def wrapper(*args, **kwargs):
        # Check if user_id is in the kwargs (for route parameters)
        if 'user_id' in kwargs:
            original_user_id = kwargs['user_id']
            is_valid, resolved_id, error = validate_user_id(original_user_id)
            
            if not is_valid:
                from flask import jsonify
                return jsonify({
                    'success': False,
                    'error': f'Invalid user ID: {error}',
                    'original_user_id': original_user_id
                }), 400
            
            # Replace with resolved integer
            kwargs['user_id'] = resolved_id
            kwargs['original_user_id'] = original_user_id  # Keep original for logging
        
        return f(*args, **kwargs)
    
    wrapper.__name__ = f.__name__
    return wrapper

# Helper function for request body user_id resolution
def resolve_user_id_from_request(data: dict, key: str = 'user_id') -> tuple[bool, int | None, str | None]:
    """
    Resolve user_id from request data (JSON body).
    
    Args:
        data (dict): Request JSON data
        key (str): Key to look for user_id (default: 'user_id')
        
    Returns:
        tuple: (is_valid, resolved_id, error_message)
    """
    user_id_input = data.get(key)
    if not user_id_input:
        return False, None, f"Missing required field: {key}"
    
    return validate_user_id(user_id_input)
