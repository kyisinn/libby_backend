# =============================================================================
# USER ID RESOLVER UTILITY
# =============================================================================
# Centralized utility to handle Clerk user ID to integer conversion
# Used across all routes that accept user_id parameter

import hashlib
import logging

logger = logging.getLogger(__name__)

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
    try:
        # If it's already a numeric string, convert directly
        if user_id_input.isdigit():
            return int(user_id_input)
        
        # Hash the Clerk ID to create a stable integer
        # Use MD5 for speed (security not critical here, just need consistency)
        hash_object = hashlib.md5(user_id_input.encode())
        # Take first 8 characters of hex and convert to int
        user_id_int = int(hash_object.hexdigest()[:8], 16)
        
        logger.debug(f"Resolved user_id: {user_id_input} -> {user_id_int}")
        return user_id_int
        
    except (ValueError, AttributeError) as e:
        logger.error(f"Failed to resolve user_id '{user_id_input}': {e}")
        # Fallback to a default hash if conversion fails
        return int(hashlib.md5(str(user_id_input).encode()).hexdigest()[:8], 16)

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
