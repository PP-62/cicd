from functools import wraps
from typing import Callable, Any
from aiogram import types
from shared.config import get_config


def check_auth(func: Callable) -> Callable:
    """
    Декоратор для проверки авторизации пользователя
    
    Проверяет, что telegram_id пользователя находится в списке разрешенных
    """
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        config = get_config()
        user_id = message.from_user.id
        
        if user_id not in config.allowed_telegram_ids:
            await message.answer(
                "❌ У вас нет доступа к этому боту.\n"
                "Обратитесь к администратору для получения доступа."
            )
            return
        
        return await func(message, *args, **kwargs)
    
    return wrapper


def is_authorized(user_id: int) -> bool:
    """
    Проверяет, авторизован ли пользователь
    
    Args:
        user_id: Telegram ID пользователя
    
    Returns:
        True если пользователь авторизован
    """
    config = get_config()
    return user_id in config.allowed_telegram_ids
