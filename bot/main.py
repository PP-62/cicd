import asyncio
import logging
import os
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from bot.handlers import router, init_orchestrator
from shared.config import get_config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Главная функция для запуска бота"""
    # Загружаем конфигурацию
    config = get_config()

    # Получаем токен бота из переменных окружения
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN не установлен в переменных окружения")

    # Инициализируем бота и диспетчер
    bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    # Инициализируем оркестратор с ботом
    init_orchestrator(bot)

    # Автоматически обнаруживаем пайплайны при старте
    from bot.orchestrator import get_orchestrator

    orchestrator = get_orchestrator(bot=bot)
    logger.info("Обнаружение пайплайнов...")
    await orchestrator.discover_pipelines()
    logger.info("Пайплайны обнаружены")

    # Регистрируем роутер с handlers
    dp.include_router(router)

    logger.info("Бот запущен и готов к работе")

    # Запускаем polling
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
