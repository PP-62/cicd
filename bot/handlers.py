from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from bot.auth import check_auth
from bot.orchestrator import get_orchestrator
from shared.logger import get_logger
from shared.pipeline_storage import get_storage

router = Router()
storage = get_storage()
logger = get_logger()

# Будет инициализирован в main.py
orchestrator = None


def init_orchestrator(bot: Bot):
    """Инициализирует оркестратор с ботом"""
    global orchestrator
    orchestrator = get_orchestrator(bot=bot)


@router.message(Command("start"))
@check_auth
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    await message.answer(
        "👋 Привет! Я CI/CD бот для управления пайплайнами.\n\n"
        "Доступные команды:\n"
        "/menu - меню пайплайнов\n"
        "/list - список доступных пайплайнов\n"
        "/run <pipeline_name> - запуск пайплайна\n"
        "/subscribe <pipeline_name> - подписка на пайплайн (в группе)\n"
        "/status <job_id> - статус задания\n"
        "/logs <job_id> - логи задания"
    )


@router.message(Command("menu"))
@check_auth
async def cmd_menu(message: Message):
    """Обработчик команды /menu - показывает все пайплайны с автозапуском"""
    if not orchestrator:
        await message.answer("❌ Бот еще не инициализирован")
        return

    try:
        await message.answer("⏳ Загружаю меню пайплайнов...")

        # Обновляем список пайплайнов
        await orchestrator.discover_pipelines()

        pipelines = await orchestrator.list_pipelines()
        auto_run_info = orchestrator.get_pipelines_with_auto_run()

        if not pipelines:
            await message.answer("📭 Пайплайны не найдены.")
            return

        text = "📋 <b>Меню пайплайнов:</b>\n\n"
        for pipeline in pipelines:
            auto_run = "✅" if auto_run_info.get(pipeline, False) else "❌"
            text += f"{auto_run} <code>{pipeline}</code>\n"

        text += "\nИспользуйте /run <имя> для запуска"

        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    """Обработчик команды /subscribe - подписка на пайплайн в групповом чате"""
    if not orchestrator:
        await message.answer("❌ Бот еще не инициализирован")
        return

    if message.chat.type == "private":
        await message.answer(
            "❌ Подписка доступна только в групповых чатах.\n"
            "Добавьте бота в группу и используйте команду там."
        )
        return

    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "❌ Укажите имя пайплайна.\nПример: /subscribe pipeline.yaml"
            )
            return

        pipeline_name = parts[1].strip()
        chat_id = message.chat.id

        # Проверяем существование пайплайна
        pipelines = await orchestrator.list_pipelines()
        if pipeline_name not in pipelines:
            await message.answer(f"❌ Пайплайн '{pipeline_name}' не найден.")
            return

        # Создаем сообщение для отслеживания
        status_message = await message.answer(
            f"📊 Подписка на пайплайн: <b>{pipeline_name}</b>\nСтатус: ожидание запуска",
            parse_mode="HTML",
        )

        # Сохраняем подписку
        storage.subscribe_chat(chat_id, pipeline_name, status_message.message_id)

        await message.answer(
            f"✅ Подписка на '{pipeline_name}' активирована.\n"
            f"Бот будет обновлять статус выполнения в этом чате."
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(Command("list"))
@check_auth
async def cmd_list(message: Message):
    """Обработчик команды /list - список пайплайнов"""
    if not orchestrator:
        await message.answer("❌ Бот еще не инициализирован")
        return

    try:
        await message.answer("⏳ Загружаю список пайплайнов...")

        pipelines = await orchestrator.list_pipelines()
        
        if not pipelines:
            await message.answer("📭 Пайплайны не найдены.")
        else:
            pipelines_list = "\n".join([f"• {p}" for p in pipelines])
            await message.answer(
                f"📋 Доступные пайплайны:\n\n{pipelines_list}\n\n"
                "Используйте /run <имя_файла> для запуска"
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка при получении списка пайплайнов: {str(e)}")


@router.message(Command("run"))
@check_auth
async def cmd_run(message: Message):
    """Обработчик команды /run - запуск пайплайна"""
    if not orchestrator:
        await message.answer("❌ Бот еще не инициализирован")
        return

    try:
        # Извлекаем имя пайплайна из команды
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "❌ Укажите имя пайплайна.\n"
                "Пример: /run pipeline.yaml"
            )
            return
        
        pipeline_name = parts[1].strip()
        user_id = message.from_user.id
        
        await message.answer(f"🚀 Запускаю пайплайн: {pipeline_name}...")
        
        chat_id = message.chat.id if message.chat.type != "private" else None
        message_id = None

        # Проверяем подписку
        if chat_id:
            subscription = storage.get_subscription_info(chat_id, pipeline_name)
            if subscription:
                message_id = subscription.get("message_id")

        job_id = await orchestrator.run_pipeline(
            pipeline_name, user_id, chat_id=chat_id, message_id=message_id
        )
        
        await message.answer(
            f"✅ Пайплайн запущен!\n"
            f"Job ID: {job_id}\n\n"
            f"Используйте /status {job_id} для проверки статуса\n"
            f"Используйте /logs {job_id} для просмотра логов"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при запуске пайплайна: {str(e)}")


@router.message(Command("status"))
@check_auth
async def cmd_status(message: Message):
    """Обработчик команды /status - статус задания"""
    if not orchestrator:
        await message.answer("❌ Бот еще не инициализирован")
        return

    try:
        # Извлекаем job_id из команды
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "❌ Укажите ID задания.\n"
                "Пример: /status 1"
            )
            return
        
        try:
            job_id = int(parts[1].strip())
        except ValueError:
            await message.answer("❌ ID задания должен быть числом.")
            return
        
        status_info = orchestrator.get_job_status(job_id)
        
        if not status_info:
            await message.answer(f"❌ Задание с ID {job_id} не найдено.")
            return
        
        # Формируем ответ
        status_emoji = {
            'pending': '⏳',
            'running': '🔄',
            'success': '✅',
            'failed': '❌',
            'cancelled': '🚫'
        }
        
        emoji = status_emoji.get(status_info['status'], '❓')
        status_text = status_info['status'].upper()
        
        response = f"{emoji} Статус задания {job_id}:\n\n"
        response += f"Пайплайн: {status_info.get('pipeline_name', 'N/A')}\n"
        response += f"Статус: {status_text}\n"
        
        if status_info.get('started_at'):
            response += f"Запущено: {status_info['started_at']}\n"
        
        if status_info.get('finished_at'):
            response += f"Завершено: {status_info['finished_at']}\n"
        
        if status_info.get('result'):
            result = status_info['result']
            response += f"\nJobs выполнено: {result.get('jobs_completed', 0)}\n"
            response += f"Jobs с ошибками: {result.get('jobs_failed', 0)}\n"
        
        if status_info.get('error'):
            response += f"\nОшибка: {status_info['error']}\n"
        
        await message.answer(response)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при получении статуса: {str(e)}")


@router.message(Command("logs"))
@check_auth
async def cmd_logs(message: Message):
    """Обработчик команды /logs - логи задания"""
    if not orchestrator:
        await message.answer("❌ Бот еще не инициализирован")
        return

    try:
        # Извлекаем job_id из команды
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer(
                "❌ Укажите ID задания.\n"
                "Пример: /logs 1"
            )
            return
        
        try:
            job_id = int(parts[1].strip())
        except ValueError:
            await message.answer("❌ ID задания должен быть числом.")
            return
        
        logs = await orchestrator.get_job_logs(job_id)
        
        if not logs:
            await message.answer(f"📭 Логи для задания {job_id} не найдены.")
            return
        
        # Telegram имеет лимит на длину сообщения (4096 символов)
        # Разбиваем логи на части если нужно
        max_length = 4000
        
        if len(logs) <= max_length:
            await message.answer(f"📋 Логи задания {job_id}:\n\n<pre>{logs}</pre>", parse_mode="HTML")
        else:
            # Отправляем последние N строк, которые помещаются
            lines = logs.split('\n')
            response_lines = []
            current_length = 0
            
            for line in reversed(lines):
                line_with_newline = line + '\n'
                if current_length + len(line_with_newline) > max_length:
                    break
                response_lines.insert(0, line_with_newline)
                current_length += len(line_with_newline)
            
            response = ''.join(response_lines)
            await message.answer(
                f"📋 Последние логи задания {job_id}:\n\n<pre>{response}</pre>",
                parse_mode="HTML"
            )
            await message.answer(
                f"ℹ️ Показаны последние {len(response_lines)} строк. "
                f"Полные логи доступны в файле."
            )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при получении логов: {str(e)}")


@router.callback_query(F.data.startswith("confirm_") | F.data.startswith("cancel_"))
async def handle_confirmation_callback(callback: CallbackQuery):
    """Обработчик callback для confirmation jobs"""
    if not orchestrator or not orchestrator.job_processor.confirmation_executor:
        await callback.answer("Ошибка: executor не инициализирован")
        return

    result = orchestrator.job_processor.confirmation_executor.handle_callback(
        callback.data, callback.from_user.id
    )

    if result:
        if result["action"] == "confirm":
            await callback.answer("✅ Подтверждено")
        elif result["action"] == "cancel":
            await callback.answer("❌ Отменено")
    else:
        await callback.answer("Ошибка обработки")
