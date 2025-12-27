import asyncio
from typing import Dict, Any, Callable, Optional
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from shared.logger import get_logger


class TimerJobExecutor:
    """Исполнитель для job типа timer"""

    @staticmethod
    async def execute(
        job_id: int,
        job_name: str,
        job_config: Dict[str, Any],
        logger: Any,
    ) -> Dict[str, Any]:
        """Выполняет timer job"""
        duration = job_config.get("duration", 0)
        try:
            duration_seconds = int(duration)
        except (ValueError, TypeError):
            await logger.log_error(
                job_id, job_name, f"Неверное значение duration: {duration}"
            )
            return {"status": "failed", "error": "Неверное значение duration"}

        await logger.log_status(job_id, job_name, "running")
        await logger.log_output(
            job_id, job_name, f"Ожидание {duration_seconds} секунд..."
        )

        await asyncio.sleep(duration_seconds)

        await logger.log_status(job_id, job_name, "success")
        return {"status": "success"}


class ConfirmationJobExecutor:
    """Исполнитель для job типа confirmation"""

    def __init__(self, bot: Bot):
        self.bot = bot
        self.pending_confirmations: Dict[str, Dict[str, Any]] = {}

    async def execute(
        job_id: int,
        job_name: str,
        job_config: Dict[str, Any],
        logger: Any,
        chat_id: Optional[int] = None,
        message_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Выполняет confirmation job"""
        message_text = job_config.get("message", "Подтвердите выполнение")
        timeout = job_config.get("timeout", 300)  # 5 минут по умолчанию

        await logger.log_status(job_id, job_name, "waiting")

        if not chat_id or not message_id:
            await logger.log_error(
                job_id, job_name, "Confirmation job требует chat_id и message_id"
            )
            return {"status": "failed", "error": "Отсутствуют chat_id или message_id"}

        # Создаем клавиатуру с кнопками
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Подтвердить", callback_data=f"confirm_{job_id}_{job_name}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Отменить", callback_data=f"cancel_{job_id}_{job_name}"
                    ),
                ]
            ]
        )

        # Отправляем сообщение с кнопками
        try:
            sent_message = await self.bot.send_message(
                chat_id=chat_id,
                text=f"⏳ {message_text}\n\nJob: {job_name}",
                reply_to_message_id=message_id,
                reply_markup=keyboard,
            )

            # Создаем событие для ожидания подтверждения
            confirmation_event = asyncio.Event()
            confirmation_key = f"{job_id}_{job_name}"

            # Сохраняем информацию о подтверждении
            self.pending_confirmations[confirmation_key] = {
                "job_id": job_id,
                "job_name": job_name,
                "chat_id": chat_id,
                "message_id": sent_message.message_id,
                "logger": logger,
                "event": confirmation_event,
                "confirmed": None,
            }

            # Ждем подтверждения или таймаут
            try:
                await asyncio.wait_for(confirmation_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                # Таймаут
                self.pending_confirmations.pop(confirmation_key, None)
                await logger.log_error(
                    job_id, job_name, "Таймаут ожидания подтверждения"
                )
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=sent_message.message_id,
                    text=f"⏱️ Таймаут ожидания подтверждения\n\nJob: {job_name}",
                )
                return {"status": "failed", "error": "Таймаут"}

            # Проверяем результат
            result = self.pending_confirmations.pop(confirmation_key, None)
            if result and result.get("confirmed"):
                await logger.log_status(job_id, job_name, "success")
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=sent_message.message_id,
                    text=f"✅ Подтверждено\n\nJob: {job_name}",
                )
                return {"status": "success"}
            else:
                await logger.log_status(job_id, job_name, "cancelled")
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=sent_message.message_id,
                    text=f"❌ Отменено\n\nJob: {job_name}",
                )
                return {"status": "cancelled"}

        except Exception as e:
            await logger.log_error(job_id, job_name, f"Ошибка: {str(e)}")
            return {"status": "failed", "error": str(e)}

    def handle_callback(
        self, callback_data: str, user_id: int
    ) -> Optional[Dict[str, Any]]:
        """Обрабатывает callback от кнопок подтверждения"""
        if callback_data.startswith("confirm_"):
            parts = callback_data.split("_", 2)
            if len(parts) == 3:
                job_id = int(parts[1])
                job_name = parts[2]
                key = f"{job_id}_{job_name}"
                if key in self.pending_confirmations:
                    self.pending_confirmations[key]["confirmed"] = True
                    event = self.pending_confirmations[key].get("event")
                    if event:
                        event.set()
                    return {"action": "confirm", "job_id": job_id, "job_name": job_name}
        elif callback_data.startswith("cancel_"):
            parts = callback_data.split("_", 2)
            if len(parts) == 3:
                job_id = int(parts[1])
                job_name = parts[2]
                key = f"{job_id}_{job_name}"
                if key in self.pending_confirmations:
                    self.pending_confirmations[key]["confirmed"] = False
                    event = self.pending_confirmations[key].get("event")
                    if event:
                        event.set()
                    return {"action": "cancel", "job_id": job_id, "job_name": job_name}
        return None


class JobGroupExecutor:
    """Исполнитель для job типа job_group (параллельное выполнение)"""

    @staticmethod
    async def execute(
        job_id: int,
        job_name: str,
        job_config: Dict[str, Any],
        logger: Any,
        process_job_func: Callable,
        pipeline_config: Any,
        telegram_user_id: int,
    ) -> Dict[str, Any]:
        """Выполняет job_group с параллельным выполнением"""
        jobs = job_config.get("jobs", [])
        job_groups = job_config.get("job_groups", [])

        await logger.log_status(job_id, job_name, "running")

        all_tasks = []
        task_info = {}

        # Создаем задачи для jobs
        for job_item in jobs:
            if isinstance(job_item, str):
                job_item_name = job_item
                is_necessary = False
            elif isinstance(job_item, dict):
                job_item_name = job_item.get("name")
                is_necessary = job_item.get("is_necessary", False)
            else:
                continue

            task = asyncio.create_task(
                process_job_func(
                    job_id=job_id,
                    pipeline_config=pipeline_config,
                    job_name=job_item_name,
                    telegram_user_id=telegram_user_id,
                )
            )
            all_tasks.append((task, is_necessary, job_item_name))
            task_info[job_item_name] = {"is_necessary": is_necessary}

        # Создаем задачи для job_groups
        for group_item in job_groups:
            if isinstance(group_item, str):
                group_name = group_item
                is_necessary = False
            elif isinstance(group_item, dict):
                group_name = group_item.get("name")
                is_necessary = group_item.get("is_necessary", False)
            else:
                continue

            group_config = pipeline_config.get_job(group_name)
            if group_config and group_config.get("type") == "job_group":
                task = asyncio.create_task(
                    JobGroupExecutor.execute(
                        job_id=job_id,
                        job_name=group_name,
                        job_config=group_config,
                        logger=logger,
                        process_job_func=process_job_func,
                        pipeline_config=pipeline_config,
                        telegram_user_id=telegram_user_id,
                    )
                )
                all_tasks.append((task, is_necessary, group_name))
                task_info[group_name] = {"is_necessary": is_necessary}

        # Выполняем задачи параллельно
        completed = 0
        failed = 0
        status = "success"

        # Создаем список задач для gather
        task_results = {}
        for task, is_necessary, task_name in all_tasks:
            task_results[task] = (is_necessary, task_name)

        # Выполняем все задачи параллельно
        done, pending = await asyncio.wait(
            [task for task, _, _ in all_tasks], return_when=asyncio.ALL_COMPLETED
        )

        # Обрабатываем результаты
        for task in done:
            is_necessary, task_name = task_results[task]
            try:
                result = await task
                if result.get("status") == "success":
                    completed += 1
                else:
                    failed += 1
                    if is_necessary:
                        status = "failed"
                        # Отменяем оставшиеся задачи
                        for pending_task in pending:
                            pending_task.cancel()
            except asyncio.CancelledError:
                failed += 1
                if is_necessary:
                    status = "failed"
            except Exception as e:
                await logger.log_error(
                    job_id, task_name, f"Ошибка выполнения: {str(e)}"
                )
                failed += 1
                if is_necessary:
                    status = "failed"
                    # Отменяем оставшиеся задачи
                    for pending_task in pending:
                        pending_task.cancel()

        # Ждем отмены оставшихся задач
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        await logger.log_status(job_id, job_name, status)

        return {
            "status": status,
            "completed": completed,
            "failed": failed,
        }
