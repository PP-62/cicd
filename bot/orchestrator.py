import asyncio
from typing import Dict, Any, Optional
from shared.git_service import GitService
from shared.yaml_parser import YAMLParser
from worker.job_processor import JobProcessor
from shared.logger import get_logger
from shared.pipeline_storage import get_storage


class PipelineOrchestrator:
    """Оркестратор для управления выполнением пайплайнов"""

    def __init__(self, bot=None):
        self.git_service = GitService()
        self.yaml_parser = YAMLParser()
        self.job_processor = JobProcessor(bot=bot)
        self.logger = get_logger()
        self.storage = get_storage()
        self.running_jobs: Dict[int, Dict[str, Any]] = {}  # job_id -> job_info
        self._job_counter = 0
        self.bot = bot

    def _generate_job_id(self) -> int:
        """Генерирует уникальный ID для задания"""
        self._job_counter += 1
        return self._job_counter

    async def discover_pipelines(self) -> Dict[str, Any]:
        """Автоматически обнаруживает и загружает пайплайны из GitHub"""
        try:
            pipeline_files = self.git_service.list_pipelines()
            pipelines_info = {}

            for pipeline_name in pipeline_files:
                try:
                    yaml_content = self.git_service.get_pipeline_yaml(pipeline_name)
                    pipeline_config = self.yaml_parser.parse(yaml_content)
                    pipelines_info[pipeline_name] = {
                        "name": pipeline_config.name,
                        "auto_run": False,
                    }
                except Exception as e:
                    await self.logger.log_error(
                        0,
                        "orchestrator",
                        f"Ошибка загрузки пайплайна {pipeline_name}: {str(e)}",
                    )

            # Обновляем хранилище
            self.storage.update_pipelines(pipelines_info)
            return pipelines_info
        except Exception as e:
            await self.logger.log_error(
                0, "orchestrator", f"Ошибка обнаружения пайплайнов: {str(e)}"
            )
            return {}

    async def list_pipelines(self) -> list:
        """
        Получает список доступных пайплайнов из GitHub

        Returns:
            Список имен файлов пайплайнов
        """
        try:
            pipelines = self.git_service.list_pipelines()
            return pipelines
        except Exception as e:
            await self.logger.log_error(
                0, "orchestrator", f"Ошибка получения списка пайплайнов: {str(e)}"
            )
            return []

    def get_pipelines_with_auto_run(self) -> Dict[str, bool]:
        """Получает список пайплайнов с информацией об автозапуске"""
        pipelines = self.storage._load_pipelines()
        result = {}
        for name, info in pipelines.items():
            result[name] = info.get("auto_run", False)
        return result

    async def run_pipeline(
        self,
        pipeline_name: str,
        telegram_user_id: int,
        chat_id: Optional[int] = None,
        message_id: Optional[int] = None,
    ) -> int:
        """
        Запускает выполнение пайплайна

        Args:
            pipeline_name: Имя файла пайплайна (например, "pipeline.yaml")
            telegram_user_id: ID пользователя Telegram

        Returns:
            job_id - ID созданного задания

        Raises:
            Exception: Если не удалось загрузить или распарсить конфигурацию
        """
        job_id = self._generate_job_id()

        try:
            # Загружаем YAML из GitHub
            yaml_content = self.git_service.get_pipeline_yaml(pipeline_name)

            # Парсим конфигурацию
            pipeline_config = self.yaml_parser.parse(yaml_content)

            # Сохраняем информацию о задании
            self.running_jobs[job_id] = {
                "pipeline_name": pipeline_name,
                "pipeline_config": pipeline_config,
                "telegram_user_id": telegram_user_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "status": "pending",
                "started_at": None,
            }

            # Обновляем подписку если есть
            if chat_id:
                self.storage.update_subscription_job(chat_id, pipeline_name, job_id)

            # Запускаем выполнение асинхронно
            asyncio.create_task(self._execute_pipeline(job_id))

            return job_id

        except Exception as e:
            await self.logger.log_error(
                job_id, "orchestrator", f"Ошибка запуска пайплайна: {str(e)}"
            )
            raise

    async def _execute_pipeline(self, job_id: int) -> None:
        """
        Выполняет пайплайн асинхронно

        Args:
            job_id: ID задания
        """
        job_info = self.running_jobs.get(job_id)
        if not job_info:
            return

        try:
            job_info["status"] = "running"
            job_info["started_at"] = asyncio.get_event_loop().time()

            await self.logger.log_status(job_id, "orchestrator", "running")

            pipeline_config = job_info["pipeline_config"]
            telegram_user_id = job_info["telegram_user_id"]
            chat_id = job_info.get("chat_id")
            message_id = job_info.get("message_id")

            # Запускаем периодическое обновление статуса
            if chat_id and message_id and self.bot:
                asyncio.create_task(
                    self._periodic_status_update(job_id, chat_id, message_id)
                )

            # Обрабатываем пайплайн
            result = await self.job_processor.process_pipeline(
                job_id=job_id,
                pipeline_config=pipeline_config,
                telegram_user_id=telegram_user_id,
                chat_id=chat_id,
                message_id=message_id,
            )

            # Обновляем статус
            job_info["status"] = result["status"]
            job_info["finished_at"] = asyncio.get_event_loop().time()
            job_info["result"] = result

            await self.logger.log_status(job_id, "orchestrator", result["status"])

            # Финальное обновление сообщения
            if chat_id and message_id and self.bot:
                await self._update_status_message(
                    job_id, chat_id, message_id, final=True
                )
                pipeline_name = job_info.get("pipeline_name", "unknown")
                status_emoji = "✅" if result["status"] == "success" else "❌"
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"{status_emoji} {pipeline_name} - выполнена",
                )

        except Exception as e:
            job_info["status"] = "failed"
            job_info["error"] = str(e)
            await self.logger.log_error(
                job_id, "orchestrator", f"Ошибка выполнения: {str(e)}"
            )

    async def _update_status_message(
        self, job_id: int, chat_id: int, message_id: int, final: bool = False
    ):
        """Обновляет сообщение со статусом job"""
        if not self.bot:
            return

        status_info = self.get_job_status(job_id)
        if not status_info:
            return

        status_emoji = {
            "pending": "⏳",
            "running": "🔄",
            "success": "✅",
            "failed": "❌",
            "cancelled": "🚫",
        }

        emoji = status_emoji.get(status_info["status"], "❓")
        status_text = status_info["status"].upper()
        pipeline_name = status_info.get("pipeline_name", "N/A")

        text = f"{emoji} <b>{pipeline_name}</b>\nСтатус: {status_text}"

        if status_info.get("result"):
            result = status_info["result"]
            text += f"\nJobs: {result.get('jobs_completed', 0)}/{result.get('jobs_completed', 0) + result.get('jobs_failed', 0)}"

        try:
            await self.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML"
            )
        except Exception:
            pass  # Игнорируем ошибки редактирования

    async def _periodic_status_update(self, job_id: int, chat_id: int, message_id: int):
        """Периодически обновляет статус в сообщении"""
        while True:
            await asyncio.sleep(5)  # Обновляем каждые 5 секунд

            status_info = self.get_job_status(job_id)
            if not status_info:
                break

            status = status_info.get("status")
            if status in ("success", "failed", "cancelled"):
                # Финальное обновление
                await self._update_status_message(
                    job_id, chat_id, message_id, final=True
                )
                break

            await self._update_status_message(job_id, chat_id, message_id)

    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает статус задания

        Args:
            job_id: ID задания

        Returns:
            Словарь с информацией о задании или None
        """
        job_info = self.running_jobs.get(job_id)
        if not job_info:
            return None

        return {
            "job_id": job_id,
            "pipeline_name": job_info.get("pipeline_name"),
            "status": job_info.get("status"),
            "started_at": job_info.get("started_at"),
            "finished_at": job_info.get("finished_at"),
            "result": job_info.get("result"),
            "error": job_info.get("error"),
        }

    async def get_job_logs(self, job_id: int) -> str:
        """
        Получает логи задания

        Args:
            job_id: ID задания

        Returns:
            Логи задания как строка
        """
        return await self.logger.get_job_logs(job_id)


# Глобальный экземпляр оркестратора
_orchestrator_instance: Optional[PipelineOrchestrator] = None


def get_orchestrator(bot=None) -> PipelineOrchestrator:
    """Получить глобальный экземпляр оркестратора"""
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = PipelineOrchestrator(bot=bot)
    elif bot and not _orchestrator_instance.bot:
        _orchestrator_instance.bot = bot
        from worker.job_executors import ConfirmationJobExecutor

        _orchestrator_instance.job_processor.confirmation_executor = (
            ConfirmationJobExecutor(bot)
        )
    return _orchestrator_instance
