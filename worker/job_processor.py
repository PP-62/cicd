from typing import Dict, Any, List, Optional
from worker.docker_executor import DockerExecutor
from worker.job_executors import (
    TimerJobExecutor,
    ConfirmationJobExecutor,
    JobGroupExecutor,
)
from shared.logger import get_logger
from shared.yaml_parser import YAMLParser
import asyncio


class JobProcessor:
    """Класс для обработки заданий пайплайна"""

    def __init__(self, bot=None):
        self.docker_executor = DockerExecutor()
        self.logger = get_logger()
        self.confirmation_executor = ConfirmationJobExecutor(bot) if bot else None

    async def process_job(
        self,
        job_id: int,
        pipeline_config: Any,  # PipelineConfig
        job_name: str,
        telegram_user_id: int,
        chat_id: Optional[int] = None,
        message_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Обрабатывает job из пайплайна

        Args:
            job_id: ID задания
            pipeline_config: Объект PipelineConfig
            job_name: Имя job для выполнения
            telegram_user_id: ID пользователя Telegram

        Returns:
            Словарь с результатами: status, steps_completed, steps_failed
        """
        job_config = pipeline_config.get_job(job_name)

        if not job_config:
            await self.logger.log_error(
                job_id, "orchestrator", f"Job '{job_name}' не найден в конфигурации"
            )
            return {
                "status": "failed",
                "steps_completed": 0,
                "steps_failed": 0,
                "error": f"Job '{job_name}' не найден",
            }

        # Проверяем тип job
        job_type = job_config.get("type", "default")

        # Обработка специальных типов jobs
        if job_type == "timer":
            return await TimerJobExecutor.execute(
                job_id=job_id,
                job_name=job_name,
                job_config=job_config,
                logger=self.logger,
            )
        elif job_type == "confirmation":
            if not self.confirmation_executor:
                await self.logger.log_error(
                    job_id, job_name, "Confirmation job требует bot instance"
                )
                return {"status": "failed", "error": "Bot не инициализирован"}
            return await self.confirmation_executor.execute(
                job_id=job_id,
                job_name=job_name,
                job_config=job_config,
                logger=self.logger,
                chat_id=chat_id,
                message_id=message_id,
            )
        elif job_type == "job_group":
            return await JobGroupExecutor.execute(
                job_id=job_id,
                job_name=job_name,
                job_config=job_config,
                logger=self.logger,
                process_job_func=lambda **kwargs: self.process_job(
                    telegram_user_id=telegram_user_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    **kwargs,
                ),
                pipeline_config=pipeline_config,
                telegram_user_id=telegram_user_id,
            )

        # Обычный job со steps
        # Получаем базовый образ job
        job_image = job_config.get("image")
        steps = job_config.get("steps", [])

        if not steps:
            await self.logger.log_error(
                job_id, "orchestrator", f"Job '{job_name}' не содержит steps"
            )
            return {
                "status": "failed",
                "steps_completed": 0,
                "steps_failed": 0,
                "error": "Job не содержит steps",
            }

        # Логируем начало выполнения job
        await self.logger.log_status(job_id, job_name, "running")

        steps_completed = 0
        steps_failed = 0
        job_status = "success"

        # Выполняем шаги последовательно
        for step in steps:
            step_info = YAMLParser.extract_step_info(step, job_image)
            step_name = step_info["name"]
            step_image = step_info["image"]
            run_command = step_info["run"]
            env_vars = step_info["env"]

            # Логируем начало шага
            await self.logger.log_status(job_id, step_name, "running")

            # Убеждаемся, что образ доступен
            image_available = await self.docker_executor.pull_image(step_image)
            if not image_available:
                await self.logger.log_error(
                    job_id, step_name, f"Не удалось получить Docker образ: {step_image}"
                )
                steps_failed += 1
                job_status = "failed"
                continue

            # Выполняем шаг
            exit_code, logs = await self.docker_executor.execute_step(
                image=step_image,
                command=run_command,
                env_vars=env_vars,
                step_name=step_name,
            )

            # Логируем вывод
            if logs:
                await self.logger.log_output(job_id, step_name, logs)

            # Логируем завершение шага
            await self.logger.log_step_completion(job_id, step_name, exit_code)

            if exit_code == 0:
                steps_completed += 1
            else:
                steps_failed += 1
                job_status = "failed"
                # Если шаг упал, останавливаем выполнение job
                await self.logger.log_error(
                    job_id,
                    step_name,
                    f"Шаг завершился с ошибкой (exit code: {exit_code})",
                )
                break  # Прерываем выполнение при ошибке

        # Логируем финальный статус job
        await self.logger.log_status(job_id, job_name, job_status)

        return {
            "status": job_status,
            "steps_completed": steps_completed,
            "steps_failed": steps_failed,
        }

    async def process_pipeline(
        self,
        job_id: int,
        pipeline_config: Any,  # PipelineConfig
        telegram_user_id: int,
        chat_id: Optional[int] = None,
        message_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Обрабатывает весь пайплайн (все jobs)

        Args:
            job_id: ID задания
            pipeline_config: Объект PipelineConfig
            telegram_user_id: ID пользователя Telegram

        Returns:
            Словарь с результатами выполнения пайплайна
        """
        jobs = pipeline_config.list_jobs()

        if not jobs:
            await self.logger.log_error(
                job_id, "orchestrator", "Пайплайн не содержит jobs"
            )
            return {
                "status": "failed",
                "jobs_completed": 0,
                "jobs_failed": 0,
                "error": "Пайплайн не содержит jobs",
            }

        jobs_completed = 0
        jobs_failed = 0
        pipeline_status = "success"

        # Выполняем jobs последовательно
        for job_name in jobs:
            result = await self.process_job(
                job_id=job_id,
                pipeline_config=pipeline_config,
                job_name=job_name,
                telegram_user_id=telegram_user_id,
                chat_id=chat_id,
                message_id=message_id,
            )

            if result["status"] == "success":
                jobs_completed += 1
            else:
                jobs_failed += 1
                pipeline_status = "failed"
                # Можно продолжить выполнение других jobs или остановиться
                # Для MVP останавливаемся при первой ошибке
                break

        return {
            "status": pipeline_status,
            "jobs_completed": jobs_completed,
            "jobs_failed": jobs_failed,
        }
