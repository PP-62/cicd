import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from shared.config import get_config
import aiofiles


class Logger:
    """Класс для структурированного логирования в файл"""
    
    def __init__(self):
        self.config = get_config()
        self.log_path = self.config.log_path
        self._ensure_log_dir()
    
    def _ensure_log_dir(self) -> None:
        """Создает директорию для логов если её нет"""
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _format_log_line(
        self,
        job_id: int,
        step_name: str,
        log_type: str,
        content: str,
        exit_code: Optional[int] = None
    ) -> str:
        """
        Форматирует строку лога
        
        Args:
            job_id: ID задания
            step_name: Имя шага
            log_type: Тип лога (STATUS, LOG, ERROR)
            content: Содержимое лога
            exit_code: Код выхода (опционально)
        
        Returns:
            Отформатированная строка лога
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        line = f"[{timestamp}] JOB:{job_id} STEP:{step_name} {log_type}:{content}"
        
        if exit_code is not None:
            line += f" EXIT:{exit_code}"
        
        return line
    
    async def log_status(
        self,
        job_id: int,
        step_name: str,
        status: str
    ) -> None:
        """
        Логирует статус выполнения
        
        Args:
            job_id: ID задания
            step_name: Имя шага
            status: Статус (running, success, failed, cancelled)
        """
        line = self._format_log_line(job_id, step_name, "STATUS", status)
        await self._write_line(line)
    
    async def log_output(
        self,
        job_id: int,
        step_name: str,
        content: str
    ) -> None:
        """
        Логирует вывод команды
        
        Args:
            job_id: ID задания
            step_name: Имя шага
            content: Содержимое вывода
        """
        # Разбиваем на строки для читаемости
        lines = content.split('\n')
        for line_content in lines:
            if line_content.strip():  # Пропускаем пустые строки
                line = self._format_log_line(job_id, step_name, "LOG", line_content)
                await self._write_line(line)
    
    async def log_error(
        self,
        job_id: int,
        step_name: str,
        error: str
    ) -> None:
        """
        Логирует ошибку
        
        Args:
            job_id: ID задания
            step_name: Имя шага
            error: Текст ошибки
        """
        line = self._format_log_line(job_id, step_name, "ERROR", error)
        await self._write_line(line)
    
    async def log_step_completion(
        self,
        job_id: int,
        step_name: str,
        exit_code: int
    ) -> None:
        """
        Логирует завершение шага с кодом выхода
        
        Args:
            job_id: ID задания
            step_name: Имя шага
            exit_code: Код выхода
        """
        status = "success" if exit_code == 0 else "failed"
        line = self._format_log_line(job_id, step_name, "STATUS", status, exit_code)
        await self._write_line(line)
    
    async def _write_line(self, line: str) -> None:
        """
        Записывает строку в файл логов
        
        Args:
            line: Строка для записи
        """
        async with aiofiles.open(self.log_path, mode='a', encoding='utf-8') as f:
            await f.write(line + '\n')
    
    async def get_job_logs(self, job_id: int) -> str:
        """
        Получает все логи для указанного job_id
        
        Args:
            job_id: ID задания
        
        Returns:
            Все строки логов для job_id
        """
        if not self.log_path.exists():
            return ""
        
        lines = []
        async with aiofiles.open(self.log_path, mode='r', encoding='utf-8') as f:
            async for line in f:
                if f"JOB:{job_id}" in line:
                    lines.append(line.rstrip())
        
        return '\n'.join(lines)
    
    async def get_job_status(self, job_id: int) -> Optional[str]:
        """
        Получает последний статус для job_id
        
        Args:
            job_id: ID задания
        
        Returns:
            Последний статус или None
        """
        if not self.log_path.exists():
            return None
        
        last_status = None
        async with aiofiles.open(self.log_path, mode='r', encoding='utf-8') as f:
            async for line in f:
                if f"JOB:{job_id}" in line and "STATUS:" in line:
                    # Извлекаем статус из строки
                    parts = line.split("STATUS:")
                    if len(parts) > 1:
                        status_part = parts[1].split()[0] if parts[1].split() else None
                        if status_part:
                            last_status = status_part
        
        return last_status


# Глобальный экземпляр логгера
_logger_instance: Optional[Logger] = None


def get_logger() -> Logger:
    """Получить глобальный экземпляр логгера"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = Logger()
    return _logger_instance
