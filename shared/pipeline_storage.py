import json
from pathlib import Path
from typing import Dict, Any, Set, Optional
from shared.config import get_config


class PipelineStorage:
    """Хранилище информации о пайплайнах и подписках"""

    def __init__(self):
        self.config = get_config()
        self.storage_path = Path(self.config.log_dir) / "pipelines.json"
        self.subscriptions_path = Path(self.config.log_dir) / "subscriptions.json"
        self._ensure_storage()

    def _ensure_storage(self):
        """Создает файлы хранилища если их нет"""
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.storage_path.exists():
            self._save_pipelines({})
        if not self.subscriptions_path.exists():
            self._save_subscriptions({})

    def _load_pipelines(self) -> Dict[str, Any]:
        """Загружает информацию о пайплайнах"""
        try:
            with open(self.storage_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_pipelines(self, pipelines: Dict[str, Any]):
        """Сохраняет информацию о пайплайнах"""
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(pipelines, f, indent=2, ensure_ascii=False)

    def _load_subscriptions(self) -> Dict[str, Dict[str, Any]]:
        """Загружает подписки"""
        try:
            with open(self.subscriptions_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_subscriptions(self, subscriptions: Dict[str, Dict[str, Any]]):
        """Сохраняет подписки"""
        with open(self.subscriptions_path, "w", encoding="utf-8") as f:
            json.dump(subscriptions, f, indent=2, ensure_ascii=False)

    def update_pipelines(self, pipelines: Dict[str, Any]):
        """Обновляет информацию о пайплайнах"""
        current = self._load_pipelines()
        current.update(pipelines)
        self._save_pipelines(current)

    def get_pipeline_info(self, pipeline_name: str) -> Dict[str, Any]:
        """Получает информацию о пайплайне"""
        pipelines = self._load_pipelines()
        return pipelines.get(pipeline_name, {"auto_run": False})

    def set_auto_run(self, pipeline_name: str, auto_run: bool):
        """Устанавливает автозапуск для пайплайна"""
        pipelines = self._load_pipelines()
        if pipeline_name not in pipelines:
            pipelines[pipeline_name] = {}
        pipelines[pipeline_name]["auto_run"] = auto_run
        self._save_pipelines(pipelines)

    def subscribe_chat(self, chat_id: int, pipeline_name: str, message_id: int):
        """Подписывает чат на пайплайн"""
        subscriptions = self._load_subscriptions()
        chat_key = str(chat_id)
        if chat_key not in subscriptions:
            subscriptions[chat_key] = {}
        subscriptions[chat_key][pipeline_name] = {
            "message_id": message_id,
            "job_id": None,
        }
        self._save_subscriptions(subscriptions)

    def unsubscribe_chat(self, chat_id: int, pipeline_name: str):
        """Отписывает чат от пайплайна"""
        subscriptions = self._load_subscriptions()
        chat_key = str(chat_id)
        if chat_key in subscriptions:
            subscriptions[chat_key].pop(pipeline_name, None)
            if not subscriptions[chat_key]:
                subscriptions.pop(chat_key)
        self._save_subscriptions(subscriptions)

    def get_chat_subscriptions(self, chat_id: int) -> Dict[str, Any]:
        """Получает подписки чата"""
        subscriptions = self._load_subscriptions()
        return subscriptions.get(str(chat_id), {})

    def update_subscription_job(self, chat_id: int, pipeline_name: str, job_id: int):
        """Обновляет job_id в подписке"""
        subscriptions = self._load_subscriptions()
        chat_key = str(chat_id)
        if chat_key in subscriptions and pipeline_name in subscriptions[chat_key]:
            subscriptions[chat_key][pipeline_name]["job_id"] = job_id
            self._save_subscriptions(subscriptions)

    def get_all_subscriptions(self) -> Dict[str, Dict[str, Any]]:
        """Получает все подписки"""
        return self._load_subscriptions()

    def get_subscription_info(
        self, chat_id: int, pipeline_name: str
    ) -> Optional[Dict[str, Any]]:
        """Получает информацию о подписке"""
        subscriptions = self._load_subscriptions()
        chat_key = str(chat_id)
        if chat_key in subscriptions:
            return subscriptions[chat_key].get(pipeline_name)
        return None


# Глобальный экземпляр хранилища
_storage_instance = None


def get_storage() -> PipelineStorage:
    """Получить глобальный экземпляр хранилища"""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = PipelineStorage()
    return _storage_instance
