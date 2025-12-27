import yaml
import os
import re
from pathlib import Path
from typing import Dict, Any, List
from dotenv import load_dotenv

# Загружаем переменные окружения из .env
load_dotenv()


class Config:
    """Класс для загрузки и управления конфигурацией"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        """Загружает конфигурацию из YAML файла с подстановкой переменных окружения"""
        config_file = Path(self.config_path)

        if not config_file.exists():
            raise FileNotFoundError(
                f"Конфигурационный файл не найден: {self.config_path}"
            )

        with open(config_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Подстановка переменных окружения
        content = self._substitute_env_vars(content)

        self._config = yaml.safe_load(content)
        self._validate()

    def _substitute_env_vars(self, content: str) -> str:
        """Подставляет переменные окружения в формате ${VAR_NAME}"""

        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))

        pattern = r"\$\{([^}]+)\}"
        return re.sub(pattern, replace_var, content)

    def _validate(self) -> None:
        """Валидирует обязательные поля конфигурации"""
        required_sections = ["github", "users", "docker", "logging"]

        for section in required_sections:
            if section not in self._config:
                raise ValueError(f"Отсутствует обязательная секция: {section}")

        # Валидация GitHub секции
        if "repo_url" not in self._config["github"]:
            raise ValueError("github.repo_url обязателен")

        if "token" not in self._config["github"]:
            raise ValueError("github.token обязателен")

        # Валидация users секции
        if "allowed_telegram_ids" not in self._config["users"]:
            raise ValueError("users.allowed_telegram_ids обязателен")

        if not isinstance(self._config["users"]["allowed_telegram_ids"], list):
            raise ValueError("users.allowed_telegram_ids должен быть списком")

    @property
    def github_repo_url(self) -> str:
        """URL репозитория GitHub"""
        return self._config["github"]["repo_url"]

    @property
    def github_token(self) -> str:
        """Токен GitHub"""
        return self._config["github"]["token"]

    @property
    def github_pipelines_path(self) -> str:
        """Путь к пайплайнам в репозитории"""
        return self._config["github"].get("pipelines_path", ".cicd/pipelines")

    @property
    def allowed_telegram_ids(self) -> List[int]:
        """Список разрешенных Telegram ID"""
        return [int(uid) for uid in self._config["users"]["allowed_telegram_ids"]]

    @property
    def docker_memory_limit(self) -> str:
        """Лимит памяти для Docker контейнеров"""
        return self._config["docker"].get("memory_limit", "512m")

    @property
    def docker_cpu_limit(self) -> str:
        """Лимит CPU для Docker контейнеров"""
        return self._config["docker"].get("cpu_limit", "0.5")

    @property
    def docker_socket_path(self) -> str:
        """Путь к Docker socket"""
        return self._config["docker"].get("socket_path", "/var/run/docker.sock")

    @property
    def log_dir(self) -> str:
        """Директория для логов"""
        return self._config["logging"].get("log_dir", "./logs")

    @property
    def log_file(self) -> str:
        """Имя файла логов"""
        return self._config["logging"].get("log_file", "cicd.log")

    @property
    def log_path(self) -> Path:
        """Полный путь к файлу логов"""
        log_dir = Path(self.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / self.log_file


# Глобальный экземпляр конфигурации
_config_instance: Config = None


def get_config() -> Config:
    """Получить глобальный экземпляр конфигурации"""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance
