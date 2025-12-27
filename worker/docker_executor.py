import docker
from typing import Dict, Tuple
from shared.config import get_config
import asyncio
from concurrent.futures import ThreadPoolExecutor


class DockerExecutor:
    """Класс для выполнения команд в Docker контейнерах"""

    def __init__(self):
        self.config = get_config()
        self.client = docker.DockerClient(
            base_url=f"unix://{self.config.docker_socket_path}"
        )
        self.executor = ThreadPoolExecutor(max_workers=5)

    def _convert_memory_limit(self, memory_limit: str) -> int:
        memory_limit = memory_limit.lower().strip()

        if memory_limit.endswith("k"):
            return int(memory_limit[:-1]) * 1024
        elif memory_limit.endswith("m"):
            return int(memory_limit[:-1]) * 1024 * 1024
        elif memory_limit.endswith("g"):
            return int(memory_limit[:-1]) * 1024 * 1024 * 1024
        else:
            return int(memory_limit)

    async def execute_step(
        self, image: str, command: str, env_vars: Dict[str, str], step_name: str
    ) -> Tuple[int, str]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, self._execute_step_sync, image, command, env_vars, step_name
        )

    def _execute_step_sync(
        self, image: str, command: str, env_vars: Dict[str, str], step_name: str
    ) -> Tuple[int, str]:
        container = None
        try:
            # Подготовка переменных окружения
            env_list = [f"{k}={v}" for k, v in env_vars.items()]

            # Создание контейнера
            container = self.client.containers.create(
                image=image,
                command=["/bin/sh", "-c", command],
                environment=env_list,
                mem_limit=self._convert_memory_limit(self.config.docker_memory_limit),
                cpu_quota=int(float(self.config.docker_cpu_limit) * 100000),
                cpu_period=100000,
                detach=True,
                remove=False,  # Не удаляем автоматически, чтобы получить логи
                network_disabled=False,
                privileged=False,  # Безопасность: без привилегий
            )

            # Запуск контейнера
            container.start()

            # Ожидание завершения с таймаутом
            try:
                result = container.wait(timeout=3600)  # 1 час максимум
                exit_code = result.get("StatusCode", 1)
            except Exception as e:
                # Если таймаут или другая ошибка
                container.stop(timeout=10)
                exit_code = 1
                logs = f"Ошибка выполнения: {str(e)}"
                return exit_code, logs

            # Получение логов
            logs = container.logs(stdout=True, stderr=True, timestamps=False).decode(
                "utf-8", errors="replace"
            )

            return exit_code, logs

        except docker.errors.ImageNotFound:
            return (
                1,
                f"Ошибка: Docker образ '{image}' не найден. Убедитесь, что образ доступен.",
            )
        except docker.errors.APIError as e:
            return 1, f"Ошибка Docker API: {str(e)}"
        except Exception as e:
            return 1, f"Неожиданная ошибка: {str(e)}"
        finally:
            # Удаление контейнера
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass  # Игнорируем ошибки при удалении

    async def pull_image(self, image: str) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._pull_image_sync, image)

    def _pull_image_sync(self, image: str) -> bool:
        try:
            # Проверяем, есть ли образ локально
            try:
                self.client.images.get(image)
                return True
            except docker.errors.ImageNotFound:
                pass

            # Скачиваем образ
            self.client.images.pull(image)
            return True
        except Exception as e:
            print(f"Ошибка при скачивании образа {image}: {str(e)}")
            return False
