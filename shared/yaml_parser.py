import yaml
from typing import Any, Optional
from pathlib import Path


class PipelineConfig:
    def __init__(self, name: str, jobs: dict[str, Any]):
        self.name = name
        self.jobs = jobs

    def get_job(self, job_name: str) -> Optional[dict[str, Any]]:
        return self.jobs.get(job_name)

    def list_jobs(self) -> list[str]:
        return list(self.jobs.keys())


class YAMLParser:
    @staticmethod
    def parse(yaml_content: str) -> PipelineConfig:
        try:
            config = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            raise ValueError(f"Ошибка парсинга YAML: {str(e)}")

        if not isinstance(config, dict):
            raise ValueError("Конфигурация должна быть словарем")

        if "name" not in config:
            raise ValueError("Отсутствует обязательное поле 'name'")

        if "jobs" not in config:
            raise ValueError("Отсутствует обязательное поле 'jobs'")

        if not isinstance(config["jobs"], dict):
            raise ValueError("Поле 'jobs' должно быть словарем")

        jobs = config["jobs"]
        for job_name, job_config in jobs.items():
            if not isinstance(job_config, dict):
                raise ValueError(f"Job '{job_name}' должен быть словарем")

            job_type = job_config.get("type", "default")

            if job_type == "timer":
                # Timer job - требует duration
                if "duration" not in job_config:
                    raise ValueError(
                        f"Job '{job_name}' типа timer должен содержать поле 'duration'"
                    )
            elif job_type == "confirmation":
                # Confirmation job - требует message
                if "message" not in job_config:
                    raise ValueError(
                        f"Job '{job_name}' типа confirmation должен содержать поле 'message'"
                    )
            elif job_type == "job_group":
                # Job group - требует jobs или job_groups
                if "jobs" not in job_config and "job_groups" not in job_config:
                    raise ValueError(
                        f"Job '{job_name}' типа job_group должен содержать 'jobs' или 'job_groups'"
                    )
            else:
                # Default job - обычная валидация
                if "steps" not in job_config:
                    raise ValueError(f"Job '{job_name}' должен содержать поле 'steps'")

                if not isinstance(job_config["steps"], list):
                    raise ValueError(f"Job '{job_name}'.steps должен быть списком")

                if "image" not in job_config:
                    has_image = False
                    for step in job_config["steps"]:
                        if isinstance(step, dict) and "image" in step:
                            has_image = True
                            break
                    if not has_image:
                        raise ValueError(
                            f"Job '{job_name}' должен содержать 'image' на уровне job или в steps"
                        )

                for i, step in enumerate(job_config["steps"]):
                    if not isinstance(step, dict):
                        raise ValueError(
                            f"Job '{job_name}', step {i} должен быть словарем"
                        )

                    if "run" not in step:
                        raise ValueError(
                            f"Job '{job_name}', step {i} должен содержать поле 'run'"
                        )

        return PipelineConfig(name=config["name"], jobs=jobs)

    @staticmethod
    def parse_file(file_path: str) -> PipelineConfig:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        return YAMLParser.parse(content)

    @staticmethod
    def validate_step(step: dict[str, Any]) -> bool:
        if not isinstance(step, dict):
            return False

        if "run" not in step:
            return False

        return True

    @staticmethod
    def extract_step_info(
        step: dict[str, Any], job_image: Optional[str] = None
    ) -> dict[str, Any]:
        step_name = step.get("name", "unnamed")
        step_image = step.get("image", job_image)
        run_command = step.get("run", "")
        env_vars = step.get("env", {})

        if not step_image:
            raise ValueError(
                f"Step '{step_name}' не содержит 'image' и job не имеет базового образа"
            )

        return {
            "name": step_name,
            "image": step_image,
            "run": run_command,
            "env": env_vars if isinstance(env_vars, dict) else {},
        }
