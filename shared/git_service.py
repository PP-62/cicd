import requests
import base64
from pathlib import Path
from shared.config import get_config


class GitService:
    """Сервис для работы с GitHub API"""

    def __init__(self):
        self.config = get_config()
        self.base_url = "https://api.github.com"
        self.token = self.config.github_token
        self.repo_url = self.config.github_repo_url
        self._parse_repo_info()

    def _parse_repo_info(self) -> None:
        url = self.repo_url.strip()
        if url.endswith(".git"):
            url = url[:-4]
        if url.startswith("https://github.com/"):
            parts = url.replace("https://github.com/", "").split("/")
        elif url.startswith("git@github.com:"):
            parts = url.replace("git@github.com:", "").split("/")
        else:
            raise ValueError(
                f"Неподдерживаемый формат URL репозитория: {self.repo_url}"
            )

        if len(parts) < 2:
            raise ValueError(f"Неверный формат URL репозитория: {self.repo_url}")

        self.owner = parts[0]
        self.repo = parts[1]

    def _get_headers(self) -> dict[str, str]:
        headers = {"Accept": "application/vnd.github.v3+json"}

        if self.token:
            headers["Authorization"] = f"token {self.token}"

        return headers

    def get_file_content(self, file_path: str, branch: str = "main") -> str:
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}/contents/{file_path}"
        params = {"ref": branch}

        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()

        data = response.json()

        if data.get("encoding") == "base64":
            content = base64.b64decode(data["content"]).decode("utf-8")
        else:
            content = data.get("content", "")

        return content

    def list_pipelines(self, branch: str = "main") -> list:
        pipelines_path = self.config.github_pipelines_path
        url = (
            f"{self.base_url}/repos/{self.owner}/{self.repo}/contents/{pipelines_path}"
        )
        params = {"ref": branch}

        try:
            response = requests.get(url, headers=self._get_headers(), params=params)
            response.raise_for_status()

            files = response.json()
            pipeline_files = []

            if isinstance(files, list):
                for file_info in files:
                    if file_info.get("type") == "file" and (
                        file_info.get("name", "").endswith(".yaml")
                        or file_info.get("name", "").endswith(".yml")
                    ):
                        pipeline_files.append(file_info["name"])

            return pipeline_files
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return []
            raise

    def get_pipeline_yaml(self, pipeline_name: str, branch: str = "main") -> str:
        pipelines_path = self.config.github_pipelines_path
        file_path = f"{pipelines_path}/{pipeline_name}"

        return self.get_file_content(file_path, branch)
