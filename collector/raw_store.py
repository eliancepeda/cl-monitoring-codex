import json
from pathlib import Path
from typing import Any


class ArtifactStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _target_path(self, relative_path: str) -> Path:
        path = Path(relative_path)
        if path.is_absolute():
            raise ValueError("artifact path must stay within the store root")

        root = self.root.resolve()
        target = (root / path).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise ValueError("artifact path must stay within the store root") from exc

        return target

    def write_json(self, relative_path: str, payload: Any) -> str:
        target = self._target_path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return str(target)

    def write_text(self, relative_path: str, content: str) -> str:
        target = self._target_path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return str(target)
