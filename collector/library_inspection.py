import re
from pathlib import Path
from zipfile import BadZipFile, ZipFile


def inspect_wheel_sources(targets: list[dict[str, str]]) -> list[dict[str, object]]:
    observations: list[dict[str, object]] = []
    for target in targets:
        wheel_path = Path(target["wheel_path"])
        internal_path = target["internal_path"]

        if not wheel_path.exists():
            observations.append(
                {
                    "wheel_path": str(wheel_path),
                    "internal_path": internal_path,
                    "status": "unknown",
                    "matched_flags": [],
                    "snippets": [],
                }
            )
            continue

        try:
            with ZipFile(wheel_path) as archive:
                if internal_path not in archive.namelist():
                    observations.append(
                        {
                            "wheel_path": str(wheel_path),
                            "internal_path": internal_path,
                            "status": "unknown",
                            "matched_flags": [],
                            "snippets": [],
                        }
                    )
                    continue

                source = archive.read(internal_path).decode("utf-8", errors="replace")
                matched_flags = sorted(set(re.findall(r"--?[a-zA-Z][a-zA-Z0-9_-]*", source)))
                snippets = [
                    line.strip()
                    for line in source.splitlines()
                    if any(flag in line for flag in matched_flags)
                ][:20]
                observations.append(
                    {
                        "wheel_path": str(wheel_path),
                        "internal_path": internal_path,
                        "status": "fact",
                        "matched_flags": matched_flags,
                        "snippets": snippets,
                    }
                )
        except BadZipFile:
            observations.append(
                {
                    "wheel_path": str(wheel_path),
                    "internal_path": internal_path,
                    "status": "unknown",
                    "matched_flags": [],
                    "snippets": [],
                }
            )

    return observations
