import argparse
import shutil
from pathlib import Path

from collector.config import load_settings
from collector.discovery import run_discovery
from collector.raw_store import ArtifactStore
from collector.transport import GetOnlyTransport


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run read-only Crawlab discovery")
    parser.add_argument(
        "--project-id",
        action="append",
        required=True,
        dest="project_id",
        help="Target Crawlab project id; pass twice for the approved pair",
    )
    parser.add_argument(
        "--output-root",
        default="docs/discovery",
        help="Artifact output directory",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings(project_ids=args.project_id, output_root=args.output_root)
    shutil.rmtree(Path(settings.output_root), ignore_errors=True)
    transport = GetOnlyTransport(
        base_url=settings.base_url,
        api_key=settings.api_key,
        throttle_seconds=settings.throttle_seconds,
    )
    store = ArtifactStore(settings.output_root)
    run_discovery(settings, transport, store)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
