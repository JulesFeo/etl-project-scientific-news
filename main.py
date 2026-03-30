import sys
from pathlib import Path

import yaml

from src.pipeline import run


def load_config(path: str = "config/config.yaml") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/config.yaml"
    config = load_config(config_path)
    run(config)


if __name__ == "__main__":
    main()
