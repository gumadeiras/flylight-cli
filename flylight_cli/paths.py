from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path, PureWindowsPath


APP_NAME = "flylight"


def _env_root(environ: Mapping[str, str], name: str, *, windows: bool = False) -> Path | None:
    value = environ.get(name)
    if not value:
        return None
    if windows:
        return Path(value) if PureWindowsPath(value).is_absolute() else None
    path = Path(value).expanduser()
    return path if path.is_absolute() else None


def default_cache_dir(
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: Path | None = None,
) -> Path:
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    home = Path.home() if home is None else home
    if platform == "darwin":
        return home / "Library" / "Caches" / APP_NAME
    if platform == "win32":
        for name in ("LOCALAPPDATA", "APPDATA"):
            root = _env_root(environ, name, windows=True)
            if root:
                return root / APP_NAME / "Cache"
        return home / "AppData" / "Local" / APP_NAME / "Cache"
    root = _env_root(environ, "XDG_CACHE_HOME")
    if root:
        return root / APP_NAME
    return home / ".cache" / APP_NAME


def default_data_dir(
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: Path | None = None,
) -> Path:
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    home = Path.home() if home is None else home
    if platform == "darwin":
        return home / "Library" / "Application Support" / APP_NAME
    if platform == "win32":
        for name in ("LOCALAPPDATA", "APPDATA"):
            root = _env_root(environ, name, windows=True)
            if root:
                return root / APP_NAME
        return home / "AppData" / "Local" / APP_NAME
    root = _env_root(environ, "XDG_DATA_HOME")
    if root:
        return root / APP_NAME
    return home / ".local" / "share" / APP_NAME
