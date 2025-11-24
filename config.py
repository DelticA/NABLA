import configparser
from pathlib import Path
from typing import Tuple


def load_config() -> Tuple[Path, Path, bool, bool]:
    """
    读取配置：
    [paths]
    data_root = ...
    db_path   = ...

    [import]
    full_rebuild = true/false

    [main]
    ingest_on_start = true/false
    """
    cfg = configparser.ConfigParser()

    # 先读 example，再读 local，让 local 覆盖 example
    read_files = cfg.read(
        ["config.example.ini", "config.local.ini"],
        encoding="utf-8",
    )
    if not read_files:
        raise RuntimeError(
            "未找到 config.local.ini 或 config.example.ini，"
            "请先在项目根目录创建配置文件。"
        )

    if "paths" not in cfg:
        raise RuntimeError("配置文件中缺少 [paths] 段。")

    data_root = Path(cfg["paths"]["data_root"])
    db_path = Path(cfg["paths"]["db_path"])

    # 导入模式：是否每次全量重建
    full_rebuild = True
    if cfg.has_section("import") and cfg.has_option("import", "full_rebuild"):
        full_rebuild = cfg.getboolean("import", "full_rebuild")

    # main 开关：启动时是否自动执行 ingest_all()
    ingest_on_start = False  # 默认不自动导入
    if cfg.has_section("main") and cfg.has_option("main", "ingest_on_start"):
        ingest_on_start = cfg.getboolean("main", "ingest_on_start")

    return data_root, db_path, full_rebuild, ingest_on_start


DATA_ROOT, DB_PATH, FULL_REBUILD, INGEST_ON_START = load_config()
