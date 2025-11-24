from pathlib import Path
import duckdb
import pandas as pd
from config import DATA_ROOT, DB_PATH, FULL_REBUILD


# ---------- DuckDB数据库 ----------

def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    sql = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = ?
    """
    return con.execute(sql, [table_name]).fetchone()[0] > 0


def drop_table_if_needed(con: duckdb.DuckDBPyConnection, table_name: str):
    con.execute(f"DROP TABLE IF EXISTS {table_name}")


def ensure_import_log(con: duckdb.DuckDBPyConnection):
    """
    记录每个 csv 文件是否已导入过：
    - file_path: 完整路径
    - table_name: 对应 DuckDB 表名（quotes / tick_trades / tick_orders）
    - file_mtime: 文件修改时间（epoch 秒）
    - imported_at: 导入时间
    - rows_imported: 该次导入行数
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS import_log (
            file_path     TEXT,
            table_name    TEXT,
            file_mtime    DOUBLE,
            imported_at   TIMESTAMP,
            rows_imported BIGINT,
            PRIMARY KEY (file_path, table_name)
        )
        """
    )


# ---------- 公共清洗逻辑 ----------

KEYWORDS_STR_COL = ["编号", "代码", "序号", "委托号"]  # 名字含这些字的列一律转字符串


def _force_id_like_columns_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """
    把所有“编号 / 代码 / 序号 / 委托号”相关的列统一转成字符串。
    这些字段本质是 ID，不需要做数值运算，避免 uint64/int64 溢出。
    """
    for col in df.columns:
        if any(kw in col for kw in KEYWORDS_STR_COL):
            df[col] = df[col].astype("string")
    return df


def _drop_header_like_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    删除“重复表头”那种行：任意单元格的值 == 它所在列的列名。
    典型场景：中间多了一行 '万得代码, 交易所代码, 自然日, 时间, ...'。
    """
    if df.empty:
        return df

    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        mask |= (df[col].astype(str) == col)
    return df[~mask].copy()


def _common_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    通用清洗：
    1) 删除中间重复表头行
    2) 去掉 Unnamed 列
    3) 基于“自然日”生成 trade_date
    4) 把编号/代码类列统一转为字符串
    """
    # 0) 删掉表中间的重复表头行
    df = _drop_header_like_rows(df)

    # 1) 去掉多余 Unnamed 列
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    # 2) 生成 trade_date（基于 自然日）
    if "自然日" not in df.columns:
        raise ValueError("CSV 中找不到 '自然日' 列，无法生成 trade_date")

    # 原始自然日 → 字符串，去空格，把类似 20250102.0 结尾的 .0 去掉
    s = (
        df["自然日"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    # 只保留“正好 8 位数字”的字符串；其他视为非法日期整行丢弃
    mask_valid = s.str.match(r"^\d{8}$")
    df = df[mask_valid].copy()
    s = s[mask_valid]

    # 此时 s 里都是 20250102 这种：按 %Y%m%d 解析
    df["trade_date"] = pd.to_datetime(s, format="%Y%m%d").dt.date

    # 3) 把“编号/代码/序号/委托号”相关列统一转字符串
    df = _force_id_like_columns_to_str(df)

    return df


# 行情专用过滤：剔除成交价/量/额/笔数为 0 的行
def _filter_zero_trades_in_quotes(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["成交价", "成交量", "成交额", "成交笔数"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"行情数据中缺少这些列: {missing}")

    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    mask = (df[cols] != 0).all(axis=1)
    return df[mask].copy()


# ---------- 三类 CSV 读取 ----------

def load_quotes_csv(csv_path: Path) -> pd.DataFrame:
    """
    行情.csv：
    万得代码, 交易所代码, 自然日, 时间, 成交价, 成交量, 成交额, 成交笔数, ...
    """
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    df = _filter_zero_trades_in_quotes(df)
    return df


def load_tick_trades_csv(csv_path: Path) -> pd.DataFrame:
    """
    逐笔成交.csv：
    万得代码, 交易所代码, 自然日, 时间, 成交编号, 成交代码, 委托代码, BS标志,
    成交价格, 成交数量, ...
    """
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    return df


def load_tick_orders_csv(csv_path: Path) -> pd.DataFrame:
    """
    逐笔委托.csv：
    万得代码, 交易所代码, 自然日, 时间, 委托编号, 交易所委托号, 委托类型,
    委托代码, 委托价格, 委托数量, ...
    """
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    return df


# ---------- 通用导入（支持全量 & 增量） ----------

def ingest_category(
    con: duckdb.DuckDBPyConnection,
    file_pattern: str,
    table_name: str,
    loader_func,
    full_rebuild: bool,
):
    """
    file_pattern: 匹配的文件名，例如 "行情.csv" / "逐笔成交.csv" / "逐笔委托.csv"
    table_name:   DuckDB 里的目标表名
    loader_func:  对应的 pandas 读取函数
    full_rebuild: True=本次全量重建，False=增量导入
    """
    files = sorted(DATA_ROOT.rglob(file_pattern))
    print(f"[{table_name}] 在 {DATA_ROOT} 下找到 {len(files)} 个 {file_pattern} 文件")
    if not files:
        print(f"[{table_name}] 没找到任何 {file_pattern}，跳过。")
        return

    table_already_exists = table_exists(con, table_name)

    for csv_file in files:
        csv_path_str = str(csv_file)
        mtime = csv_file.stat().st_mtime  # 文件修改时间（秒）

        if not full_rebuild:
            # 增量模式：检查 import_log 里是否已有该文件记录且 mtime 未变化
            row = con.execute(
                """
                SELECT file_mtime FROM import_log
                WHERE file_path = ? AND table_name = ?
                """,
                [csv_path_str, table_name],
            ).fetchone()
            if row is not None and abs(row[0] - mtime) < 1e-6:
                print(f"[{table_name}] 跳过已导入且未修改: {csv_path_str}")
                continue

        print(f"[{table_name}] 导入: {csv_path_str}")
        df = loader_func(csv_file)
        rows = len(df)

        if rows == 0:
            print(f"[{table_name}] {csv_path_str} 过滤后为空，跳过。")
            con.execute(
                """
                INSERT OR REPLACE INTO import_log
                    (file_path, table_name, file_mtime, imported_at, rows_imported)
                VALUES (?, ?, ?, now(), ?)
                """,
                [csv_path_str, table_name, mtime, 0],
            )
            continue

        con.register("tmp_import", df)

        if not table_already_exists:
            # 第一次导入该表：根据 df 结构创建表
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_import")
            table_already_exists = True
        else:
            # 表已存在：按列名插入
            con.execute(
                f"INSERT INTO {table_name} BY NAME SELECT * FROM tmp_import"
            )

        con.unregister("tmp_import")

        # 记录 / 更新 import_log
        con.execute(
            """
            INSERT OR REPLACE INTO import_log
                (file_path, table_name, file_mtime, imported_at, rows_imported)
            VALUES (?, ?, ?, now(), ?)
            """,
            [csv_path_str, table_name, mtime, rows],
        )

    print(f"[{table_name}] 导入完成。")


# ---------- 主入口：三种 CSV 一次性导入 ----------

def ingest_all():
    print("DATA_ROOT:", DATA_ROOT)
    print("DB_PATH  :", DB_PATH)
    print("FULL_REBUILD:", FULL_REBUILD)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    ensure_import_log(con)

    if FULL_REBUILD:
        print("FULL_REBUILD = True，先删除旧表并清空 import_log。")
        drop_table_if_needed(con, "quotes")
        drop_table_if_needed(con, "tick_trades")
        drop_table_if_needed(con, "tick_orders")
        con.execute("DELETE FROM import_log")

    ingest_category(con, "行情.csv",   "quotes",      load_quotes_csv,      FULL_REBUILD)
    ingest_category(con, "逐笔成交.csv", "tick_trades", load_tick_trades_csv, FULL_REBUILD)
    ingest_category(con, "逐笔委托.csv", "tick_orders", load_tick_orders_csv, FULL_REBUILD)

    con.close()
    print("全部导入完成。")


if __name__ == "__main__":
    ingest_all()
