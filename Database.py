from pathlib import Path
import duckdb
import pandas as pd

DATA_ROOT = Path("/Volumes/Delta/Data/Quant")
DB_PATH = Path("/Volumes/Delta/Data/quant.duckdb")
FULL_REBUILD = True   # 建议这轮先重建

# ========== DuckDB 小工具 ==========

def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    sql = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = ?
    """
    return con.execute(sql, [table_name]).fetchone()[0] > 0

def drop_table_if_needed(con: duckdb.DuckDBPyConnection, table_name: str):
    con.execute(f"DROP TABLE IF EXISTS {table_name}")


# ========== 公共清洗逻辑 ==========

KEYWORDS_STR_COL = ["编号", "代码", "序号", "委托号"]  # 名字含这些字的列一律转字符串

def _force_id_like_columns_to_str(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if any(kw in col for kw in KEYWORDS_STR_COL):
            df[col] = df[col].astype("string")
    return df

def _drop_header_like_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    删除“重复表头”那种行：任何单元格的值 == 它所在列的列名。
    典型场景：中间多了一行 '万得代码, 交易所代码, 自然日, 时间, ...'
    """
    # 对每一列生成一个 bool 掩码：这一列中值等于列名的地方
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        mask |= (df[col].astype(str) == col)
    # 取反，保留“不是重复表头”的行
    return df[~mask].copy()

def _common_clean(df: pd.DataFrame) -> pd.DataFrame:
    # 0) 先删掉中间重复的表头行
    df = _drop_header_like_rows(df)

    # 1) 去掉多余 Unnamed 列
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    # 2) 生成 trade_date（基于 自然日）
    if "自然日" not in df.columns:
        raise ValueError("CSV 中找不到 '自然日' 列，无法生成 trade_date")

    df["trade_date"] = pd.to_datetime(
        df["自然日"].astype(str), format="%Y%m%d"
    ).dt.date

    # 3) 把“编号/代码/序号/委托号”相关列统一转字符串，避免 uint64/int64 溢出
    df = _force_id_like_columns_to_str(df)

    return df


# ========== 行情专用：过滤成交价/量/额/笔数为 0 的行 ==========

def _filter_zero_trades_in_quotes(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["成交价", "成交量", "成交额", "成交笔数"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"行情数据中缺少这些列: {missing}")

    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    mask = (df[cols] != 0).all(axis=1)
    return df[mask].copy()


# ========== 三类 CSV 读取函数 ==========

def load_quotes_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    df = _filter_zero_trades_in_quotes(df)
    return df

def load_tick_trades_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    # 如果将来也想过滤成交价格/数量为 0，可以在这里再加一段
    return df

def load_tick_orders_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    return df


# ========== 通用导入 & 主入口（跟之前一致） ==========

def ingest_category(con, file_pattern, table_name, loader_func):
    files = sorted(DATA_ROOT.rglob(file_pattern))
    print(f"[{table_name}] 在 {DATA_ROOT} 下找到 {len(files)} 个 {file_pattern} 文件")
    if not files:
        print(f"[{table_name}] 没找到任何 {file_pattern}，跳过。")
        return

    first_file = True
    for csv_file in files:
        print(f"[{table_name}] 导入: {csv_file}")
        df = loader_func(csv_file)
        con.register("tmp_import", df)

        if first_file:
            if not table_exists(con, table_name):
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_import")
            else:
                con.execute(
                    f"INSERT INTO {table_name} BY NAME SELECT * FROM tmp_import"
                )
            first_file = False
        else:
            con.execute(
                f"INSERT INTO {table_name} BY NAME SELECT * FROM tmp_import"
            )

        con.unregister("tmp_import")

    print(f"[{table_name}] 导入完成。")

def ingest_all():
    print("DATA_ROOT:", DATA_ROOT)
    print("DB_PATH  :", DB_PATH)
    con = duckdb.connect(str(DB_PATH))

    if FULL_REBUILD:
        print("FULL_REBUILD = True，先删除旧表。")
        drop_table_if_needed(con, "quotes")
        drop_table_if_needed(con, "tick_trades")
        drop_table_if_needed(con, "tick_orders")

    ingest_category(con, "行情.csv",   "quotes",      load_quotes_csv)
    ingest_category(con, "逐笔成交.csv", "tick_trades", load_tick_trades_csv)
    ingest_category(con, "逐笔委托.csv", "tick_orders", load_tick_orders_csv)

    con.close()
    print("全部导入完成。")

if __name__ == "__main__":
    ingest_all()
