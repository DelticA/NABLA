from pathlib import Path
import duckdb
import pandas as pd

# ==== 1. 配置区 ====

DATA_ROOT = Path("/Volumes/Delta/Data/Quant")
DB_PATH = Path("/Volumes/Delta/Data/quant.duckdb")

# 这轮建议保持 True，方便重建干净的表
FULL_REBUILD = True


# ==== 2. DuckDB 帮助函数 ====

def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    sql = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = ?
    """
    return con.execute(sql, [table_name]).fetchone()[0] > 0


def drop_table_if_needed(con: duckdb.DuckDBPyConnection, table_name: str):
    con.execute(f"DROP TABLE IF EXISTS {table_name}")


# ==== 3. 公共清洗逻辑：去掉 Unnamed，生成 trade_date，并把编号/代码类列转成字符串 ====

KEYWORDS_STR_COL = ["编号", "代码", "序号", "委托号"]  # 名字里含这些字的一律当字符串


def _force_id_like_columns_to_str(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if any(kw in col for kw in KEYWORDS_STR_COL):
            df[col] = df[col].astype("string")
    return df


def _common_clean(df: pd.DataFrame) -> pd.DataFrame:
    # 1) 去掉多余 Unnamed 列
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    # 2) 生成 trade_date（基于 自然日）
    if "自然日" not in df.columns:
        raise ValueError("CSV 中找不到 '自然日' 列，无法生成 trade_date")

    df["trade_date"] = pd.to_datetime(
        df["自然日"].astype(str), format="%Y%m%d"
    ).dt.date

    # 3) 强制将所有“编号 / 代码 / 序号 / 委托号”相关列转成字符串
    df = _force_id_like_columns_to_str(df)

    return df


# ==== 4. 行情专用过滤：剔除成交价/成交量/成交额/成交笔数有 0 的行 ====

def _filter_zero_trades_in_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    对 quotes（行情）数据：
    成交价 / 成交量 / 成交额 / 成交笔数 有任意一个为 0 的行全部剔除。
    """
    cols = ["成交价", "成交量", "成交额", "成交笔数"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"行情数据中缺少这些列: {missing}")

    # 确保是数值类型（如果本来就是 float/int，这一步也没影响）
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 保留“所有这四个字段都非 0”的行
    mask = (df[cols] != 0).all(axis=1)
    df = df[mask].copy()

    return df


# ==== 5. 三类 CSV 的读取函数（用 GBK 编码） ====

def load_quotes_csv(csv_path: Path) -> pd.DataFrame:
    """
    行情.csv：
    万得代码, 交易所代码, 自然日, 时间, 成交价, 成交量, 成交额, 成交笔数, ...
    """
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    df = _filter_zero_trades_in_quotes(df)  # ★ 这里加了过滤逻辑
    return df


def load_tick_trades_csv(csv_path: Path) -> pd.DataFrame:
    """
    逐笔成交.csv：
    万得代码, 交易所代码, 自然日, 时间, 成交编号, 成交代码, 委托代码, BS标志,
    成交价格, 成交数量, ...
    """
    df = pd.read_csv(csv_path, encoding="gbk")
    df = _common_clean(df)
    # 逐笔成交里字段名是 “成交价格 / 成交数量”，和你指定的四个不一样，
    # 这里暂时不做 0 过滤，如果你也想过滤这类，我们可以单独再加一段。
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


# ==== 6. 通用导入函数：将一类文件导入指定表 ====

def ingest_category(
    con: duckdb.DuckDBPyConnection,
    file_pattern: str,
    table_name: str,
    loader_func,
):
    files = sorted(DATA_ROOT.rglob(file_pattern))
    print(f"[{table_name}] 在 {DATA_ROOT} 下找到 {len(files)} 个 {file_pattern} 文件")

    if not files:
        print(f"[{table_name}] 没找到任何 {file_pattern}，跳过。")
        return

    first_file = True

    for csv_file in files:
        print(f"[{table_name}] 导入: {csv_file}")
        df = loader_func(csv_file)

        # 注册临时视图
        con.register("tmp_import", df)

        if first_file:
            # 第一次：根据 df 结构建表
            if not table_exists(con, table_name):
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_import")
            else:
                con.execute(
                    f"INSERT INTO {table_name} BY NAME SELECT * FROM tmp_import"
                )
            first_file = False
        else:
            # 后续：按列名插入
            con.execute(
                f"INSERT INTO {table_name} BY NAME SELECT * FROM tmp_import"
            )

        con.unregister("tmp_import")

    print(f"[{table_name}] 导入完成。")


# ==== 7. 主入口：三种 CSV 一次性导入 ====

def ingest_all():
    print("DATA_ROOT:", DATA_ROOT)
    print("DB_PATH  :", DB_PATH)

    con = duckdb.connect(str(DB_PATH))

    if FULL_REBUILD:
        print("FULL_REBUILD = True，先删除旧表。")
        drop_table_if_needed(con, "quotes")
        drop_table_if_needed(con, "tick_trades")
        drop_table_if_needed(con, "tick_orders")

    # 行情
    ingest_category(
        con=con,
        file_pattern="行情.csv",
        table_name="quotes",
        loader_func=load_quotes_csv,
    )

    # 逐笔成交
    ingest_category(
        con=con,
        file_pattern="逐笔成交.csv",
        table_name="tick_trades",
        loader_func=load_tick_trades_csv,
    )

    # 逐笔委托
    ingest_category(
        con=con,
        file_pattern="逐笔委托.csv",
        table_name="tick_orders",
        loader_func=load_tick_orders_csv,
    )

    con.close()
    print("全部导入完成。")


if __name__ == "__main__":
    ingest_all()
