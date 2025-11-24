from pathlib import Path
from datetime import date, datetime
from typing import Optional, Sequence, Union
import duckdb
import pandas as pd
from config import DB_PATH

DateLike = Union[str, date, datetime]


class QuantDatabase:
    """
    封装对 DuckDB 的读取操作，给回测/策略使用。
    """

    def __init__(self, db_path: Optional[Union[str, Path]] = None, read_only: bool = True):
        if db_path is None:
            db_path = DB_PATH
        self.db_path = Path(db_path)
        self.con = duckdb.connect(str(self.db_path), read_only=read_only)

    # ---- 小工具 ----

    @staticmethod
    def _to_date(d: DateLike) -> date:
        if isinstance(d, datetime):
            return d.date()
        if isinstance(d, date):
            return d
        return pd.to_datetime(d).date()

    def close(self):
        self.con.close()

    @staticmethod
    def _attach_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
        """
        用 trade_date + 时间 生成一个带毫秒精度的 datetime 索引。

        约定：
        - trade_date: ingest 阶段已经清洗好的日期列 (DATE)
        - 时间:
            * 6 位: HHMMSS              -> 毫秒=000
            * 7~9 位: HHMMSSmmm 变体   -> 取最后 3 位为毫秒
        其他格式一律视为非法，整行丢弃。
        """
        if "trade_date" not in df.columns or "时间" not in df.columns or df.empty:
            return df

        # 日期部分：直接用 trade_date
        date_str = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")

        # 时间原始字符串
        time_raw = (
            df["时间"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)  # 去掉可能的 .0
        )

        # 只保留数字
        time_digits = time_raw.str.replace(r"\D", "", regex=True)
        len_digits = time_digits.str.len()

        # 合法长度：6 或 >6
        mask_valid = (len_digits == 6) | (len_digits > 6)
        bad = (~mask_valid).sum()
        if bad:
            print(f"[QuantDatabase] 有 {bad} 条时间格式异常记录被丢弃（长度不是 6 或 >6）。")

        df = df[mask_valid].copy()
        if df.empty:
            return df

        date_str = date_str[mask_valid]
        time_digits = time_digits[mask_valid]
        len_digits = len_digits[mask_valid]

        # 无毫秒：HHMMSS
        mask_no_ms = (len_digits == 6)
        mask_with_ms = ~mask_no_ms

        h = pd.Series(index=time_digits.index, dtype="string")
        m = pd.Series(index=time_digits.index, dtype="string")
        s = pd.Series(index=time_digits.index, dtype="string")
        ms = pd.Series("000", index=time_digits.index, dtype="string")

        # 1) 无毫秒
        if mask_no_ms.any():
            t6 = time_digits[mask_no_ms].str.zfill(6)
            h[mask_no_ms] = t6.str.slice(0, 2)
            m[mask_no_ms] = t6.str.slice(2, 4)
            s[mask_no_ms] = t6.str.slice(4, 6)

        # 2) 有毫秒：取最后 3 位为毫秒，前面作为 HHMMSS
        if mask_with_ms.any():
            td = time_digits[mask_with_ms]
            ms_part = td.str.slice(-3)
            hms_part = td.str.slice(0, -3).str.zfill(6)

            h[mask_with_ms] = hms_part.str.slice(0, 2)
            m[mask_with_ms] = hms_part.str.slice(2, 4)
            s[mask_with_ms] = hms_part.str.slice(4, 6)
            ms[mask_with_ms] = ms_part

        # 毫秒 -> 微秒（3 位 + 3 个 0）
        micro_str = ms + "000"

        dt_str = (
            date_str.astype(str)
            + h.astype(str)
            + m.astype(str)
            + s.astype(str)
            + micro_str.astype(str)
        )

        # 最终应为 8+2+2+2+6 = 20 位数字
        mask_dt = dt_str.str.match(r"^\d{20}$")
        bad2 = (~mask_dt).sum()
        if bad2:
            print(f"[QuantDatabase] 有 {bad2} 条日期时间组合异常被丢弃。")
            df = df[mask_dt].copy()
            dt_str = dt_str[mask_dt]

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(dt_str, format="%Y%m%d%H%M%S%f")
        df = df.set_index("datetime").sort_index()
        return df

    # ---- 常用查询接口 ----

    def list_symbols(self) -> pd.DataFrame:
        """
        返回 quotes 表中出现过的所有万得代码。
        """
        return self.con.execute(
            "SELECT DISTINCT 万得代码 AS symbol FROM quotes ORDER BY symbol"
        ).df()

    def load_quotes(
        self,
        symbol: str,
        start_date: DateLike,
        end_date: DateLike,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """
        读取区间内某只股票的分笔行情（quotes 表）。
        """
        start_d = self._to_date(start_date)
        end_d = self._to_date(end_date)

        cols_expr = "*" if columns is None else ", ".join(columns)

        sql = f"""
        SELECT {cols_expr}
        FROM quotes
        WHERE 万得代码 = ?
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date, 时间
        """

        df = self.con.execute(sql, [symbol, start_d, end_d]).df()
        df = self._attach_datetime_index(df)
        return df

    def load_tick_trades(
        self,
        symbol: str,
        trade_date: DateLike,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """
        读取某一天某只股票的逐笔成交（tick_trades 表）。
        """
        d = self._to_date(trade_date)
        cols_expr = "*" if columns is None else ", ".join(columns)

        sql = f"""
        SELECT {cols_expr}
        FROM tick_trades
        WHERE 万得代码 = ?
          AND trade_date = ?
        ORDER BY 自然日, 时间
        """

        df = self.con.execute(sql, [symbol, d]).df()
        df = self._attach_datetime_index(df)
        return df

    def load_tick_orders(
        self,
        symbol: str,
        trade_date: DateLike,
        columns: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """
        读取某一天某只股票的逐笔委托（tick_orders 表）。
        """
        d = self._to_date(trade_date)
        cols_expr = "*" if columns is None else ", ".join(columns)

        sql = f"""
        SELECT {cols_expr}
        FROM tick_orders
        WHERE 万得代码 = ?
          AND trade_date = ?
        ORDER BY 自然日, 时间
        """

        df = self.con.execute(sql, [symbol, d]).df()
        df = self._attach_datetime_index(df)
        return df
