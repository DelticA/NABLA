from __future__ import print_function, absolute_import
import os
import time
import chinese_calendar
from datetime import date, datetime, timedelta
import pandas as pd
# from conda_build.os_utils.liefldd import get_symbols  # ✂ 建议移除：未使用
# from sympy import false                               # ✂ 建议移除：未使用

def get_tradeday(start_str, end_str):
    start = datetime.strptime(start_str, '%Y-%m-%d')
    end   = datetime.strptime(end_str,   '%Y-%m-%d')
    lst = chinese_calendar.get_workdays(start, end)
    # 保险剔周末
    lst = [d for d in lst if d.isoweekday() not in (6, 7)]
    return [d.strftime('%Y-%m-%d') for d in lst]

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns', None)

today = date.today().strftime("%Y-%m-%d")
# 你的代码里固定成某天也可以，例如：
# today = '2025-10-10'

SAVE_DIR = r"D:\DATA_daily"
os.makedirs(SAVE_DIR, exist_ok=True)

def read_existing_wide_csv(path: str):
    """返回 (df, max_date_str)。若文件不存在或无效，df为空，max_date_str=None。"""
    if not os.path.exists(path):
        return pd.DataFrame(), None

    df = pd.read_csv(path, index_col=0)
    if df.empty:
        return pd.DataFrame(), None

    # 解析索引为日期
    idx = pd.to_datetime(df.index.astype(str), errors='coerce')

    # ✅ 关键修复：DatetimeIndex 直接用 .strftime（不能用 .dt）
    # NaT 先暂时填补为最小日期，避免 .strftime 失败；随后再回填为 None
    mask_nat = idx.isna()
    if mask_nat.any():
        idx_filled = idx.copy()
        idx_filled[mask_nat] = pd.Timestamp("1900-01-01")
        df.index = pd.Index(idx_filled.strftime("%Y-%m-%d"))
        # 把原本的 NaT 行重新标记回去（可选：如果你希望直接丢弃无效索引，也可以 df = df[~mask_nat]）
        df.index = df.index.where(~mask_nat, other=None)
    else:
        df.index = pd.Index(idx.strftime("%Y-%m-%d"))

    # 计算最大有效日期
    max_dt = pd.to_datetime(df.index, errors='coerce').max()
    max_date = None if pd.isna(max_dt) else max_dt.strftime('%Y-%m-%d')
    return df, max_date

# ========== 辅助函数：把增量“宽表”与历史合并（行=日期；列=ts_code；同日保留最后一行） ==========
def merge_wide_and_save(old_df: pd.DataFrame, new_df: pd.DataFrame, path: str):
    if old_df.empty and new_df.empty:
        merged = pd.DataFrame()
    elif old_df.empty:
        merged = new_df
    elif new_df.empty:
        merged = old_df
    else:
        # 对齐列并纵向拼接；同一天保留“新”的
        all_cols = sorted(set(old_df.columns) | set(new_df.columns))
        old_df = old_df.reindex(columns=all_cols)
        new_df = new_df.reindex(columns=all_cols)
        merged = pd.concat([old_df, new_df], axis=0)
        merged = merged[~merged.index.duplicated(keep='last')]

    if not merged.empty:
        merged = merged.sort_index()

    tmp = path + ".tmp"
    merged.to_csv(tmp)
    os.replace(tmp, path)
    print(f"✅ 保存：{path}（共 {len(merged)} 天）")
    return merged

# ========== 主函数：增量更新（或首跑全量） ==========
def get_total_mv(today: str):
    """
    将各指标写入：
      high.csv, close.csv, change.csv, open.csv, low.csv,
      vol_ratio.csv, turn_over.csv, vol.csv, amount.csv, total_mv.csv, st.csv
    规则：
    - 若文件存在：从历史最大日期的次日 → today 做增量
    - 若不存在：从 '2023-08-20' → today 全量
    """
    import tushare as ts
    pro = ts.pro_api('23469bd0e75228a4a7c650005d4589b5afa491fe1f379c2aaa27d23e')

    # 读取任意一个已有文件决定“增量起点”。优先 close.csv；都没有则全量。
    probe_path = os.path.join(SAVE_DIR, "close.csv")
    probe_df, probe_max = read_existing_wide_csv(probe_path)

    if probe_max is None:
        start_str = '2023-09-20'
        print(f"⚙️ 未发现历史数据，准备全量生成：{start_str} → {today}")
    else:
        start_dt = datetime.strptime(probe_max, "%Y-%m-%d") + timedelta(days=1)
        start_str = start_dt.strftime("%Y-%m-%d")
        print(f"🔄 增量更新：{start_str} → {today}")

    T = get_tradeday(start_str, today)
    if not T:
        print("🎉 无需更新（没有新交易日）。")
        return 0

    # 为每个指标准备增量列3
    highlist, closelist, changelist, openlist, lowlist = [], [], [], [], []
    vol_ratiolist, turn_overlist, vollist, amountlist, total_mvlist = [], [], [], [], []
    st_list = []

    # 拉取增量数据
    for t in T:
        t1 = datetime.strptime(t, "%Y-%m-%d")
        times = t1.strftime("%Y%m%d")

        # 抗抖动重试
        for retry in range(3):
            try:
                df2  = pro.bak_daily(trade_date=times)       # 含 high/close/change/open/low/vol_ratio/turn_over/vol/amount/total_mv
                stdf = pro.stock_st(trade_date=times)        # ST 标记
                break
            except Exception as e:
                wait = 1.2 * (retry + 1)
                print(f"⚠️ {t} 拉取失败：{e}；{wait:.1f}s 后重试...")
                time.sleep(wait)
        else:
            print(f"❌ {t} 连续失败，跳过。")
            continue

        if df2 is None or df2.empty:
            print(f"ℹ️ {t} bak_daily 为空，跳过。")
            continue

        # 宽表：一行=一天，列=ts_code
        idx = [t]
        highdf      = pd.DataFrame([df2.set_index("ts_code")["high"].to_dict()],       index=idx)
        closedf     = pd.DataFrame([df2.set_index("ts_code")["close"].to_dict()],      index=idx)
        changedf    = pd.DataFrame([df2.set_index("ts_code")["change"].to_dict()],     index=idx)
        opendf      = pd.DataFrame([df2.set_index("ts_code")["open"].to_dict()],       index=idx)
        lowdf       = pd.DataFrame([df2.set_index("ts_code")["low"].to_dict()],        index=idx)
        vol_ratiodf = pd.DataFrame([df2.set_index("ts_code")["vol_ratio"].to_dict()],  index=idx)
        turn_overdf = pd.DataFrame([df2.set_index("ts_code")["turn_over"].to_dict()],  index=idx)
        voldf       = pd.DataFrame([df2.set_index("ts_code")["vol"].to_dict()],        index=idx)
        amountdf    = pd.DataFrame([df2.set_index("ts_code")["amount"].to_dict()],     index=idx)
        total_mvdf  = pd.DataFrame([df2.set_index("ts_code")["total_mv"].to_dict()],   index=idx)

        # ST：有该 ts_code 则赋 1；否则列缺失（之后合并会自动对齐）
        if stdf is not None and not stdf.empty and 'ts_code' in stdf.columns:
            st_row = pd.DataFrame(data=[{c:1 for c in stdf['ts_code'].values}], index=idx)
        else:
            st_row = pd.DataFrame(index=idx)

        # 收集
        highlist.append(highdf);        closelist.append(closedf);      changelist.append(changedf)
        openlist.append(opendf);        lowlist.append(lowdf);          vol_ratiolist.append(vol_ratiodf)
        turn_overlist.append(turn_overdf);  vollist.append(voldf);     amountlist.append(amountdf)
        total_mvlist.append(total_mvdf);    st_list.append(st_row)

        # time.sleep(0.12)  # 轻微节流，可按需调整/去掉

    # 组装增量宽表（可能为空）
    def _cat(lst): return pd.concat(lst) if lst else pd.DataFrame()

    high_new      = _cat(highlist)
    close_new     = _cat(closelist)
    change_new    = _cat(changelist)
    open_new      = _cat(openlist)
    low_new       = _cat(lowlist)
    vol_ratio_new = _cat(vol_ratiolist)
    turn_over_new = _cat(turn_overlist)
    vol_new       = _cat(vollist)
    amount_new    = _cat(amountlist)
    total_mv_new  = _cat(total_mvlist)
    st_new        = _cat(st_list).fillna(0)

    # 逐个与历史合并后保存
    files = {
        "high.csv":       high_new,
        "close.csv":      close_new,
        "change.csv":     change_new,
        "open.csv":       open_new,
        "low.csv":        low_new,
        "vol_ratio.csv":  vol_ratio_new,
        "turn_over.csv":  turn_over_new,
        "vol.csv":        vol_new,
        "amount.csv":     amount_new,
        "total_mv.csv":   total_mv_new,
        "st.csv":         st_new,
    }

    for fname, new_df in files.items():
        path = os.path.join(SAVE_DIR, fname)
        old_df, _ = read_existing_wide_csv(path)
        merge_wide_and_save(old_df, new_df, path)

    return 0

# 运行
get_total_mv(today)
