from ingest import ingest_all
from database import QuantDatabase
from config import DATA_ROOT, DB_PATH, FULL_REBUILD, INGEST_ON_START


def main():
    print("DATA_ROOT:", DATA_ROOT)
    print("DB_PATH  :", DB_PATH)
    print("FULL_REBUILD:", FULL_REBUILD)
    print("INGEST_ON_START:", INGEST_ON_START)

    if INGEST_ON_START:
        ingest_all()

    # 正常读取 / 回测逻辑
    db = QuantDatabase(read_only=True)

    # 示例：列出库里前几个标的
    print("symbols head:")
    print(db.list_symbols().head())

    # 示例：读取一小段行
    df = db.load_quotes("000001.SZ", "2025-01-02", "2025-01-02")
    print(df.head())

    db.close()


if __name__ == "__main__":
    main()
