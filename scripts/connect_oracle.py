#!/usr/bin/env python3
"""Oracle 容器连接测试脚本
用法: python scripts/connect_oracle.py [版本标签]
      版本标签可选: 11g, 18c, 21c, 23c (默认 21c)
"""

import sys
import oracledb

CONFIGS = {
    "11g": {
        "user": "testuser",
        "password": "Password1!",
        "dsn": "localhost:11520/XE",
        "note": "仅 thick mode 支持 11g",
    },
    "18c": {
        "user": "testuser",
        "password": "Password1!",
        "dsn": "localhost:11521/XEPDB1",
    },
    "21c": {
        "user": "testuser",
        "password": "Password1!",
        "dsn": "localhost:11522/XEPDB1",
    },
    "23c": {
        "user": "testuser",
        "password": "Password1!",
        "dsn": "localhost:11523/FREEPDB1",
    },
}


def main():
    label = sys.argv[1] if len(sys.argv) > 1 else "21c"
    if label not in CONFIGS:
        print(f"未知版本: {label}，可用: {list(CONFIGS.keys())}")
        sys.exit(1)

    cfg = CONFIGS[label]
    note = cfg.pop("note", None)
    print(f"连接 Oracle {label} ({cfg['dsn']})...")
    if note:
        print(f"提示: {note}")

    try:
        conn = oracledb.connect(**cfg)
    except oracledb.DatabaseError as e:
        err_str = str(e)
        if "DPY-3010" in err_str:
            print(f"连接失败: oracledb thin mode 不支持 Oracle {label}")
            print("  请安装 Oracle Instant Client 并使用 thick mode:")
            print("    oracledb.init_oracle_client()")
        else:
            print(f"连接失败: {e}")
        sys.exit(1)

    with conn.cursor() as cur:
        cur.execute("SELECT BANNER FROM V$VERSION")
        rows = cur.fetchall()
        print("连接成功!")
        for row in rows:
            print(f"  {row[0]}")
    conn.close()


if __name__ == "__main__":
    main()