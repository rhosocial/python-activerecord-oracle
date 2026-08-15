"""
Oracle 数据库连接测试脚本

从环境变量读取连接参数，默认连接到本地 Oracle 实例：
- Oracle 11g XE: 端口 11520
- Oracle 18c XE: 端口 11521
- Oracle 21c XE: 端口 11522
- Oracle 23c Free: 端口 11523

环境变量: ORACLE_HOST, ORACLE_PORT, ORACLE_USER, ORACLE_PASSWORD, ORACLE_SERVICE
"""

import argparse
import os
import oracledb
import socket
import sys

HOST = os.environ.get("ORACLE_HOST", "localhost")

INSTANCES = [
    {
        "name": "Oracle 11g XE",
        "port": 11520,
        "service_names": ["XE", "XEPDB1"],
        "sid": "XE",
    },
    {
        "name": "Oracle 18c XE",
        "port": 11521,
        "service_names": ["XEPDB1", "XE"],
        "sid": "XE",
    },
    {
        "name": "Oracle 21c XE",
        "port": 11522,
        "service_names": ["XEPDB1", "XE"],
        "sid": "XE",
    },
    {
        "name": "Oracle 23c Free",
        "port": 11523,
        "service_names": ["FREEPDB1", "FREE"],
        "sid": "FREE",
    },
]


def check_port_open(host, port, timeout=5):
    """检查端口是否可达"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def diagnose_listener(instance):
    """诊断监听器状态"""
    port = instance["port"]
    name = instance["name"]

    # 尝试连接监听器并探测服务
    reachable_services = []
    unreachable_services = []

    for sn in instance["service_names"]:
        try:
            dsn = f"{HOST}:{port}/{sn}"
            conn = oracledb.connect(user="system", password="dummy", dsn=dsn)
            # 如果连 dummy 密码都能连上...不太可能
            conn.close()
            reachable_services.append(sn)
        except oracledb.Error as e:
            err = str(e)
            if "ORA-01017" in err or "ORA-28000" in err:
                # 凭证错误 = 服务存在，数据库已启动
                reachable_services.append(sn)
            else:
                unreachable_services.append(sn)

    for sid in [instance.get("sid", "")]:
        if not sid:
            continue
        try:
            dsn = oracledb.makedsn(HOST, port, sid=sid)
            conn = oracledb.connect(user="system", password="dummy", dsn=dsn)
            conn.close()
            reachable_services.append(f"SID:{sid}")
        except oracledb.Error as e:
            err = str(e)
            if "ORA-01017" in err or "ORA-28000" in err:
                reachable_services.append(f"SID:{sid}")
            else:
                unreachable_services.append(f"SID:{sid}")

    if reachable_services:
        print(f"  可达服务: {', '.join(reachable_services)}")
    if unreachable_services:
        print(f"  不可达服务: {', '.join(unreachable_services)}")

    return len(reachable_services) > 0


def test_connection(instance, password_override=None):
    """测试一个 Oracle 实例"""
    name = instance["name"]
    port = instance["port"]

    # 先检查端口
    if not check_port_open(HOST, port):
        print(f"[{name}] 端口 {port} 不可达")
        print(f"  可能原因: 容器未启动")
        return False

    print(f"[{name}] 端口 {port} 可达")

    # 诊断监听器
    has_services = diagnose_listener(instance)

    if not has_services:
        print(f"  诊断: 监听器可达但无服务注册")
        print(f"  可能原因: 数据库仍在启动中（Oracle 首次启动可能需要 10-30 分钟）")
        print(f"  建议: 等待几分钟后重新运行此脚本")
        return False

    # 尝试连接
    default_password = os.environ.get("ORACLE_PASSWORD", "Password1!")
    credentials = []
    if password_override:
        credentials = [
            {"user": "testuser", "password": password_override, "desc": "testuser"},
            {"user": "system", "password": password_override, "desc": "system"},
        ]
    else:
        credentials = [
            {"user": os.environ.get("ORACLE_USER", "testuser"), "password": default_password, "desc": "testuser"},
            {"user": "system", "password": default_password, "desc": "system"},
        ]

    for credential in credentials:
        user = credential["user"]
        password = credential["password"]

        for sn in instance["service_names"]:
            try:
                dsn = f"{HOST}:{port}/{sn}"
                mode = 0
                conn = oracledb.connect(user=user, password=password, dsn=dsn, mode=mode)
                cursor = conn.cursor()

                cursor.execute("SELECT BANNER FROM v$version WHERE ROWNUM = 1")
                row = cursor.fetchone()
                version = row[0] if row else "未知"

                print(f"  连接成功! (Service={sn}, 用户={user})")
                print(f"  版本: {version}")

                cursor.execute("SELECT USER, SYS_CONTEXT('USERENV','DB_NAME') FROM DUAL")
                row = cursor.fetchone()
                print(f"  当前用户: {row[0]}, 数据库: {row[1]}")

                cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
                tables = [row[0] for row in cursor.fetchall()]
                if tables:
                    print(f"  用户表: {', '.join(tables)}")
                else:
                    print(f"  暂无用户表")

                cursor.close()
                conn.close()
                return True
            except oracledb.Error as e:
                err = str(e)
                if "ORA-01017" in err:
                    continue  # 凭证无效，尝试下一个
                elif "ORA-12514" in err or "DPY-6001" in err:
                    continue  # 服务名未注册
                else:
                    print(f"  [{sn}, {user}]: {err[:100]}")

    print(f"  所有凭证均失败")
    print(f"  诊断: 监听器可达，服务已注册，但密码不正确")
    print(f"  注意: docker-compose 中 ORACLE_PASSWORD=password 不满足 Oracle 密码复杂度要求")
    print(f"        (需要 >=8 字符，含大小写字母和数字)")
    print(f"  建议: 使用 --password 参数指定正确密码，或重置容器密码")
    return False


def main():
    parser = argparse.ArgumentParser(description="Oracle 数据库连接测试")
    parser.add_argument("--password", help="指定密码（覆盖默认值）")
    args = parser.parse_args()

    print("=" * 60)
    print("Oracle 连接测试")
    print("=" * 60)
    print(f"oracledb 版本: {oracledb.__version__}")
    print(f"客户端模式: {'Thin' if oracledb.is_thin_mode() else 'Thick'}")
    print(f"目标主机: {HOST}")
    print()

    results = {}
    for instance in INSTANCES:
        results[instance["name"]] = test_connection(instance, password_override=args.password)
        print()

    print("=" * 60)
    print("测试结果汇总:")
    for name, success in results.items():
        status = "成功" if success else "失败"
        print(f"  {name}: {status}")
    print("=" * 60)

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
