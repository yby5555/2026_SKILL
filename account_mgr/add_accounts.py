"""
账号录入脚本 - 将 Google 账号信息写入 MongoDB

用法：
    修改下方 ACCOUNTS 列表后直接运行：
        python add_accounts.py
"""

import sys
from pathlib import Path

# 确保能找到同目录的 config.py 和 mongo_utils.py
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime, timezone
from config import STATUS_PENDING
from mongo_utils import create_mongo_client, get_collection

# ==================== 账号列表（在这里填写账号） ====================
ACCOUNTS = [
    # {"email": "xxx@gmail.com", "password": "密码", "totp_key": "2FA密钥（无则留空）"},
    # {"email": "EnnalscrAvey@gmail.com",      "password": "edtc6qn23",   "totp_key": "aoks5pv5ekncg6ycbau3l2f7i4pa36e4"},

    # {"email": "VieauTakemura@gmail.com",      "password": "wzslsxbvzdv", "totp_key": "gszqli4nnfgwjmdarhvjsq7q4o2cehix"},
    # {"email": "DanielwalterDresch@gmail.com", "password": "tl7zbhuwzut", "totp_key": "olfawz73mf3titwe3acewh6x3h4o4xxq"},
    # {"email": "KansecoKosme@gmail.com", "password": "1iirbyqciop","totp_key": "an6u2p22qryeu3qbzyt2xumor5tcwjdc"},
    # {"email": "LeitheiserFigueira@gmail.com", "password": "zsdsgqsqlgi","totp_key": "3pqpdx2e3hnhkn742hg3ybikzvmkqsey"},
    # {"email": "hardcoregamer1026@gmail.com", "password": "gV0Ry0ctG1Z6Gd","totp_key": "nhes7famqcjhlphhrfncxwf3qtx75vv4"},
    {"email": "s5524h24h723@mubanima26.sbs", "password": "*2lN3eW1"},
    # {"email": "s5524h24h772@mubanima26.sbs", "password": "!9U@jXKn"},

]
# =====================================================================


def main():
    if not ACCOUNTS:
        print("[WARN] ACCOUNTS 列表为空。")
        return

    client = create_mongo_client()
    col    = get_collection(client)
    print(f"[OK] 已连接 MongoDB\n")

    now = datetime.now(timezone.utc)
    inserted = updated = 0

    for acc in ACCOUNTS:
        email    = acc["email"].strip()
        password = acc["password"].strip()
        totp_key = acc.get("totp_key", "").strip()

        result = col.update_one(
            {"email": email},
            {
                "$setOnInsert": {
                    "created_at": now,
                    "ai_credits": None,
                    "status":     STATUS_PENDING,
                    "status_msg": "",
                },
                "$set": {
                    "password":   password,
                    "totp_key":   totp_key,
                    "updated_at": now,
                },
            },
            upsert=True,
        )

        if result.upserted_id:
            print(f"  [INSERT] {email}")
            inserted += 1
        else:
            print(f"  [UPDATE] {email}")
            updated += 1

    print(f"\n[DONE] 插入: {inserted} | 更新: {updated}")
    client.close()


if __name__ == "__main__":
    main()
