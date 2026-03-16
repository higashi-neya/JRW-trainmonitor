"""
JR西日本 関西エリア 異常列車監視スクリプト（多路線対応版）
===========================================================
・複数路線を1サービスで並列監視
・稼働時間を環境変数で制限（デフォルト6時〜22時）
・路線ごとに combos/{line}.json でホワイトリスト管理
・3分ごとに監視、異常検知後は9分ごとに再通知
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))
from pathlib import Path

import requests

# ────────────────────────────────────────────────
# ログ設定
# ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# 環境変数
# ────────────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

def get_webhook_url(line: str) -> str:
    """路線ごとのWebhook URLを取得。個別設定がなければ共通URLを使用。
    例）kobesanyo → 環境変数 WEBHOOK_kobesanyo があればそれを使用
    """
    key = f"WEBHOOK_{line.upper()}"
    return os.environ.get(key, DISCORD_WEBHOOK_URL)
POLL_MIN            = int(os.environ.get("POLL_MIN",      "3"))
RENOTIFY_MIN        = int(os.environ.get("RENOTIFY_MIN",  "9"))
ACTIVE_HOUR_START   = int(os.environ.get("ACTIVE_HOUR_START", "6"))   # 稼働開始時刻
ACTIVE_HOUR_END     = int(os.environ.get("ACTIVE_HOUR_END",  "22"))   # 稼働終了時刻

# 監視する路線（カンマ区切りで環境変数から、なければ全路線）
_DEFAULT_LINES = ",".join([
    "kobesanyo", "kyoto", "hokurikubiwako", "osakaloop",
    "hanwahagoromo", "yamatoji", "ako", "kosei", "nara",
    "sagano", "sanin1", "sanin2", "osakahigashi", "takarazuka",
    "fukuchiyama", "tozai", "gakkentoshi", "bantan", "maizuru",
    "yumesaki", "kansaiairport",
    "hokuriku", "kusatsu", "wakayama1", "wakayama2",
    "manyomahoroba", "kansai", "kinokuni",
])
LINES = [l.strip() for l in os.environ.get("LINES", _DEFAULT_LINES).split(",") if l.strip()]

API_BASE    = "https://www.train-guide.westjr.co.jp/api/v3"
COMBOS_DIR  = Path(__file__).parent / "combos"
CACHE_DIR   = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)




# ────────────────────────────────────────────────
# データ構造
# ────────────────────────────────────────────────
@dataclass
class DetectedEntry:
    first_detected: datetime
    last_notified:  datetime
    notify_count:   int = 0
    last_prev:      str = ""
    last_next:      str = ""


# ────────────────────────────────────────────────
# キャッシュ永続化
# ────────────────────────────────────────────────
def cache_path(line: str) -> Path:
    return CACHE_DIR / f"{line}.json"


def save_cache(line: str, cache: dict):
    """キャッシュをJSONファイルに保存"""
    try:
        data = {}
        for key, entry in cache.items():
            data[key] = {
                "first_detected": entry.first_detected.isoformat(),
                "last_notified":  entry.last_notified.isoformat(),
                "notify_count":   entry.notify_count,
                "last_prev":      entry.last_prev,
                "last_next":      entry.last_next,
            }
        cache_path(line).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.error(f"[{line}] キャッシュ保存失敗: {e}")


def load_cache(line: str) -> dict:
    """JSONファイルからキャッシュを読み込み"""
    path = cache_path(line)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cache = {}
        for key, v in data.items():
            cache[key] = DetectedEntry(
                first_detected = datetime.fromisoformat(v["first_detected"]),
                last_notified  = datetime.fromisoformat(v["last_notified"]),
                notify_count   = v["notify_count"],
                last_prev      = v["last_prev"],
                last_next      = v["last_next"],
            )
        log.info(f"[{line}] キャッシュ読み込み: {len(cache)}件")
        return cache
    except Exception as e:
        log.error(f"[{line}] キャッシュ読み込み失敗（空で起動）: {e}")
        return {}


# ────────────────────────────────────────────────
# 稼働時間チェック
# ────────────────────────────────────────────────
def is_active_hour() -> bool:
    now = datetime.now(JST)
    return ACTIVE_HOUR_START <= now.hour < ACTIVE_HOUR_END


def seconds_until_active() -> int:
    """次の稼働開始まで何秒か"""
    now = datetime.now(JST)
    start = now.replace(hour=ACTIVE_HOUR_START, minute=0, second=0, microsecond=0)
    if now >= start:
        # 翌日の開始まで
        from datetime import timedelta
        start += timedelta(days=1)
    return int((start - now).total_seconds())


# ────────────────────────────────────────────────
# combos/{line}.json 読み込み
# ────────────────────────────────────────────────
def load_combos(line: str) -> tuple[set, set, set, set]:
    """
    Returns:
        strict        : {(type, dest, cars), ...}  両数指定あり
        loose         : {(type, dest), ...}         両数問わず許可
        u_alert       : {(type, dest), ...}         う列番のみ検知
        wildcard_types: {type, ...}                 種別まるごとスルー（両数・行先問わず）
    """
    path = COMBOS_DIR / f"{line}.json"
    if not path.exists():
        log.warning(f"[{line}] combos/{line}.json が見つかりません。全列車を検知対象にします。")
        return set(), set(), set(), set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.error(f"[{line}] combos読み込み失敗: {e}")
        return set(), set(), set(), set()

    strict, loose, u_alert, wildcard_types = set(), set(), set(), set()
    for item in data:
        t = str(item.get("type", "")).strip()
        d = str(item.get("dest", "")).strip()
        c = item.get("cars")
        u = bool(item.get("u_alert"))
        w = bool(item.get("wildcard"))  # 種別まるごとスルー

        if not t:
            continue

        # wildcard: true なら種別単位で全スルー（dest不要）
        if w:
            wildcard_types.add(t)
            continue

        if not d:
            continue
        if u:
            u_alert.add((t, d))
        if c is None:
            loose.add((t, d))
        else:
            strict.add((t, d, int(c)))

    log.debug(
        f"[{line}] combos: strict={len(strict)} loose={len(loose)} "
        f"u_alert={len(u_alert)} wildcard={len(wildcard_types)}"
    )
    return strict, loose, u_alert, wildcard_types


# ────────────────────────────────────────────────
# 判定
# ────────────────────────────────────────────────
def is_normal(train: dict, strict: set, loose: set, u_alert: set, wildcard_types: set) -> bool:
    """
    判定ロジック：
    ① wildcard_types に種別が入っていれば両数・行先問わず全スルー
    ② u_alert エントリはう列番のみ検知
    ③ loose   エントリは両数問わずスルー
    ④ strict  エントリは (種別, 行先, 両数) 3点一致でスルー
    ⑤ それ以外は検知
    """
    t, d, c = train["type"], train["dest"], train["cars"]
    is_u    = train["no"].startswith("う")

    # ① 種別まるごとスルー（快速・普通・特急など両数チェック不要な種別）
    if t in wildcard_types:
        return True

    # ② u_alert：う列番なら検知、通常列番はスルー
    if (t, d) in u_alert:
        return not is_u

    # ③ 両数問わずスルー
    if (t, d) in loose:
        return True

    # ④ 両数指定あり：3点一致でスルー（新快速など両数チェックあり）
    if c is not None and (t, d, c) in strict:
        return True

    return False


# ────────────────────────────────────────────────
# API
# ────────────────────────────────────────────────
def fetch_json(url: str) -> dict | None:
    try:
        r = requests.get(url, timeout=10,
                         headers={"Referer": "https://www.train-guide.westjr.co.jp/"})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"API取得失敗 ({url}): {e}")
        return None


def fetch_station_map(line: str) -> dict[str, str]:
    data = fetch_json(f"{API_BASE}/{line}_st.json")
    if not data:
        return {}
    st_map = {}
    for st in data.get("stations", []):
        info = st.get("info", {})
        code = str(info.get("code", "")).zfill(4)
        if code:
            st_map[code] = info.get("name", "不明")
    return st_map


def extract_text(val) -> str:
    """APIの値が {'text': '姫路', ...} 形式でも文字列でも名称を返す"""
    if isinstance(val, dict):
        return str(val.get("text", "不明")).strip()
    return str(val).strip() if val else "不明"


def extract_station_name(code: str, st_map: dict) -> str:
    """
    駅コードから駅名を返す。
    "0435_0462" → "0435" 部分で照合
    "0416_####" → "0416" 部分で照合（####は終点付近を意味する）
    """
    base = code.split("_")[0].zfill(4)
    return st_map.get(base, "不明")


def parse_trains(data: dict, st_map: dict) -> list[dict]:
    trains = []
    for t in data.get("trains", []):
        no = str(t.get("no", "不明"))

        # 行先：{"text": "姫路", "code": "...", "line": "..."} 形式
        dest = extract_text(t.get("dest", "不明"))

        # 種別：displayType をそのまま使用（"新快速"/"普通" など）
        type_name = str(t.get("displayType", "") or "不明").strip()

        # 走行位置："0435_0462"（前駅_次駅）or "0416_####"（終点付近）
        pos_str = str(t.get("pos", ""))
        if "_" in pos_str:
            parts     = pos_str.split("_")
            prev_code = parts[0].zfill(4)
            next_code = parts[1] if len(parts) > 1 else ""
        elif "." in pos_str:
            # 旧形式フォールバック
            parts     = pos_str.split(".")
            prev_code = parts[0].zfill(4)
            next_code = parts[1].zfill(4) if len(parts) > 1 else ""
        else:
            prev_code = pos_str.zfill(4)
            next_code = ""

        prev_name = extract_station_name(prev_code, st_map) if prev_code else "不明"
        # "####" は停車中を示すコード。stopTimeが入っていれば停車中と確定
        is_stopped = bool(t.get("stopTime", ""))
        if next_code and next_code != "####":
            next_name = extract_station_name(next_code, st_map)
        else:
            next_name = "停車中" if is_stopped else "走行中（終点付近）"

        # 両数：numberOfCars が正式フィールド
        cars_raw = (t.get("numberOfCars") or t.get("cars") or t.get("carNum")
                    or (len(t["carInfo"]) if t.get("carInfo") else None))
        try:
            cars = int(cars_raw) if cars_raw is not None else None
        except (ValueError, TypeError):
            cars = None

        # 列車名：種別に「特急」が含まれる場合のみ取得
        nickname = ""
        if "特急" in type_name:
            nickname = str(t.get("nickname", "") or "").strip()

        # 遅延分数
        try:
            delay = int(t.get("delayMinutes", 0) or 0)
        except (ValueError, TypeError):
            delay = 0

        trains.append({"no": no, "type": type_name, "dest": dest, "nickname": nickname,
                        "prev": prev_name, "next": next_name, "cars": cars, "delay": delay})
    return trains


# ────────────────────────────────────────────────
# Discord 通知
# ────────────────────────────────────────────────
def notify_discord(line: str, line_label: str, train: dict,
                   is_renotify: bool = False, notify_count: int = 1,
                   same_position: bool = False):
    webhook_url = get_webhook_url(line)
    if not webhook_url:
        log.warning(f"[{line}] Webhook URL未設定のため通知スキップ")
        return

    cars_line = f"🚋 両数：{train['cars']}両\n" if train["cars"] is not None else ""
    pos_alert = "\n🚨 前回通知時と同じ位置です（停車中または遅延の可能性）" if same_position else ""

    nickname_line = f"🚅 列車名：**{train['nickname']}**\n" if train.get("nickname") else ""
    delay = train.get("delay", 0)
    if delay >= 60:
        delay_line = "⏳ 遅れ：60分以上\n"
    elif delay > 0:
        delay_line = f"⏳ 遅れ：{delay}分\n"
    else:
        delay_line = "✅ 定刻\n"

    if is_renotify:
        header = f"🔁 **所定外列車･代走が引き続き走行中です**\n（{notify_count}回目の通知）"
    else:
        header = "⚠️ **所定外列車･代走を検知しました**"

    message = (
        f"{header}\n"
        f"🛤️ 路線：{line_label}\n"
        f"🚃 列車番号：`{train['no']}`\n"
        f"🏷️ 種別：**{train['type']}**\n"
        f"{nickname_line}"
        f"🎯 行先：**{train['dest']}**\n"
        f"{cars_line}"
        f"{delay_line}"
        f"📍 現在地：{train['prev']} ➡️ {train['next']}{pos_alert}\n"
        f"🕐 {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}（JST）\n"
        f"\u200b"
    )
    try:
        r = requests.post(webhook_url, json={"content": message}, timeout=10)
        r.raise_for_status()
        label = "再通知" if is_renotify else "初回通知"
        cars_disp = f"{train['cars']}両" if train["cars"] is not None else "両数不明"
        log.info(f"[{line}] Discord {label}: {train['no']} {train['type']} {train['dest']} {cars_disp}")
    except Exception as e:
        log.error(f"[{line}] Discord通知失敗: {e}")


# ────────────────────────────────────────────────
# 路線ごとの状態管理
# ────────────────────────────────────────────────
LINE_LABELS = {
    "kobesanyo":     "JR神戸線･山陽本線",
    "kyoto":         "JR京都線",
    "hokurikubiwako":"琵琶湖線・北陸線",
    "osakaloop":     "大阪環状線",
    "hanwahagoromo": "阪和線・羽衣線",
    "yamatoji":      "大和路線",
    "ako":           "赤穂線",
    "kosei":         "湖西線",
    "nara":          "奈良線",
    "sagano":        "嵯峨野線",
    "sanin1":        "山陰線（園部〜福知山）",
    "sanin2":        "山陰線（福知山〜居組）",
    "osakahigashi":  "おおさか東線",
    "takarazuka":    "福知山線",
    "fukuchiyama":   "JR宝塚線・福知山線",
    "tozai":         "JR東西線･片町線",
    "gakkentoshi":   "JR東西線･片町線",
    "bantan":        "播但線",
    "maizuru":       "舞鶴線",
    "yumesaki":      "JRゆめ咲線",
    "kansaiairport": "関西空港線",
    "hokuriku":      "北陸線",
    "kusatsu":       "草津線",
    "wakayama1":     "和歌山線（王寺〜五条）",
    "wakayama2":     "和歌山線（五条〜和歌山）",
    "manyomahoroba": "万葉まほろば線",
    "kansai":        "関西本線（加茂〜亀山）",
    "kinokuni":      "きのくに線",
}


def poll_line(line: str, st_map: dict, cache: dict) -> dict:
    """1路線分のポーリング処理。更新されたst_mapを返す"""
    label = LINE_LABELS.get(line, line)

    # 駅情報（初回のみ取得）
    if not st_map:
        st_map = fetch_station_map(line)
        log.info(f"[{line}] 駅情報 {len(st_map)} 件取得")

    strict, loose, u_alert, wildcard_types = load_combos(line)

    data = fetch_json(f"{API_BASE}/{line}.json")
    if data is None:
        return st_map

    trains      = parse_trains(data, st_map)
    now         = datetime.now(JST)
    active_keys = set()

    for train in trains:
        # 列車番号のみでキャッシュ識別（両数変動による誤連投を防ぐ）
        cache_key = train["no"]
        active_keys.add(cache_key)

        if is_normal(train, strict, loose, u_alert, wildcard_types):
            continue

        if cache_key not in cache:
            cache[cache_key] = DetectedEntry(
                first_detected=now, last_notified=now, notify_count=1,
                last_prev=train["prev"], last_next=train["next"],
            )
            log.warning(f"[{line}] 初回検知: {train['no']} {train['type']} {train['dest']} {train['prev']}→{train['next']}")
            notify_discord(line, label, train, is_renotify=False, notify_count=1)

        else:
            entry   = cache[cache_key]
            elapsed = (now - entry.last_notified).total_seconds()
            if elapsed >= RENOTIFY_MIN * 60:
                same_pos = (train["prev"] == entry.last_prev and train["next"] == entry.last_next)
                entry.last_notified = now
                entry.notify_count += 1
                entry.last_prev = train["prev"]
                entry.last_next = train["next"]
                log.warning(f"[{line}] 再通知 {entry.notify_count}回目{'(同一位置)' if same_pos else ''}: {train['no']}")
                notify_discord(line, label, train, is_renotify=True,
                               notify_count=entry.notify_count, same_position=same_pos)

    # 走行終了した列車をキャッシュから削除
    for k in set(cache.keys()) - active_keys:
        log.info(f"[{line}] 走行終了・キャッシュ削除: {k}")
        del cache[k]

    # キャッシュをファイルに保存
    save_cache(line, cache)

    return st_map


# ────────────────────────────────────────────────
# メインループ
# ────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("JR西日本 関西エリア 異常列車監視 起動")
    log.info(f"監視路線: {len(LINES)} 路線")
    log.info(f"稼働時間: {ACTIVE_HOUR_START}時〜{ACTIVE_HOUR_END}時")
    log.info(f"監視間隔: {POLL_MIN}分 | 再通知: {RENOTIFY_MIN}分")
    log.info(f"Discord通知: {'有効' if DISCORD_WEBHOOK_URL else '無効（URL未設定）'}")
    log.info("=" * 60)

    # 路線ごとのキャッシュ・駅情報を保持（ファイルから復元）
    st_maps: dict[str, dict] = {line: {} for line in LINES}
    caches:  dict[str, dict] = {line: load_cache(line) for line in LINES}
    loop_count = 0

    while True:

        # 稼働時間外なら待機
        if not is_active_hour():
            wait_sec = seconds_until_active()
            wait_min = wait_sec // 60
            log.info(f"稼働時間外（{ACTIVE_HOUR_START}時〜{ACTIVE_HOUR_END}時）。{wait_min}分後に再開します。")
            # キャッシュをリセット（翌朝は新鮮な状態で開始）
            for line in LINES:
                caches[line].clear()
                save_cache(line, {})
            time.sleep(wait_sec)
            continue

        loop_count += 1
        log.info(f"━━━ ポーリング #{loop_count} ━━━")

        for line in LINES:
            try:
                st_maps[line] = poll_line(line, st_maps[line], caches[line])
            except Exception as e:
                log.error(f"[{line}] 予期しないエラー: {e}")

        log.info(f"全路線チェック完了。{POLL_MIN}分後に次回ポーリング。")
        time.sleep(POLL_MIN * 60)


if __name__ == "__main__":
    main()
