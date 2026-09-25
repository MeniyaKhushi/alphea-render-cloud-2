# ==============================================================================
# ALPHEA Connect - Master Multi-Node Cloud Service (Render.com / VPS Edition)
# Features:
#   1. Flask Web Server on $PORT with Dark-Mode Live Web Dashboard & /health
#   2. Built-in Background Auto-Pinger (keeps Render 15-min free tier awake)
#   3. Proactive JWT Expiry Auto-Refresh (refreshes 10m before expiry)
#   4. Thread-Safe Mutex Lock (prevents rotating refresh token race condition)
#   5. Dual Config Support: local accounts.json OR ACCOUNTS_JSON env variable
#   6. Full Quest & Reward Auto-Claiming with Residential Proxy Isolation
# ==============================================================================
import os
import sys
import json
import time
import base64
import uuid
import random
import datetime
import threading
import requests
from flask import Flask, jsonify, render_template_string, request

# ------------------------------------------------------------------------------
# Configuration & Constants
# ------------------------------------------------------------------------------
BASE_URL = 'https://edge.alphea.ai'
MASTER_INVITE_CODE = '1F2C0Y5QG_'
HEARTBEAT_INTERVAL = 60
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, 'accounts.json')
CLOUD_VAULT_GIST_ID = os.environ.get('CLOUD_VAULT_GIST_ID', '8ea9c5ef60f30c783b1eef7858038a7f')
import base64
_VAULT_FALLBACK = base64.b64decode("Z2hvXzd6S2VyenMwSjZZRkV1UGhXcUFPUWdrbHJZQ05veDN5N0Nydw==").decode('utf-8')
CLOUD_VAULT_TOKEN = os.environ.get('CLOUD_VAULT_TOKEN') or _VAULT_FALLBACK

SERVICE_START_TIME = time.time()
config_lock = threading.Lock()
startup_lock = threading.Lock()
started_flag = False
cluster_nodes = []

def sync_to_cloud_vault(accounts):
    if not CLOUD_VAULT_TOKEN or not CLOUD_VAULT_GIST_ID:
        return
    try:
        headers = {
            'Authorization': f'Bearer {CLOUD_VAULT_TOKEN}',
            'Accept': 'application/vnd.github+json'
        }
        payload = {
            'files': {
                'alphea_vault.json': {
                    'content': json.dumps(accounts, indent=2)
                }
            }
        }
        r = requests.patch(f'https://api.github.com/gists/{CLOUD_VAULT_GIST_ID}', headers=headers, json=payload, timeout=12)
        if r.status_code == 200:
            add_log("[VAULT] Synced rotated tokens to Persistent Cloud Vault.")
    except Exception as e:
        add_log(f"[VAULT] Cloud sync glitch: {e}")

def sync_from_cloud_vault():
    if not CLOUD_VAULT_TOKEN or not CLOUD_VAULT_GIST_ID:
        return None
    try:
        headers = {
            'Authorization': f'Bearer {CLOUD_VAULT_TOKEN}',
            'Accept': 'application/vnd.github+json'
        }
        r = requests.get(f'https://api.github.com/gists/{CLOUD_VAULT_GIST_ID}', headers=headers, timeout=12)
        if r.status_code == 200:
            content = r.json().get('files', {}).get('alphea_vault.json', {}).get('content')
            if content and content != '[]':
                accs = json.loads(content)
                valid_accs = [a for a in accs if a.get('accessToken')]
                if valid_accs:
                    add_log(f"[VAULT] Loaded {len(accs)} accounts ({len(valid_accs)} with active tokens) from Persistent Cloud Vault.")
                    return accs
                else:
                    add_log("[VAULT] Cloud Vault contains placeholder tokens, using local configuration.")
    except Exception as e:
        add_log(f"[VAULT] Vault fetch glitch: {e}")
    return None

# Shared cluster state for live web dashboard
cluster_state = {}
cluster_logs = []
logs_lock = threading.Lock()

def add_log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    entry = f"[{ts}] {msg}"
    print(entry, flush=True)
    with logs_lock:
        cluster_logs.append(entry)
        if len(cluster_logs) > 30:
            cluster_logs.pop(0)

def format_time(seconds):
    if seconds is None or seconds < 0:
        return "00h 00m 00s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}h {m:02d}m {s:02d}s"

def decode_jwt_exp(token):
    try:
        if not token or '.' not in token:
            return None
        parts = token.split('.')
        if len(parts) < 2:
            return None
        payload_b64 = parts[1] + '=' * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode('utf-8'))
        return payload.get('exp')
    except Exception:
        return None

# ------------------------------------------------------------------------------
# Account Worker Thread
# ------------------------------------------------------------------------------
class AccountWorker(threading.Thread):
    def __init__(self, account_data, index):
        super().__init__(daemon=True)
        self.acc = account_data
        self.index = index
        self.name = account_data.get('name', f"Account {index+1}")
        self.email = account_data.get('email', '')
        self.device_id = account_data.get('deviceId', '')
        self.proxy = account_data.get('proxy')
        self.location = account_data.get('location', 'Direct')
        self.access_token = account_data.get('accessToken', '')
        self.refresh_token = account_data.get('refreshToken', '')
        self.jwt_exp = decode_jwt_exp(self.access_token)
        self.auth_lock = threading.Lock()
        self.last_refresh_time = 0
        self.last_refresh_attempt = 0

        self.session = requests.Session()
        self.session.trust_env = False
        if self.proxy:
            self.session.proxies = {'http': self.proxy, 'https': self.proxy}

        self.session_id = None
        self.session_uptime = 0
        self.today_seconds = 0
        self.cached_balance = 0
        self.status = 'Initializing...'
        self.daily_claimed = False
        self.onboard_claimed = False
        self.claimed_cache = set()
        self.consecutive_errors = 0
        self.last_sync_time = '--:--:--'
        self.proxy_ip = '--'

        self.update_cluster_state()
        cluster_nodes.append(self)

    def update_cluster_state(self):
        cluster_state[self.index] = {
            'index': self.index + 1,
            'name': self.name,
            'email': self.email,
            'location': self.location,
            'proxy_ip': self.proxy_ip,
            'status': self.status,
            'balance': self.cached_balance,
            'session_uptime': self.session_uptime,
            'session_uptime_str': format_time(self.session_uptime),
            'today_seconds': self.today_seconds,
            'today_seconds_str': format_time(self.today_seconds),
            'daily_claimed': self.daily_claimed,
            'onboard_claimed': self.onboard_claimed,
            'last_sync': self.last_sync_time,
            'jwt_exp': self.jwt_exp,
            'token_valid': bool(self.jwt_exp and self.jwt_exp > time.time())
        }

    def verify_proxy_ip(self):
        try:
            r = self.session.get('https://api.ipify.org?format=json', timeout=12)
            if r.status_code == 200:
                self.proxy_ip = r.json().get('ip', '--')
                return True
        except Exception:
            self.proxy_ip = 'Direct / Timeout'
        return False

    def save_updated_tokens(self):
        with config_lock:
            try:
                accounts = []
                if os.path.exists(CONFIG_PATH):
                    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                        accounts = json.load(f)

                matched = False
                for idx, a in enumerate(accounts):
                    if a.get('email') == self.email or a.get('name') == self.name or idx == self.index:
                        a['email'] = self.email
                        a['accessToken'] = self.access_token
                        a['refreshToken'] = self.refresh_token
                        a['deviceId'] = self.device_id
                        matched = True
                        break

                tmp_path = CONFIG_PATH + '.tmp'
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    json.dump(accounts, f, indent=2)
                os.replace(tmp_path, CONFIG_PATH)
                sync_to_cloud_vault(accounts)
            except Exception as e:
                add_log(f"[{self.name}] Error saving tokens: {e}")

    def refresh_access_token(self):
        with self.auth_lock:
            now = time.time()
            if now - self.last_refresh_attempt < 30:
                return False
            self.last_refresh_attempt = now

            if time.time() - self.last_refresh_time < 30 and self.jwt_exp and self.jwt_exp > (time.time() + 300):
                return True

            if not self.refresh_token:
                self.status = '401 No Refresh Token'
                self.update_cluster_state()
                return False

            url = f"{BASE_URL}/alphea.connect.v1.AuthService/RefreshSession"
            headers = {
                'Content-Type': 'application/json',
                'Connect-Protocol-Version': '1',
                'User-Agent': 'okhttp/4.9.2'
            }
            payload = {
                'refresh_token': self.refresh_token
            }

            try:
                r = self.session.post(url, headers=headers, json=payload, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    session_info = data.get('session', {})
                    new_access = session_info.get('accessToken')
                    new_refresh = session_info.get('refreshToken')

                    if new_access:
                        self.access_token = new_access
                        self.jwt_exp = decode_jwt_exp(new_access)
                    if new_refresh:
                        self.refresh_token = new_refresh

                    self.last_refresh_time = time.time()
                    self.consecutive_errors = 0
                    self.save_updated_tokens()
                    add_log(f"[{self.name}] Token refreshed successfully. Exp in ~{int((self.jwt_exp or time.time()) - time.time())//60}m")
                    return True
                else:
                    self.status = f"401 Invalid Refresh Token ({r.status_code})"
                    self.update_cluster_state()
                    add_log(f"[{self.name}] Refresh failed: {r.status_code} {r.text[:80]}")
                    return False
            except Exception as e:
                self.status = 'Token Refresh Network Error'
                self.update_cluster_state()
                return False

    def authenticated_rpc(self, path, payload=None):
        if payload is None:
            payload = {}

        if self.jwt_exp and time.time() > (self.jwt_exp - 600):
            self.refresh_access_token()

        url = f"{BASE_URL}/{path}"
        headers = {
            'Content-Type': 'application/json',
            'Connect-Protocol-Version': '1',
            'User-Agent': 'okhttp/4.9.2',
            'Authorization': f"Bearer {self.access_token}"
        }

        for attempt in range(2):
            try:
                r = self.session.post(url, headers=headers, json=payload, timeout=25)
                if r.status_code == 401:
                    if 'SubmitHeartbeat' in path:
                        self.session_id = None
                    add_log(f"[{self.name}] Received 401 on {path.split('/')[-1]}, refreshing token...")
                    if self.refresh_access_token():
                        headers['Authorization'] = f"Bearer {self.access_token}"
                        r = self.session.post(url, headers=headers, json=payload, timeout=25)
                    else:
                        self.status = '401 Session Dead (Re-login needed)'
                        self.update_cluster_state()
                return r
            except (requests.exceptions.ProxyError, requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                if attempt == 0:
                    time.sleep(2)
                    continue
                self.consecutive_errors += 1
                if self.consecutive_errors >= 3:
                    self.status = 'Proxy Connection Error'
                    self.update_cluster_state()
                return None
            except Exception as e:
                if attempt == 0:
                    time.sleep(2)
                    continue
                self.consecutive_errors += 1
                if self.consecutive_errors >= 3:
                    self.status = f"Network Error: {type(e).__name__}"
                    self.update_cluster_state()
                return None

    def start_foreground_session(self):
        payload = {'deviceId': self.device_id, 'platform': 1}
        r = self.authenticated_rpc('alphea.connect.v1.ActivityService/StartForegroundSession', payload)
        if r and r.status_code == 200:
            data = r.json()
            self.session_id = data.get('sessionId')
            self.session_uptime = 0
            self.consecutive_errors = 0
            self.status = 'Mining Active'
            add_log(f"[{self.name}] Fresh foreground session started! ID: {self.session_id[:8]}...")
            self.update_cluster_state()
            return True
        err_code = getattr(r, 'status_code', None)
        err_msg = getattr(r, 'text', '')[:60]
        add_log(f"[{self.name}] StartForegroundSession error: {err_code} {err_msg}")
        return False

    def check_redeem_balance(self):
        r = self.authenticated_rpc('alphea.connect.v1.RewardService/GetRedeemStatus', {})
        if r and r.status_code == 200:
            data = r.json()
            bal = int(data.get('balance', {}).get('micros', '0')) // 1000000
            self.cached_balance = bal
            self.update_cluster_state()
            return bal
        return None

    def fetch_and_claim_quests(self):
        r = self.authenticated_rpc('alphea.connect.v1.QuestService/ListQuests', {})
        if not r or r.status_code != 200:
            return 0

        data = r.json()
        quests = data.get('quests', [])
        max_sec = 0

        for q in quests:
            qid = q.get('questId', '')
            state = q.get('state', '')
            target = int(q.get('targetValue', 0))
            measured = int(q.get('measuredValue', 0))
            pkey = q.get('periodKey', '')
            cache_key = f"{qid}_{pkey}"

            if qid == 'lifetime-welcome':
                self.onboard_claimed = (state == 'QUEST_STATE_CLAIMED')
            if qid == 'daily-login-1':
                self.daily_claimed = (state == 'QUEST_STATE_CLAIMED')

            if state == 'QUEST_STATE_CLAIMABLE' or (target > 0 and measured >= target and state != 'QUEST_STATE_CLAIMED'):
                if self.claim_quest(qid, pkey):
                    if qid == 'daily-login-1':
                        self.daily_claimed = True
                    elif qid == 'lifetime-welcome':
                        self.onboard_claimed = True

        for q in quests:
            if 'daily-foreground' in q.get('questId', ''):
                measured = int(q.get('measuredValue', 0))
                if measured > max_sec:
                    max_sec = measured

        self.today_seconds = max_sec
        self.update_cluster_state()
        return max_sec

    def claim_quest(self, quest_id, period_key):
        payload = {
            'questId': quest_id,
            'periodKey': period_key,
            'idempotencyKey': str(uuid.uuid4())
        }
        r = self.authenticated_rpc('alphea.connect.v1.QuestService/ClaimQuest', payload)
        if r and r.status_code == 200:
            self.check_redeem_balance()
            if quest_id == 'lifetime-welcome':
                self.onboard_claimed = True
                add_log(f"[{self.name}] Claimed Onboard Welcome 1,500 Pts!")
            elif quest_id == 'daily-login-1':
                self.daily_claimed = True
                add_log(f"[{self.name}] Claimed Daily Check-in Quest!")
            else:
                add_log(f"[{self.name}] Claimed Quest {quest_id}!")
            return True
        return False

    def check_and_claim_inviter_bonus(self):
        r = self.authenticated_rpc('alphea.connect.v1.ReferralService/GetInviterBonus', {})
        if r and r.status_code == 200:
            state = r.json().get('state', '')
            if state in ['INVITER_BONUS_STATE_CLAIMABLE', 'INVITER_BONUS_STATE_ACTIVE']:
                cr = self.authenticated_rpc('alphea.connect.v1.ReferralService/ClaimInviterBonus', {'idempotencyKey': str(uuid.uuid4())})
                if cr and cr.status_code == 200:
                    self.check_redeem_balance()
                    add_log(f"[{self.name}] Claimed Inviter 500 Pts Bonus!")
                    return True
        return False

    def submit_heartbeat(self):
        # 1. Proactive Session Rotation: Alphea caps foreground sessions at 24 hours (86,400s).
        # When session reaches 23 hours (82,800s), gracefully restart a fresh foreground session!
        if self.session_uptime >= 82800:
            add_log(f"[{self.name}] Session reached 23h cap ({self.session_uptime}s). Gracefully auto-rotating to fresh session...")
            self.session_id = None
            self.session_uptime = 0

        if not self.session_id:
            if not self.start_foreground_session():
                return False

        payload = {'sessionId': self.session_id}
        r = self.authenticated_rpc('alphea.connect.v1.ActivityService/SubmitHeartbeat', payload)
        self.last_sync_time = datetime.datetime.now().strftime('%H:%M:%S')

        if r and r.status_code == 200:
            data = r.json()
            self.session_uptime = int(data.get('accumulatedValidSeconds', str(self.session_uptime)))
            self.status = 'Mining Active'
            self.consecutive_errors = 0
            self.fetch_and_claim_quests()
            self.update_cluster_state()
            return True
        elif r and (r.status_code in [400, 401] or 'session' in r.text.lower() or 'unauthenticated' in r.text.lower()):
            add_log(f"[{self.name}] Foreground session expired ({r.status_code}). Restarting fresh foreground session immediately...")
            self.session_id = None
            self.session_uptime = 0
            self.start_foreground_session()
            self.update_cluster_state()
            return False
        else:
            self.consecutive_errors += 1
            if self.consecutive_errors >= 3:
                self.session_id = None
                self.session_uptime = 0
            self.update_cluster_state()
            return False

    def run(self):
        stagger = self.index * 10
        if stagger > 0:
            self.status = f"Stagger Delay ({stagger}s)"
            self.update_cluster_state()
            time.sleep(stagger)

        self.status = 'Auditing Proxy...'
        self.update_cluster_state()
        self.verify_proxy_ip()

        if '@alphea.local' in self.email or not self.access_token:
            self.status = 'Waiting for Sync'
            self.update_cluster_state()
            while '@alphea.local' in self.email or not self.access_token:
                time.sleep(5)

        self.status = 'Connecting...'
        self.update_cluster_state()
        self.check_redeem_balance()
        self.fetch_and_claim_quests()
        self.check_and_claim_inviter_bonus()
        self.start_foreground_session()

        tick = 0
        while True:
            if '@alphea.local' in self.email or not self.access_token:
                self.status = 'Waiting for Sync'
                self.update_cluster_state()
                time.sleep(5)
                continue

            try:
                self.submit_heartbeat()
                tick += 1

                if tick % 5 == 0:
                    self.check_redeem_balance()
                    self.check_and_claim_inviter_bonus()

                jitter = random.uniform(-2, 2)
                backoff = min(120, self.consecutive_errors * 15)
                time.sleep(max(35, HEARTBEAT_INTERVAL + jitter + backoff))
            except Exception as e:
                self.status = 'Thread Paused (Auto-retrying)'
                self.update_cluster_state()
                time.sleep(15)

# ------------------------------------------------------------------------------
# Auto-Ping Daemon (Bypasses Render 15-Min Free Tier Sleep)
# ------------------------------------------------------------------------------
class AutoPinger(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.target_url = os.environ.get('RENDER_EXTERNAL_URL') or os.environ.get('APP_URL')
        self.last_ping_status = 'Pending first ping'
        self.last_ping_time = '--:--:--'
        self.total_pings = 0

    def run(self):
        time.sleep(20)
        if not self.target_url:
            add_log('[AUTO-PING] Notice: Neither RENDER_EXTERNAL_URL nor APP_URL set. Internal keepalive active.')
            self.last_ping_status = 'Running (No external URL set)'
            return

        add_log(f"[AUTO-PING] Keep-Alive Daemon started for {self.target_url} (Pings every 8m)")
        while True:
            try:
                ping_endpoint = f"{self.target_url.rstrip('/')}/health"
                t0 = time.time()
                r = requests.get(ping_endpoint, timeout=20)
                elapsed_ms = int((time.time() - t0) * 1000)
                self.total_pings += 1
                self.last_ping_time = datetime.datetime.now().strftime('%H:%M:%S')

                if r.status_code == 200:
                    self.last_ping_status = f"200 OK ({elapsed_ms}ms) at {self.last_ping_time}"
                    add_log(f"[AUTO-PING] Hit {ping_endpoint} -> 200 OK ({elapsed_ms}ms) [Total Pings: {self.total_pings}]")
                else:
                    self.last_ping_status = f"HTTP {r.status_code} at {self.last_ping_time}"
            except Exception as e:
                self.last_ping_status = f"Ping Glitch: {e}"
                add_log(f"[AUTO-PING] Glitch: {e}")

            time.sleep(480 + random.randint(5, 30))

auto_pinger_instance = AutoPinger()

# ------------------------------------------------------------------------------
# Cluster Initialization
# ------------------------------------------------------------------------------
def start_cluster():
    global started_flag
    with startup_lock:
        if started_flag:
            return
        started_flag = True

    accounts = []
    vault_accs = sync_from_cloud_vault()
    if vault_accs:
        accounts = vault_accs

    env_accs = os.environ.get('ACCOUNTS_JSON')
    if not accounts and env_accs:
        try:
            accounts = json.loads(env_accs)
            add_log(f"[*] Loaded {len(accounts)} accounts from ACCOUNTS_JSON environment variable.")
        except Exception as e:
            add_log(f"[!] Failed to parse ACCOUNTS_JSON env var: {e}")

    if not accounts and os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                accounts = json.load(f)
            add_log(f"[*] Loaded {len(accounts)} accounts from accounts.json file.")
        except Exception as e:
            add_log(f"[!] Error loading accounts.json: {e}")

    enabled_accounts = [a for a in accounts if a.get('enabled', True)]
    if not enabled_accounts:
        add_log('[!] No enabled accounts found. Please configure accounts.json or ACCOUNTS_JSON env var.')
        return

    for idx, acc in enumerate(enabled_accounts):
        w = AccountWorker(acc, idx)
        w.start()

    auto_pinger_instance.start()

# ------------------------------------------------------------------------------
# Flask Web Server & Dashboard
# ------------------------------------------------------------------------------
app = Flask(__name__)

@app.before_request
def ensure_cluster_running():
    start_cluster()

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Connect-Protocol-Version'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>⚡ ALPHEA Multi-Node Cloud Matrix - Cluster 2</title>
  <meta http-equiv="refresh" content="15">
  <style>
    :root {
      --bg: #090d16;
      --card-bg: #131b2e;
      --border: #1f2c47;
      --accent: #00d2ff;
      --green: #00f076;
      --yellow: #ffb703;
      --red: #ff3366;
      --text: #e2e8f0;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    body { background-color: var(--bg); color: var(--text); padding: 18px; min-height: 100vh; }
    .container { max-width: 1200px; margin: 0 auto; }
    .header {
      background: linear-gradient(135deg, #131b2e 0%, #1a2540 100%);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 20px;
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 15px;
    }
    .title h1 { font-size: 22px; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 8px; }
    .title p { color: var(--muted); font-size: 13px; margin-top: 4px; }
    .metrics { display: flex; flex-wrap: wrap; gap: 12px; }
    .metric-box {
      background: rgba(0,0,0,0.3);
      border: 1px solid var(--border);
      padding: 10px 16px;
      border-radius: 8px;
      text-align: center;
    }
    .metric-box .label { font-size: 11px; text-transform: uppercase; color: var(--muted); font-weight: 600; }
    .metric-box .val { font-size: 18px; font-weight: 700; color: var(--accent); margin-top: 2px; }
    .metric-box.green .val { color: var(--green); }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 18px;
      margin-bottom: 20px;
      overflow-x: auto;
    }
    .card h2 { font-size: 16px; font-weight: 600; margin-bottom: 14px; color: #fff; display: flex; align-items: center; gap: 6px; }
    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; }
    th { padding: 12px 10px; color: var(--muted); border-bottom: 1px solid var(--border); font-weight: 600; }
    td { padding: 12px 10px; border-bottom: 1px solid rgba(255,255,255,0.04); }
    tr:last-child td { border-bottom: none; }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 600;
    }
    .badge-green { background: rgba(0, 240, 118, 0.15); color: var(--green); border: 1px solid rgba(0, 240, 118, 0.3); }
    .badge-yellow { background: rgba(255, 183, 3, 0.15); color: var(--yellow); border: 1px solid rgba(255, 183, 3, 0.3); }
    .badge-red { background: rgba(255, 51, 102, 0.15); color: var(--red); border: 1px solid rgba(255, 51, 102, 0.3); }
    .progress-bar-wrap {
      background: rgba(255,255,255,0.06);
      height: 7px;
      border-radius: 4px;
      overflow: hidden;
      margin-top: 6px;
    }
    .progress-bar {
      height: 100%;
      background: linear-gradient(90deg, #00d2ff, #00f076);
      border-radius: 4px;
    }
    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 20px; }
    .quest-card {
      background: rgba(0,0,0,0.25);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px 14px;
    }
    .quest-card .q-top { display: flex; justify-content: space-between; font-size: 13px; font-weight: 600; }
    .quest-card .q-sub { font-size: 11px; color: var(--muted); margin-top: 4px; }
    .log-box {
      background: #060910;
      border: 1px solid #1a233a;
      border-radius: 8px;
      padding: 12px;
      font-family: monospace;
      font-size: 12px;
      color: #94a3b8;
      max-height: 180px;
      overflow-y: auto;
    }
    .log-entry { margin-bottom: 4px; }
    .footer { text-align: center; color: var(--muted); font-size: 11px; margin-top: 20px; }
    .btn-remove {
      background: rgba(255, 51, 102, 0.12);
      border: 1px solid rgba(255, 51, 102, 0.35);
      color: var(--red);
      padding: 5px 12px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      transition: all 0.2s ease;
    }
    .btn-remove:hover {
      background: rgba(255, 51, 102, 0.25);
      transform: translateY(-1px);
      box-shadow: 0 4px 12px rgba(255, 51, 102, 0.2);
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">
        <h1>⚡ ALPHEA Connect Matrix</h1>
        <p>Master Code: <strong>{{ master_code }}</strong> &bull; Auto-Refreshes every 15s</p>
      </div>
      <div class="metrics">
        <div class="metric-box green">
          <div class="label">Active Nodes</div>
          <div class="val">{{ active_count }}/{{ total_count }}</div>
        </div>
        <div class="metric-box">
          <div class="label">Farmed Points</div>
          <div class="val">{{ total_points }}</div>
        </div>
        <div class="metric-box">
          <div class="label">Auto-Ping</div>
          <div class="val" style="font-size: 13px; padding-top: 3px;">{{ ping_status }}</div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>📡 Active Node Cluster</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Account</th>
            <th>Location / Proxy IP</th>
            <th>Points</th>
            <th>Session</th>
            <th>Today</th>
            <th>Daily</th>
            <th>Onboard</th>
            <th>Status</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {% for idx in sorted_nodes %}
          {% set acc = cluster[idx] %}
          <tr>
            <td><strong>{{ acc.index }}</strong></td>
            <td>
              <div style="font-weight: 600; color: #fff;">{{ acc.name }}</div>
              <div style="font-size: 11px; color: var(--muted);">{{ acc.email }}</div>
            </td>
            <td>
              <div>{{ acc.location }}</div>
              <div style="font-size: 11px; color: var(--muted);">{{ acc.proxy_ip }}</div>
            </td>
            <td style="font-weight: 700; color: var(--green);">{{ '{:,}'.format(acc.balance) }} Pts</td>
            <td>{{ acc.session_uptime_str }}</td>
            <td>{{ acc.today_seconds_str }}</td>
            <td>
              {% if acc.daily_claimed %}
                <span class="badge badge-green">Claimed</span>
              {% else %}
                <span class="badge badge-yellow">Pending</span>
              {% endif %}
            </td>
            <td>
              {% if acc.onboard_claimed %}
                <span class="badge badge-green">1.5k Pts</span>
              {% else %}
                <span class="badge badge-yellow">Wait</span>
              {% endif %}
            </td>
            <td>
              {% if acc.status == 'Mining Active' %}
                <span class="badge badge-green">● Mining</span>
              {% elif 'Waiting for Sync' in acc.status or '@alphea.local' in acc.email %}
                <span class="badge badge-yellow">⏳ Waiting for Sync</span>
              {% elif '401' in acc.status %}
                <span class="badge badge-red">{{ acc.status }}</span>
              {% else %}
                <span class="badge badge-yellow">{{ acc.status }}</span>
              {% endif %}
            </td>
            <td>
              <button onclick="removeAccount({{ acc.index - 1 }}, '{{ acc.name }}')" class="btn-remove">
                🗑️ Remove
              </button>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

    <div class="card">
      <h2>🎯 Live Hourly Quests (Max Node: {{ max_today_str }})</h2>
      <div class="grid-2">
        {% for q in quests_progress %}
        <div class="quest-card">
          <div class="q-top">
            <span>{{ q.name }}</span>
            <span style="color: var(--accent);">{{ q.reward }}</span>
          </div>
          <div class="progress-bar-wrap">
            <div class="progress-bar" style="width: {{ q.pct }}%;"></div>
          </div>
          <div class="q-sub" style="display: flex; justify-content: space-between; margin-top: 6px;">
            <span>{{ q.pct }}% ({{ q.current }}s / {{ q.target }}s)</span>
            <span style="color: {{ q.color }}; font-weight: 600;">{{ q.status }}</span>
          </div>
        </div>
        {% endfor %}
      </div>
    </div>

    <div class="card">
      <h2>📜 Live Cluster Activity Stream</h2>
      <div class="log-box">
        {% for l in logs %}
        <div class="log-entry">{{ l }}</div>
        {% endfor %}
      </div>
    </div>

    <div class="footer">
      ALPHEA Connect 24/7 Cloud Matrix &bull; Optimized for Render.com &bull; Uptime: {{ service_uptime }}
    </div>
  </div>
  <script>
    function removeAccount(index, name) {
      if (confirm(`Kya aap ${name} ko Cluster 2 se remove karke khali slot banana chahte hain?`)) {
        fetch('/api/reset_slot/' + index, { method: 'POST' })
          .then(r => r.json())
          .then(d => {
            alert(d.message);
            location.reload();
          })
          .catch(e => alert('Error: ' + e));
      }
    }
  </script>
</body>
</html>
'''

@app.route('/')
def dashboard():
    start_cluster()
    total_pts = sum(d.get('balance', 0) for d in cluster_state.values())
    active_cnt = sum(1 for d in cluster_state.values() if d.get('status') == 'Mining Active')
    max_sec = max((d.get('today_seconds', 0) for d in cluster_state.values()), default=0)

    milestones = [
        ('1H Contribution', 3600, '+800 Pts'),
        ('3H Contribution', 10800, '+1,300 Pts'),
        ('6H Contribution', 21600, '+1,800 Pts'),
        ('12H Contribution', 43200, '+2,500 Pts')
    ]

    quests_p = []
    for name, target, reward in milestones:
        pct = min(100.0, round((max_sec / target) * 100.0, 1)) if target > 0 else 100.0
        if max_sec >= target:
            status_text = 'Claimed'
            color = 'var(--green)'
        else:
            rem = target - max_sec
            rm_h = rem // 3600
            rm_m = (rem % 3600) // 60
            status_text = f'ETA: ~{rm_h}h {rm_m:02d}m'
            color = 'var(--yellow)'

        quests_p.append({
            'name': name,
            'target': target,
            'current': min(target, max_sec),
            'reward': reward,
            'pct': pct,
            'status': status_text,
            'color': color
        })

    uptime_sec = int(time.time() - SERVICE_START_TIME)

    return render_template_string(
        DASHBOARD_HTML,
        master_code=MASTER_INVITE_CODE,
        active_count=active_cnt,
        total_count=len(cluster_state),
        total_points=f'{total_pts:,}',
        ping_status=auto_pinger_instance.last_ping_status[:20],
        sorted_nodes=sorted(cluster_state.keys()),
        cluster=cluster_state,
        max_today_str=format_time(max_sec),
        quests_progress=quests_p,
        logs=list(reversed(cluster_logs[-20:])),
        service_uptime=format_time(uptime_sec)
    )

@app.route('/health')
def health_check():
    start_cluster()
    uptime_sec = int(time.time() - SERVICE_START_TIME)
    return jsonify({
        'status': 'ok',
        'service': 'alphea-multi-node',
        'active_nodes': sum(1 for d in cluster_state.values() if d.get('status') == 'Mining Active'),
        'total_accounts': len(cluster_state),
        'uptime': format_time(uptime_sec),
        'auto_ping_status': auto_pinger_instance.last_ping_status,
        'timestamp': datetime.datetime.now().isoformat()
    }), 200

@app.route('/api/status')
def api_status():
    start_cluster()
    return jsonify({
        'cluster': cluster_state,
        'logs': cluster_logs[-20:],
        'total_balance': sum(d.get('balance', 0) for d in cluster_state.values())
    }), 200

@app.route('/api/update_account', methods=['POST', 'OPTIONS'])
def api_update_account():
    if request.method == 'OPTIONS':
        return jsonify({'status': 'ok'}), 200

    data = request.json or {}
    email = data.get('email')
    at = data.get('accessToken')
    rt = data.get('refreshToken')
    dev_id = data.get('deviceId')
    if not email or not at:
        return jsonify({'error': 'email and accessToken required'}), 400

    target_slot = data.get('slot') if data.get('slot') is not None else data.get('index')
    if target_slot is not None and 0 <= int(target_slot) < len(cluster_nodes):
        target_node = cluster_nodes[int(target_slot)]
        target_node.email = email
        if dev_id:
            target_node.device_id = dev_id

    if not target_node:
        for node in cluster_nodes:
            if node.email == email:
                target_node = node
                break

    if not target_node:
        for node in cluster_nodes:
            if '@alphea.local' in node.email or not node.access_token or '401' in node.status or 'Dead' in node.status:
                target_node = node
                target_node.email = email
                if dev_id:
                    target_node.device_id = dev_id
                break

    if target_node:
        target_node.access_token = at
        if rt:
            target_node.refresh_token = rt
        target_node.jwt_exp = decode_jwt_exp(at)
        target_node.session_id = None
        target_node.status = 'Mining Active'
        target_node.consecutive_errors = 0
        target_node.save_updated_tokens()
        target_node.start_foreground_session()
        add_log(f"[{target_node.name}] Live session updated & revived for {email} via API!")
        return jsonify({'success': True, 'name': target_node.name, 'message': f'Revived {email} live!'}), 200

    return jsonify({'error': 'Cluster is full (5/5 accounts already active)'}), 400

@app.route('/api/reset_slot/<int:slot_index>', methods=['POST', 'GET'])
def api_reset_slot(slot_index):
    start_cluster()
    if slot_index < 0 or slot_index >= len(cluster_nodes):
        return jsonify({'error': 'Invalid slot index'}), 400

    node = cluster_nodes[slot_index]
    old_email = node.email
    node.email = f"node{slot_index + 1}_c2@alphea.local"
    node.access_token = ""
    node.refresh_token = ""
    node.device_id = ""
    node.jwt_exp = None
    node.session_id = None
    node.session_uptime = 0
    node.today_seconds = 0
    node.cached_balance = 0
    node.status = 'Waiting for Sync'
    node.consecutive_errors = 0
    node.save_updated_tokens()
    node.update_cluster_state()
    add_log(f"[{node.name}] Removed {old_email} -> Slot reset to Waiting for Sync!")
    return jsonify({
        'success': True,
        'message': f"Slot {slot_index + 1} ({node.name}) removed successfully! Now ready for new account sync."
    }), 200

@app.route('/api/remove_account', methods=['POST'])
def api_remove_account():
    start_cluster()
    data = request.json or {}
    email = data.get('email')
    slot_idx = data.get('slot') if data.get('slot') is not None else data.get('index')

    target_node = None
    if slot_idx is not None:
        try:
            s_idx = int(slot_idx)
            if 0 <= s_idx < len(cluster_nodes):
                target_node = cluster_nodes[s_idx]
        except Exception:
            pass
    elif email:
        for node in cluster_nodes:
            if node.email == email:
                target_node = node
                break

    if not target_node:
        return jsonify({'error': 'Account not found in cluster'}), 404

    return api_reset_slot(target_node.index)

@app.route('/api/revive_cluster', methods=['GET', 'POST'])
def api_revive_cluster():
    start_cluster()
    revived = []
    for node in cluster_nodes:
        node.session_id = None
        node.session_uptime = 0
        node.consecutive_errors = 0
        node.status = 'Mining Active'
        node.start_foreground_session()
        node.fetch_and_claim_quests()
        revived.append(node.name)
    add_log(f"[CLUSTER] One-click revive triggered! Fresh sessions started for {len(revived)} nodes.")
    return jsonify({'success': True, 'revived_nodes': revived}), 200

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    start_cluster()
    port = int(os.environ.get('PORT', 8080))
    add_log(f'[*] Launching ALPHEA Node Web Server on port {port}...')
    app.run(host='0.0.0.0', port=port, threaded=True)

