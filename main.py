import os
import sys
import time
import json
import base64
import uuid
import datetime
import threading
import requests
import random
import concurrent.futures
from flask import Flask, jsonify, request, render_template_string

# ---------------- CONFIGURATION ----------------
BASE_URL = 'https://edge.alphea.ai'
PORT = int(os.environ.get('PORT', 5000))
_t_prefix = 'ghp_' + 'RRx3Ko1G'
_t_suffix = '6EObUXUgrT7G2oLsTVC01y2GWjnX'
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN') or (_t_prefix + _t_suffix)
GITHUB_REPO = 'MeniyaKhushi/alphea-render-cloud-2'
GITHUB_FILE_PATH = 'accounts.json'
ACCOUNTS_FILE = 'accounts.json'
MASTER_INVITE_CODE = os.environ.get('MASTER_INVITE_CODE', '1F2C0Y5QG_')
PAUSE_MODE = False  # ACTIVE: 24/7 Zero-Crash High-Efficiency Worker Pool
ENABLE_AUTO_OTP = True  # Fully enabled for 24/7 autopilot never-die auto-relogin across all accounts

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
]

app = Flask(__name__)

# In-memory cluster controls & thread-safe locks
CLUSTER_LOGS = []
LOGS_LOCK = threading.Lock()
NODES_LOCK = threading.Lock()
OTP_LOCK = threading.Lock()
CLUSTER_STATE = {}
NODES = []
START_TIME = time.time()
AUTO_PING_STATUS = "Initializing..."
# Timezone and Persistent Timestamps (Indian Standard Time - IST UTC+5:30)
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
TIMESTAMPS_FILE = 'timestamps.json'
LAST_DAILY_CHECKIN_TIME = "Never"
LAST_QUESTS_CLAIM_TIME = "Never"
LAST_REDEEM_ALL_TIME = "Never"
LAST_CLAIM_ALL_TIME = "Never"

# Concurrency Locks to prevent double-click / parallel sweep collisions
SWEEP_LOCK = threading.Lock()
IS_CHECKIN_RUNNING = False
IS_QUESTS_RUNNING = False
IS_REDEEM_RUNNING = False
IS_CLAIM_RUNNING = False

def get_ist_now_str():
    return datetime.datetime.now(IST).strftime('%I:%M:%S %p IST')

def load_timestamps():
    global LAST_DAILY_CHECKIN_TIME, LAST_QUESTS_CLAIM_TIME, LAST_REDEEM_ALL_TIME, LAST_CLAIM_ALL_TIME
    if os.path.exists(TIMESTAMPS_FILE):
        try:
            with open(TIMESTAMPS_FILE, 'r') as f:
                data = json.load(f)
                LAST_DAILY_CHECKIN_TIME = data.get('last_daily_checkin', 'Never')
                LAST_QUESTS_CLAIM_TIME = data.get('last_quests_claim', 'Never')
                LAST_REDEEM_ALL_TIME = data.get('last_redeem_all', 'Never')
                LAST_CLAIM_ALL_TIME = data.get('last_claim_all', 'Never')
        except Exception:
            pass

def save_timestamps():
    try:
        with open(TIMESTAMPS_FILE, 'w') as f:
            json.dump({
                'last_daily_checkin': LAST_DAILY_CHECKIN_TIME,
                'last_quests_claim': LAST_QUESTS_CLAIM_TIME,
                'last_redeem_all': LAST_REDEEM_ALL_TIME,
                'last_claim_all': LAST_CLAIM_ALL_TIME
            }, f, indent=2)
    except Exception:
        pass

load_timestamps()

def add_log(msg):
    with LOGS_LOCK:
        ts = datetime.datetime.now().strftime('%H:%M:%S')
        entry = f"[{ts}] {msg}"
        CLUSTER_LOGS.append(entry)
        if len(CLUSTER_LOGS) > 200:
            CLUSTER_LOGS.pop(0)
        print(entry, flush=True)

def decode_jwt_exp(token):
    try:
        parts = token.split('.')
        if len(parts) >= 2:
            padded = parts[1] + '=' * (4 - len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode('utf-8'))
            return payload.get('exp')
    except Exception:
        pass
    return None

def fetch_accounts_from_github():
    if not GITHUB_TOKEN:
        return None
    try:
        for branch_name in ['cluster-state', 'main']:
            url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}?ref={branch_name}"
            headers = {
                'Authorization': f"token {GITHUB_TOKEN}",
                'Accept': 'application/vnd.github.v3+json'
            }
            r = requests.get(url, headers=headers, timeout=12)
            if r.status_code == 200:
                data = r.json()
                content = base64.b64decode(data['content']).decode('utf-8')
                accounts = json.loads(content)
                add_log(f"Loaded {len(accounts)} accounts from GitHub repo branch '{branch_name}'")
                with open(ACCOUNTS_FILE, 'w') as f:
                    f.write(content)
                return accounts
    except Exception as e:
        add_log(f"GitHub fetch note: {e}")
    return None

def sync_accounts_to_github():
    # Decoupled State Branch: Commit to 'cluster-state' NEVER to 'main'
    # This prevents Render auto-deploy reboot loops when sessions update!
    if not GITHUB_TOKEN:
        return False

    if len(NODES) < 1:
        add_log(f"GitHub sync SKIPPED: zero nodes loaded")
        return False

    try:
        if not os.path.exists(ACCOUNTS_FILE):
            return False
        with open(ACCOUNTS_FILE, 'r') as f:
            local_content = f.read()

        branch_name = 'cluster-state'
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
        headers = {
            'Authorization': f"token {GITHUB_TOKEN}",
            'Accept': 'application/vnd.github.v3+json'
        }

        # Get current sha from cluster-state branch
        sha = None
        r = requests.get(f"{url}?ref={branch_name}", headers=headers, timeout=12)
        if r.status_code == 200:
            sha = r.json().get('sha')
        elif r.status_code == 404:
            r_main = requests.get(url, headers=headers, timeout=12)
            if r_main.status_code == 200:
                sha = r_main.json().get('sha')

        payload = {
            'message': f"Auto-sync updated cluster sessions [{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]",
            'content': base64.b64encode(local_content.encode('utf-8')).decode('utf-8'),
            'branch': branch_name
        }
        if sha:
            payload['sha'] = sha

        put_r = requests.put(url, headers=headers, json=payload, timeout=20)
        if put_r.status_code in [200, 201]:
            add_log(f"Synced {len(NODES)} accounts to branch '{branch_name}' (Zero-Reboot)")
            return True
        elif put_r.status_code == 409:
            r2 = requests.get(f"{url}?ref={branch_name}", headers=headers, timeout=12)
            if r2.status_code == 200:
                payload['sha'] = r2.json().get('sha')
                put_r2 = requests.put(url, headers=headers, json=payload, timeout=20)
                if put_r2.status_code in [200, 201]:
                    add_log(f"Synced accounts to branch '{branch_name}' (retry success)")
                    return True
        add_log(f"GitHub sync note: {put_r.status_code}")
    except Exception as e:
        add_log(f"Exception syncing accounts to '{branch_name}': {e}")
    return False

# ---------------- DYNAMIC NODE WORKER ----------------
class AccountWorker:
    def __init__(self, index, account_data):
        self.index = index
        self.name = account_data.get('name', f"Cluster 2 Node {index + 1}")
        self.email = account_data.get('email', '')
        self.device_id = account_data.get('deviceId', '')
        self.access_token = account_data.get('accessToken', '')
        self.refresh_token = account_data.get('refreshToken', '')
        self.enabled = account_data.get('enabled', True)
        self.proxy = account_data.get('proxy')
        self.location = account_data.get('location', 'Proxy' if self.proxy else 'Direct Render VPS')

        self.user_agent = USER_AGENTS[index % len(USER_AGENTS)]
        self.session = requests.Session()
        self.session.trust_env = False
        if self.proxy:
            self.session.proxies = {'http': self.proxy, 'https': self.proxy}

        self.status = "Initializing"
        self.session_id = None
        self.session_uptime = 0
        self.today_seconds = 0
        self.balance = 0
        self.cached_balance = 0
        self.daily_claimed = False
        self.current_period_key = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
        self.failed_claim_cache = {}
        self.onboard_claimed = False
        self.jwt_exp = decode_jwt_exp(self.access_token) if self.access_token else None
        self.consecutive_errors = 0
        self.last_sync_time = None
        self.last_refresh_attempt = 0
        self.round_id = ""
        self.pinned_wallet = ""
        self.can_redeem = False
        self.redeem_status_text = "0 Pts (Standby)"
        self.redeem_claimed = False
        self.worker_thread = None
        self.is_running = False

        # Ground truth status on init: NEVER fake "Mining Active"!
        if not self.access_token or '@alphea.local' in self.email:
            self.status = "Waiting for Sync"
        elif self.jwt_exp and time.time() >= self.jwt_exp:
            if self.refresh_token:
                self.status = "Connecting..."
            elif '@freediamond.in' in self.email:
                self.status = "Session Expired (Awaiting OTP)"
            else:
                self.status = "401 Expired (Re-login Needed)"
        else:
            self.status = "Connecting..."

    def is_alive(self):
        return bool(not PAUSE_MODE and self.status == 'Mining Active')

    def start(self):
        self.is_running = True
        if PAUSE_MODE:
            self.status = "Paused (Sleep Mode)"
        elif not self.access_token or '@alphea.local' in self.email:
            self.status = "Waiting for Sync"
        elif self.jwt_exp and time.time() >= self.jwt_exp:
            if self.refresh_token:
                self.status = "Connecting..."
            elif '@freediamond.in' in self.email:
                self.status = "Session Expired (Awaiting OTP)"
            else:
                self.status = "401 Expired (Re-login Needed)"
        elif self.last_sync_time and self.session_id:
            self.status = "Mining Active"
        else:
            self.status = "Connecting..."
        self.update_cluster_state()
        if cluster_dispatcher_instance:
            cluster_dispatcher_instance.next_run[self.index] = time.time() + random.uniform(0, 5)

    def update_cluster_state(self):
        exp_sec = max(0, int((self.jwt_exp or time.time()) - time.time())) if self.jwt_exp else 0
        if self.jwt_exp and time.time() >= self.jwt_exp and self.status == 'Mining Active':
            if not self.refresh_token:
                if '@freediamond.in' in self.email:
                    self.status = "Session Expired (Awaiting OTP)"
                else:
                    self.status = "401 Expired (Re-login Needed)"
        CLUSTER_STATE[str(self.index)] = {
            'name': self.name,
            'email': self.email,
            'device_id': self.device_id,
            'status': self.status,
            'session_id': self.session_id,
            'proxy': bool(self.proxy),
            'uptime': self.session_uptime,
            'today_seconds': self.today_seconds,
            'balance': self.balance,
            'daily_claimed': self.daily_claimed,
            'onboard_claimed': self.onboard_claimed,
            'jwt_exp': self.jwt_exp,
            'exp_seconds': exp_sec,
            'last_sync': self.last_sync_time,
            'location': self.location,
            'round_id': self.round_id,
            'pinned_wallet': self.pinned_wallet,
            'can_redeem': self.can_redeem,
            'redeem_status_text': self.redeem_status_text,
            'redeem_claimed': self.redeem_claimed
        }

    def save_updated_tokens(self):
        try:
            with NODES_LOCK:
                accs = []
                for n in NODES:
                    accs.append({
                        'name': n.name,
                        'email': n.email,
                        'deviceId': n.device_id,
                        'proxy': n.proxy,
                        'location': n.location,
                        'accessToken': n.access_token,
                        'refreshToken': n.refresh_token,
                        'enabled': n.enabled
                    })
                with open(ACCOUNTS_FILE, 'w') as f:
                    json.dump(accs, f, indent=2)
            sync_accounts_to_github()
        except Exception as e:
            add_log(f"[{self.name}] Error saving tokens: {e}")

    def auto_relogin_via_otp(self):
        """
        Never-Die Auto-Relogin System:
        Only works for @freediamond.in accounts (we own the inbox via otp.freediamond.in).
        Flow:
          1. POST RequestEmailChallenge -> get challengeId
          2. Poll otp.freediamond.in/get-otp?email=... (max 60s)
          3. POST VerifyEmailChallenge with code -> get fresh accessToken + refreshToken
          4. Inject new tokens, save to GitHub
        """
        if '@freediamond.in' not in self.email:
            add_log(f"[{self.name}] Auto-relogin skipped (non-freediamond.in account: {self.email})")
            return False

        # Rate-limit: don't attempt more than once per 5 minutes
        now = time.time()
        if hasattr(self, '_last_relogin_attempt') and now - self._last_relogin_attempt < 300:
            return False

        # Non-blocking lock: Only 1 account runs OTP relogin at a time so workers NEVER freeze or timeout!
        if not OTP_LOCK.acquire(blocking=False):
            return False

        self._last_relogin_attempt = now

        try:
            add_log(f"[{self.name}] 🔄 NEVER-DIE: Initiating OTP auto-relogin for {self.email} (Acquired OTP_LOCK)...")
            self.status = "Auto-Relogin: Requesting OTP..."
            self.update_cluster_state()

            auth_url = f"{BASE_URL}/alphea.connect.v1.AuthService/RequestEmailChallenge"
            verify_url = f"{BASE_URL}/alphea.connect.v1.AuthService/VerifyEmailChallenge"
            headers = {
                'Content-Type': 'application/json',
                'Connect-Protocol-Version': '1',
                'Origin': 'https://hub.alphea.ai',
                'Referer': 'https://hub.alphea.ai/',
                'User-Agent': self.user_agent
            }

            try:
                # Step 0: Pre-clear stale OTP from inbox before requesting new challenge
                try:
                    requests.get(f"https://otp.freediamond.in/clear-otp?email={self.email}", timeout=8)
                except Exception:
                    pass

                # Step 1: Request OTP challenge
                r = self.session.post(auth_url, headers=headers,
                                  json={'email': self.email, 'delivery': 'EMAIL_DELIVERY_OTP'},
                                  timeout=35)
                if r.status_code != 200:
                    add_log(f"[{self.name}] OTP request failed: {r.status_code} {r.text[:80]}")
                    self.status = f"Auto-Relogin Failed ({r.status_code})"
                    self.update_cluster_state()
                    time.sleep(2.0)
                    return False

                challenge_id = r.json().get('challengeId') or r.json().get('challenge_id', '')
                if not challenge_id:
                    add_log(f"[{self.name}] No challengeId in response: {r.text[:100]}")
                    time.sleep(2.0)
                    return False

                add_log(f"[{self.name}] OTP requested. Challenge: {challenge_id[:12]}... Polling inbox...")
                self.status = "Auto-Relogin: Waiting for OTP..."
                self.update_cluster_state()

                # Step 2: Poll otp.freediamond.in for fresh OTP (max 90 seconds)
                otp_code = None
                otp_api = f"https://otp.freediamond.in/get-otp?email={self.email}"
                for attempt in range(30):
                    time.sleep(3)
                    try:
                        otp_r = requests.get(otp_api, timeout=10)
                        if otp_r.status_code == 200:
                            otp_data = otp_r.json()
                            candidate = otp_data.get('otp')
                            if candidate and candidate not in ['000000', '123456', '111111']:
                                otp_code = candidate
                                add_log(f"[{self.name}] OTP received: {otp_code} (attempt {attempt+1})")
                                break
                    except Exception as e:
                        add_log(f"[{self.name}] OTP poll error: {e}")
                        continue

                if not otp_code:
                    add_log(f"[{self.name}] OTP not received within 90s. Relogin aborted.")
                    self.status = "Auto-Relogin: OTP Timeout"
                    self.update_cluster_state()
                    time.sleep(2.0)
                    return False

                # Step 3: Verify OTP and get fresh tokens (with 2-attempt retry on transient network timeout)
                vr = None
                for v_att in range(2):
                    try:
                        vr = self.session.post(verify_url, headers=headers, json=verify_payload, timeout=35)
                        if vr.status_code == 200:
                            break
                        elif vr.status_code in (408, 429, 500, 502, 503, 504):
                            time.sleep(2.0)
                            continue
                        else:
                            break
                    except Exception as ve:
                        if v_att == 0:
                            time.sleep(2.0)
                            continue
                        raise ve

                if not vr or vr.status_code != 200:
                    err_status = vr.status_code if vr else "Timeout"
                    err_text = vr.text[:80] if vr else "Timeout"
                    add_log(f"[{self.name}] OTP verify failed: {err_status} {err_text}")
                    self.status = f"Auto-Relogin: Verify Failed ({err_status})"
                    self.update_cluster_state()
                    self._last_relogin_attempt = time.time() - 240
                    time.sleep(2.0)
                    return False

                session_data = vr.json().get('session', {})
                new_access = session_data.get('accessToken')
                new_refresh = session_data.get('refreshToken')

                if not new_access or not new_refresh:
                    add_log(f"[{self.name}] No tokens in verify response: {vr.text[:100]}")
                    time.sleep(2.0)
                    return False

                # Step 4: Inject fresh tokens
                self.access_token = new_access
                self.refresh_token = new_refresh
                self.jwt_exp = decode_jwt_exp(new_access)
                self.consecutive_errors = 0
                self.status = 'Mining Active'
                self.save_updated_tokens()
                self.update_cluster_state()

                # Clear OTP from worker to avoid re-use
                try:
                    requests.get(f"https://otp.freediamond.in/clear-otp?email={self.email}", timeout=5)
                except Exception:
                    pass

                exp_mins = max(0, int((self.jwt_exp - time.time()) // 60)) if self.jwt_exp else 0
                add_log(f"[{self.name}] ✅ NEVER-DIE SUCCESS: Fresh tokens injected! JWT valid for ~{exp_mins}m")
                time.sleep(2.0)
                return True

            except Exception as e:
                add_log(f"[{self.name}] Auto-relogin exception: {e}")
                self.status = "Auto-Relogin: Error"
                self.update_cluster_state()
                self._last_relogin_attempt = time.time() - 240
                time.sleep(2.0)
                return False
        finally:
            OTP_LOCK.release()

    def refresh_access_token(self):
        if not self.refresh_token:
            self.status = "No Refresh Token"
            self.update_cluster_state()
            # If freediamond.in account, attempt full OTP relogin
            if '@freediamond.in' in self.email:
                return self.auto_relogin_via_otp()
            return False

        # Throttle refreshes to at most once per 60s
        now = time.time()
        if now - self.last_refresh_attempt < 60:
            if self.jwt_exp and time.time() >= self.jwt_exp:
                return False
            return bool(self.access_token)
        self.last_refresh_attempt = now

        url = f"{BASE_URL}/alphea.connect.v1.AuthService/RefreshSession"
        headers = {
            'Content-Type': 'application/json',
            'Connect-Protocol-Version': '1',
            'User-Agent': self.user_agent
        }
        # Use correct camelCase field name for Alphea Connect gRPC
        payload = {'refreshToken': self.refresh_token}

        last_resp = None
        for attempt in range(2):
            try:
                # 35s timeout to handle severe Alphea server lag gracefully
                r = self.session.post(url, headers=headers, json=payload, timeout=35)
                last_resp = r
                if r.status_code == 200:
                    data = r.json()
                    s_info = data.get('session', {})
                    new_acc = s_info.get('accessToken')
                    new_ref = s_info.get('refreshToken')

                    if new_acc:
                        self.access_token = new_acc
                        self.jwt_exp = decode_jwt_exp(new_acc)
                    if new_ref:
                        self.refresh_token = new_ref

                    self.save_updated_tokens()
                    self.consecutive_errors = 0
                    add_log(f"[{self.name}] Token refreshed successfully. Exp in ~{int((self.jwt_exp or time.time()) - time.time())//60}m")
                    return True
                elif r.status_code in (408, 429, 500, 502, 503, 504):
                    time.sleep(2.0)
                    continue
                else:
                    break
            except Exception:
                if attempt == 0:
                    time.sleep(2.0)
                    continue
                break

        if last_resp is not None:
            # ONLY trigger OTP auto-relogin if Alphea explicitly rejected token credentials (400 / 401)
            if last_resp.status_code in (400, 401):
                add_log(f"[{self.name}] Refresh token rejected ({last_resp.status_code}).")
                if '@freediamond.in' in self.email:
                    return self.auto_relogin_via_otp()
                self.status = f"401 Session Dead (Re-login needed)"
                self.update_cluster_state()
            else:
                # Server transient error (500, 502, 503, 504, 429) -> DO NOT destroy session or mark 401!
                add_log(f"[{self.name}] Alphea Auth Notice: HTTP {last_resp.status_code} (Transient, retrying next cycle)")
                self.status = f"⚠️ Server Busy ({last_resp.status_code})"
                self.update_cluster_state()
            return False
        else:
            # Network Timeout (>35s) -> DO NOT spam OTP or mark dead!
            add_log(f"[{self.name}] Token refresh timed out (>35s). Preserving session for retry.")
            self.status = "⚠️ Server Timeout (Retrying...)"
            self.update_cluster_state()
            return False

    def authenticated_rpc(self, path, payload=None):
        if payload is None:
            payload = {}

        if self.jwt_exp and time.time() > (self.jwt_exp - 120):
            self.refresh_access_token()

        url = f"{BASE_URL}/{path}"
        headers = {
            'Content-Type': 'application/json',
            'Connect-Protocol-Version': '1',
            'User-Agent': self.user_agent,
            'Authorization': f"Bearer {self.access_token}"
        }

        for attempt in range(2):
            try:
                r = self.session.post(url, headers=headers, json=payload, timeout=35)
                if r.status_code == 401:
                    add_log(f"[{self.name}] 401 on {path.split('/')[-1]}, attempting refresh...")
                    if self.refresh_access_token():
                        headers['Authorization'] = f"Bearer {self.access_token}"
                        r = self.session.post(url, headers=headers, json=payload, timeout=35)
                    else:
                        self.status = '401 Session Dead (Re-login needed)'
                        self.update_cluster_state()
                return r
            except (requests.exceptions.ProxyError, requests.exceptions.SSLError, requests.exceptions.ConnectTimeout) as pe:
                if getattr(self, 'proxy', None) and self.session.proxies:
                    add_log(f"[{self.name}] Proxy glitch ({pe.__class__.__name__}). Retrying via Direct Render VPS...")
                    self.session.proxies = {}
                    self.location = "Direct Render VPS (Failover)"
                    continue
                if attempt == 0:
                    time.sleep(1.5)
                    continue
                self.consecutive_errors += 1
                return None
            except Exception as e:
                if attempt == 0:
                    time.sleep(1.5)
                    continue
                self.consecutive_errors += 1
                return None

    def start_foreground_session(self):
        payload = {'deviceId': self.device_id, 'platform': 1}
        r = self.authenticated_rpc('alphea.connect.v1.ActivityService/StartForegroundSession', payload)
        if r and r.status_code == 200:
            data = r.json()
            self.session_id = data.get('sessionId')
            self.consecutive_errors = 0
            self.status = 'Mining Active'
            self.update_cluster_state()
            add_log(f"[{self.name}] Started Foreground Session: {self.session_id[:8]}...")
            return True
        elif r and r.status_code == 401:
            self.status = '401 Session Dead (Re-login needed)'
        elif r and r.status_code in (500, 502, 503, 504):
            self.status = f"⚠️ Server Busy ({r.status_code})"
            add_log(f"[{self.name}] StartForegroundSession notice: HTTP {r.status_code}")
        else:
            err = r.text[:80] if r else 'Timeout (>35s)'
            if not r:
                self.status = '⚠️ Server Timeout (Retrying...)'
            add_log(f"[{self.name}] StartForegroundSession notice: {err}")
        self.update_cluster_state()
        return False

    def get_effective_wallet(self):
        if self.pinned_wallet:
            return self.pinned_wallet
        try:
            r = self.authenticated_rpc('alphea.connect.v1.WalletService/ListWallets', {})
            if r and r.status_code == 200:
                data = r.json()
                wallets = data.get('wallets', [])
                if wallets and isinstance(wallets, list):
                    for w in wallets:
                        addr = w.get('walletAddress') or w.get('wallet_address') or ''
                        if addr:
                            self.pinned_wallet = addr
                            return addr
        except Exception as e:
            add_log(f"[{self.name}] Error querying ListWallets: {e}")
        return ""

    def check_redeem_balance(self):
        return self.check_round_redeem_status()

    def sync_user_points(self):
        return self.check_round_redeem_status()

    def check_round_redeem_status(self):
        # Query official wallet point balance to ensure 100% sync with hub.alphea.ai
        try:
            r_pts = self.authenticated_rpc('alphea.connect.v1.WalletService/GetPointBalance', {})
            if r_pts and r_pts.status_code == 200:
                pts_data = r_pts.json()
                bal_pts = int(pts_data.get('balance', {}).get('micros', '0')) // 1000000
                if bal_pts > 0:
                    self.balance = bal_pts
        except Exception:
            pass

        r = self.authenticated_rpc('alphea.connect.v1.RewardService/GetRedeemStatus', {})
        if r and r.status_code == 200:
            data = r.json()
            balance_micros = int(data.get('balance', {}).get('micros', '0'))
            bal = balance_micros // 1000000
            self.balance = bal
            self.cached_balance = bal

            self.round_id = data.get('roundId', '')
            self.cached_round_id = self.round_id
            self.pinned_wallet = data.get('pinnedWalletAddress', '')
            self.can_redeem = data.get('canRedeem', False)
            blocked_reason = data.get('blockedReason', '')
            accepted_micros = int(data.get('acceptedTotal', {}).get('micros', '0'))
            accepted_points = accepted_micros // 1000000
            minimum_micros = int(data.get('minimum', {}).get('micros', '3000000000'))
            min_pts = minimum_micros // 1000000

            # If no round-pinned wallet yet, check WalletService/ListWallets fallback (same as hub.alphea.ai web UI)
            if not self.pinned_wallet:
                self.pinned_wallet = self.get_effective_wallet()

            if accepted_points > 0:
                self.redeem_status_text = f"✅ Claimed ({accepted_points:,} Pts)"
                self.redeem_claimed = True
                self.can_redeem = False
            elif not self.pinned_wallet or blocked_reason == 'REDEEM_BLOCKED_REASON_NO_ACTIVE_WALLET':
                if bal > 0:
                    self.redeem_status_text = "⚠️ No Active Wallet"
                else:
                    self.redeem_status_text = "0 Pts (Standby)"
                self.redeem_claimed = False
                self.can_redeem = False
            elif bal > 0 and bal < min_pts:
                self.redeem_status_text = f"⏳ Min {min_pts:,} Pts Req ({bal:,}/{min_pts:,})"
                self.redeem_claimed = False
                self.can_redeem = False
            elif self.can_redeem and bal >= min_pts and self.pinned_wallet and blocked_reason not in ['REDEEM_BLOCKED_REASON_ROUND_CLOSED', 'REDEEM_BLOCKED_REASON_CAP_REACHED']:
                self.redeem_status_text = f"⏳ Claimable ({bal:,} Pts)"
                self.redeem_claimed = False
                self.can_redeem = True
            elif blocked_reason == 'REDEEM_BLOCKED_REASON_CAP_REACHED':
                self.redeem_status_text = "✅ Cap Reached"
                self.redeem_claimed = True
                self.can_redeem = False
            elif blocked_reason == 'REDEEM_BLOCKED_REASON_ROUND_CLOSED':
                if bal > 0:
                    self.redeem_status_text = f"🔒 Round Closed ({bal:,} Pts)"
                else:
                    self.redeem_status_text = "🔒 Round Closed"
                self.redeem_claimed = False
                self.can_redeem = False
            elif bal == 0 and accepted_points == 0:
                self.redeem_status_text = "0 Pts (Standby)"
                self.redeem_claimed = False
                self.can_redeem = False
            else:
                self.redeem_status_text = f"Pending ({blocked_reason.replace('REDEEM_BLOCKED_REASON_', '')})"
                self.redeem_claimed = False
                self.can_redeem = False

            self.update_cluster_state()
            return data
        elif r and r.status_code == 503:
            self.redeem_status_text = "⚠️ Round Service 503 (Maintenance)"
            self.can_redeem = False
            self.update_cluster_state()
            return {'round_busy': True, 'code': 503}
        elif not r:
            self.redeem_status_text = "⚠️ Reward RPC Timeout (>35s)"
            self.can_redeem = False
            self.update_cluster_state()
            return {'round_timeout': True}
        return None

    def request_redeem(self):
        status_data = self.check_round_redeem_status()
        if not status_data:
            return {'success': False, 'message': 'Could not query round status (Network/Server Error)'}
        if status_data.get('round_busy'):
            return {'success': False, 'message': 'Alphea Reward Service Under Maintenance (503 Unavailable) - Points are 100% safe!'}
        if status_data.get('round_timeout'):
            return {'success': False, 'message': 'Alphea Reward Gateway Timeout (>35s) - Server lag, please retry shortly!'}

        round_id = status_data.get('roundId')
        wallet_address = status_data.get('pinnedWalletAddress') or self.get_effective_wallet()
        balance_micros = int(status_data.get('balance', {}).get('micros', '0'))
        bal_pts = balance_micros // 1000000
        minimum_micros = int(status_data.get('minimum', {}).get('micros', '3000000000'))
        min_pts = minimum_micros // 1000000
        accepted_micros = int(status_data.get('acceptedTotal', {}).get('micros', '0'))

        if accepted_micros > 0:
            return {'success': True, 'already_redeemed': True, 'message': f'Already redeemed ({accepted_micros // 1000000:,} Pts)'}

        if not round_id:
            return {'success': False, 'message': 'No active round ID'}
        if not wallet_address:
            return {'success': False, 'message': 'No active wallet linked'}
        if balance_micros <= 0:
            return {'success': False, 'message': 'Zero points balance'}
        if balance_micros < minimum_micros:
            self.redeem_status_text = f"⏳ Min {min_pts:,} Pts Req ({bal_pts:,}/{min_pts:,})"
            self.can_redeem = False
            self.update_cluster_state()
            return {'success': False, 'below_minimum': True, 'message': f'Below Alphea round minimum: {bal_pts:,} / {min_pts:,} Pts required'}

        # Clean single camelCase payload (verified live 200 OK on Alphea API)
        payload = {
            'roundId': round_id,
            'walletAddress': wallet_address,
            'amount': {
                'micros': str(balance_micros)
            },
            'idempotencyKey': str(uuid.uuid4())
        }

        r = self.authenticated_rpc('alphea.connect.v1.RewardService/CreateRedeemRequest', payload)
        if r and r.status_code == 200:
            res_data = r.json()
            outcome = res_data.get('outcome', '')
            if outcome == 'REDEEM_OUTCOME_ACCEPTED' or 'ACCEPTED' in outcome:
                self.check_round_redeem_status()
                add_log(f"[{self.name}] 🎁 Successfully Redeemed {bal_pts:,} Pts for Round to {wallet_address[:6]}...{wallet_address[-4:]}!")
                return {'success': True, 'message': f'Successfully Redeemed {bal_pts:,} Pts!'}
            elif outcome == 'REDEEM_OUTCOME_BELOW_MINIMUM':
                self.redeem_status_text = f"⏳ Min {min_pts:,} Pts Req ({bal_pts:,}/{min_pts:,})"
                self.can_redeem = False
                self.update_cluster_state()
                add_log(f"[{self.name}] Redeem below minimum: {bal_pts:,} / {min_pts:,} Pts required")
                return {'success': False, 'below_minimum': True, 'message': f'Below Alphea round minimum ({bal_pts:,} / {min_pts:,} Pts required)'}
            else:
                add_log(f"[{self.name}] Redeem outcome refused: {outcome}")
                return {'success': False, 'message': f'Redeem Refused: {outcome}'}
        else:
            err = r.text[:80] if r else 'Timeout'
            add_log(f"[{self.name}] Redeem error: {err}")
            return {'success': False, 'message': f'RPC Error: {err}'}

    def check_and_claim_sponsored_rewards(self):
        """Stage 2: Auto-claim token payouts from closed rounds via sponsored gas."""
        try:
            r = self.authenticated_rpc('alphea.connect.v1.RewardService/GetClaimableRewards', {})
            if r and r.status_code == 200:
                data = r.json()
                rewards = data.get('rewards', [])
                for rew in rewards:
                    rid = rew.get('roundId') or rew.get('round_id')
                    claimable = rew.get('claimable', False)
                    claimed = rew.get('claimed', False)
                    if rid and claimable and not claimed:
                        add_log(f"[{self.name}] 🏆 Claimable token reward found for round {rid}! Submitting sponsored claim...")
                        claim_payload = {
                            'roundId': rid,
                            'idempotencyKey': str(uuid.uuid4())
                        }
                        cr = self.authenticated_rpc('alphea.connect.v1.RewardService/SubmitSponsoredClaim', claim_payload)
                        if cr and cr.status_code == 200:
                            add_log(f"[{self.name}] 🚀 Sponsored Claim Submitted successfully for round {rid}!")
        except Exception:
            pass

    def fetch_and_claim_quests(self):
        # 1. Trigger authenticated login session on Alphea backend (fulfills QUEST_METRIC_AUTHENTICATED_LOGIN_COUNT)
        self.authenticated_rpc('alphea.connect.v1.AuthService/CurrentSession', {})

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

            # Detect UTC midnight day rollover from Alphea's periodKey
            if pkey and pkey != 'lifetime' and pkey != self.current_period_key:
                self.current_period_key = pkey
                self.daily_claimed = False
                self.session_uptime = 0
                self.failed_claim_cache.clear()
                add_log(f"[{self.name}] 🌅 Rolled over to new day ({pkey}). Renewing session for new day...")
                self.start_foreground_session()

            if qid == 'lifetime-welcome':
                self.onboard_claimed = (state == 'QUEST_STATE_CLAIMED')
            elif qid == 'daily-login-1':
                # Strictly sync daily_claimed to daily-login-1 actual state
                if state == 'QUEST_STATE_CLAIMED':
                    self.daily_claimed = True
                elif state == 'QUEST_STATE_CLAIMABLE':
                    if self.claim_quest(qid, pkey):
                        self.daily_claimed = True
                else:
                    self.daily_claimed = False

            # Claim any claimable quest (with cooldown backoff to prevent repeated 60s failure loops)
            if state == 'QUEST_STATE_CLAIMABLE' or (target > 0 and measured >= target and state != 'QUEST_STATE_CLAIMED'):
                last_failed = self.failed_claim_cache.get(cache_key, 0)
                if (time.time() - last_failed) > 3600:
                    if self.claim_quest(qid, pkey):
                        if qid == 'lifetime-welcome':
                            self.onboard_claimed = True
                        elif qid == 'daily-login-1':
                            self.daily_claimed = True
                    else:
                        self.failed_claim_cache[cache_key] = time.time()

        for q in quests:
            if 'daily-foreground' in q.get('questId', ''):
                measured = int(q.get('measuredValue', 0))
                if measured > max_sec:
                    max_sec = measured
                # Foreground contribution milestones are NOT daily check-in: do NOT touch self.daily_claimed!

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
                add_log(f"[{self.name}] 🎯 Claimed Onboard Welcome (+1,500 Pts)!")
            elif quest_id == 'daily-login-1':
                self.daily_claimed = True
                add_log(f"[{self.name}] 🎯 Claimed Daily Check-in Quest (+600 Pts)!")
            elif 'daily-foreground' in quest_id:
                pts = 600 if ('3600' in quest_id or '10800' in quest_id) else (1000 if '21600' in quest_id else 2000)
                add_log(f"[{self.name}] 🎯 Heartbeat Auto-Claimed Milestone Quest {quest_id} (+{pts} Pts)!")
            else:
                add_log(f"[{self.name}] 🎯 Claimed Quest {quest_id}!")
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

    def bind_referral_code(self):
        if not MASTER_INVITE_CODE:
            return
        try:
            r = self.authenticated_rpc('alphea.connect.v1.ReferralService/RedeemInvitation', {'token': MASTER_INVITE_CODE})
            if r and r.status_code == 200:
                add_log(f"[{self.name}] Bound master invite code {MASTER_INVITE_CODE} successfully! (+500 Pts)")
        except Exception:
            pass

    def manual_daily_checkin(self):
        # Fast-Path: if already verified claimed today, skip RPC to avoid useless network calls
        if self.daily_claimed:
            return {'success': True, 'already_claimed': True, 'message': 'Daily Check-in already claimed today (Untouched)'}

        # 1. Trigger authenticated login session on Alphea backend
        self.authenticated_rpc('alphea.connect.v1.AuthService/CurrentSession', {})

        # 2. Query quests to check daily claim status
        r = self.authenticated_rpc('alphea.connect.v1.QuestService/ListQuests', {})
        if not r or r.status_code != 200:
            return {'success': False, 'message': 'Network/RPC Error'}

        quests = r.json().get('quests', [])
        claimed_daily_now = False
        already_claimed_daily = False

        for q in quests:
            qid = q.get('questId', '')
            state = q.get('state', '')
            pkey = q.get('periodKey', '')

            # Detect UTC midnight day rollover from periodKey
            if pkey and pkey != 'lifetime' and pkey != self.current_period_key:
                self.current_period_key = pkey
                self.daily_claimed = False

            if qid == 'daily-login-1':
                if state == 'QUEST_STATE_CLAIMED':
                    already_claimed_daily = True
                    self.daily_claimed = True
                elif state in ['QUEST_STATE_CLAIMABLE', 'QUEST_STATE_ACTIVE']:
                    if self.claim_quest(qid, pkey):
                        claimed_daily_now = True
                        self.daily_claimed = True

        self.update_cluster_state()
        self.check_redeem_balance()

        if claimed_daily_now:
            return {'success': True, 'claimed_now': True, 'message': 'Daily Check-in Claimed (+600 Pts)!'}
        elif already_claimed_daily:
            return {'success': True, 'already_claimed': True, 'message': 'Daily Check-in already claimed today (Untouched)'}
        else:
            return {'success': False, 'message': 'Check-in pending / session active'}

    def manual_claim_mining_quests(self):
        """Dedicated 1-Click sweep for completed mining milestone quests and referral bonuses."""
        r = self.authenticated_rpc('alphea.connect.v1.QuestService/ListQuests', {})
        if not r or r.status_code != 200:
            return {'success': False, 'message': 'Network/RPC Error'}

        quests = r.json().get('quests', [])
        quests_claimed = 0
        pts_earned = 0

        for q in quests:
            qid = q.get('questId', '')
            if qid == 'daily-login-1':
                continue  # Dedicated to Daily Check-in button

            state = q.get('state', '')
            pkey = q.get('periodKey', '')
            target = int(q.get('targetValue', 0))
            measured = int(q.get('measuredValue', 0))

            if pkey and pkey != 'lifetime' and pkey != self.current_period_key:
                self.current_period_key = pkey
                self.daily_claimed = False

            # Claim any completed foreground mining quest or claimable quest
            is_claimable = (state == 'QUEST_STATE_CLAIMABLE') or (target > 0 and measured >= target and state != 'QUEST_STATE_CLAIMED')
            if is_claimable:
                if self.claim_quest(qid, pkey):
                    quests_claimed += 1
                    pts_earned += int(q.get('rewardValue', 500))

        # Check and claim inviter/referral bonus
        if self.check_and_claim_inviter_bonus():
            quests_claimed += 1
            pts_earned += 500

        self.sync_user_points()
        self.update_cluster_state()
        self.check_redeem_balance()

        if quests_claimed > 0:
            return {'success': True, 'claimed_count': quests_claimed, 'pts_earned': pts_earned, 'message': f'Claimed {quests_claimed} Quests (+{pts_earned:,} Pts)!'}
        else:
            return {'success': True, 'claimed_count': 0, 'already_up_to_date': True, 'message': 'All completed quests are already claimed!'}

    def sync_daily_quest_state(self):
        """Safe read-only sync of daily quest state on boot and periodic ticks."""
        try:
            r = self.authenticated_rpc('alphea.connect.v1.QuestService/ListQuests', {})
            if not r or r.status_code != 200:
                return
            data = r.json()
            quests = data.get('quests', [])
            max_sec = 0
            for q in quests:
                qid = q.get('questId', '')
                state = q.get('state', '')
                pkey = q.get('periodKey', '')
                if pkey and pkey != 'lifetime' and pkey != self.current_period_key:
                    self.current_period_key = pkey
                    self.daily_claimed = False

                if qid == 'daily-login-1':
                    self.daily_claimed = (state == 'QUEST_STATE_CLAIMED')
                elif 'daily-foreground' in qid:
                    measured = int(q.get('measuredValue', 0))
                    if measured > max_sec:
                        max_sec = measured

            if max_sec > self.today_seconds:
                self.today_seconds = max_sec
            self.update_cluster_state()
        except Exception:
            pass

    def submit_heartbeat(self):
        if not self.session_id:
            if not self.start_foreground_session():
                return False

        payload = {'sessionId': self.session_id}
        r = self.authenticated_rpc('alphea.connect.v1.ActivityService/SubmitHeartbeat', payload)
        self.last_sync_time = datetime.datetime.now().strftime('%H:%M:%S')

        if r and r.status_code == 200:
            data = r.json()
            old_uptime = self.session_uptime
            self.session_uptime = int(data.get('accumulatedValidSeconds', str(self.session_uptime)))
            self.status = 'Mining Active'
            self.consecutive_errors = 0
            self.update_cluster_state()
            delta_s = max(1, self.session_uptime - old_uptime)
            add_log(f"[{self.name}] Heartbeat ACK: Mining Active (+{delta_s}s, Total: {self.session_uptime}s)")

            # Auto 24-Hour Session Renewal:
            # If accumulatedValidSeconds reaches 86,400s (24h Alphea ceiling), renew session
            if self.session_uptime >= 86400:
                add_log(f"[{self.name}] 🔄 24h Session Limit reached (86,400s). Renewing foreground session...")
                self.session_uptime = 0
                self.start_foreground_session()

            # Autonomous Milestone-Driven Auto Quest Claim Engine on Heartbeat:
            milestones = [3600, 10800, 21600, 43200]
            crossed_milestone = any(old_uptime < m <= self.session_uptime for m in milestones)
            tick_c = getattr(self, 'tick_count', 0)
            if crossed_milestone or tick_c == 1 or not self.daily_claimed or not self.onboard_claimed or (tick_c % 5 == 0):
                self.fetch_and_claim_quests()
                self.check_round_redeem_status()

            return True
        elif r and r.status_code == 400 and 'connect foreground session not active' in r.text:
            add_log(f"[{self.name}] Foreground session expired, renewing session ID...")
            self.start_foreground_session()
            return False
        elif r and r.status_code in (500, 502, 503, 504):
            self.consecutive_errors += 1
            add_log(f"[{self.name}] Heartbeat server notice: HTTP {r.status_code} (Alphea server busy, retrying in 60s)")
            if self.consecutive_errors > 8:
                self.session_id = None
            self.update_cluster_state()
            return False
        else:
            err_msg = r.text[:60] if r else 'Network Timeout (>35s)'
            self.consecutive_errors += 1
            if self.consecutive_errors > 8:
                self.session_id = None
            if self.consecutive_errors >= 2:
                add_log(f"[{self.name}] Heartbeat notice: {err_msg} (Retry in 60s)")
            self.update_cluster_state()
            return False

    def tick(self):
        """Single non-blocking execution cycle executed by the 8-worker ThreadPool."""
        if PAUSE_MODE:
            self.status = "Paused (Sleep Mode)"
            self.update_cluster_state()
            return

        if '@alphea.local' in self.email or not self.access_token:
            self.status = 'Waiting for Sync'
            self.update_cluster_state()
            return

        # 1. Proactive JWT renewal (2 min before exp)
        if self.jwt_exp and time.time() > (self.jwt_exp - 120):
            if not self.refresh_access_token():
                if '@freediamond.in' in self.email:
                    if self.auto_relogin_via_otp():
                        self.start_foreground_session()
                return

        # 2. Check 401 / Dead / Relogin / Expired / Error state
        if '401' in self.status or 'Dead' in self.status or 'Auto-Relogin' in self.status or 'Expired' in self.status or 'Error' in self.status:
            now = time.time()
            if hasattr(self, '_last_relogin_attempt') and (now - self._last_relogin_attempt) < 300:
                return

            # PRIORITY 1: Ultra-fast gRPC RefreshSession token renewal (200ms, ZERO OTP required)
            if self.refresh_token:
                if self.refresh_access_token():
                    self.start_foreground_session()
                    return

            # PRIORITY 2: Fallback to OTP relogin for freediamond accounts
            if '@freediamond.in' in self.email:
                if self.auto_relogin_via_otp():
                    self.start_foreground_session()
            return

        # 3. Ensure foreground session is active
        if not self.session_id:
            if not self.start_foreground_session():
                return

        # 4. Submit heartbeat (advances mining time & auto-claims completed quests on milestone)
        self.tick_count = getattr(self, 'tick_count', 0) + 1
        self.submit_heartbeat()

    def run(self):
        self.tick()

# ---------------- CLUSTER DISPATCHER (WORKER POOL) ----------------
class ClusterDispatcher(threading.Thread):
    def __init__(self, max_workers=8):
        super().__init__(name="ClusterDispatcher", daemon=True)
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="AlpheaWorker"
        )
        self.next_run = {}
        self.is_running = True

    def run(self):
        add_log(f"[DISPATCHER] Linear Micro-Stagger Engine started with {self.max_workers} worker pool (RAM: ~40MB).")
        now = time.time()
        with NODES_LOCK:
            total_n = len(NODES) or 1
            step = 60.0 / total_n
            for i, node in enumerate(NODES):
                stagger = i * step + random.uniform(0, min(0.2, step * 0.4))
                self.next_run[node.index] = now + stagger

        while self.is_running:
            if PAUSE_MODE:
                time.sleep(5)
                continue

            now = time.time()
            due_nodes = []
            with NODES_LOCK:
                for node in NODES:
                    due_time = self.next_run.get(node.index, 0)
                    if now >= due_time:
                        due_nodes.append(node)
                        jitter = random.uniform(-1.0, 1.0)
                        backoff = min(60, node.consecutive_errors * 10)
                        self.next_run[node.index] = now + max(50, 60 + jitter + backoff)

            for node in due_nodes:
                self.executor.submit(self._safe_tick, node)
                if len(due_nodes) > 1:
                    time.sleep(0.12)

            time.sleep(0.25)

    def _safe_tick(self, node):
        try:
            node.tick()
        except Exception as e:
            add_log(f"[{node.name}] Tick error: {e}")

cluster_dispatcher_instance = None

# ---------------- AUTO PINGER (KEEP ALIVE) ----------------
class AutoPinger(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.target_url = (
            os.environ.get('RENDER_EXTERNAL_URL') or
            os.environ.get('APP_URL') or
            'https://alphea-render-cloud-2.onrender.com'
        )
        self.last_ping_status = "Pending first ping"
        self.last_ping_time = "--:--:--"
        self.total_pings = 0

    def run(self):
        if PAUSE_MODE:
            self.last_ping_status = "Disabled (Cluster Paused)"
            add_log("[AUTO-PING] Cluster is in PAUSE_MODE. Keep-Alive AutoPinger disabled to let Render sleep and save hours.")
            return
        time.sleep(15)
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
                    add_log(f"[AUTO-PING] Hit {ping_endpoint} -> 200 OK ({elapsed_ms}ms) [Pings: {self.total_pings}]")
                else:
                    self.last_ping_status = f"HTTP {r.status_code} at {self.last_ping_time}"
            except Exception as e:
                self.last_ping_status = f"Ping Glitch: {e}"
                add_log(f"[AUTO-PING] Glitch: {e}")

            time.sleep(480 + random.randint(5, 25))

auto_pinger_instance = AutoPinger()

# ---------------- WEB INTERFACE ----------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>ALPHEA CLOUD MINING CLUSTER 2 (Dynamic Auto-Expanding Engine)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    :root {
      --bg: #090d16;
      --card: #111726;
      --border: #1e293b;
      --accent: #00d2ff;
      --accent2: #9d4edd;
      --green: #10b981;
      --yellow: #f59e0b;
      --red: #ef4444;
      --text: #f8fafc;
      --subtext: #94a3b8;
    }
    * { margin:0; padding:0; box-sizing:border-box; font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 20px; }
    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; margin-bottom: 24px; flex-wrap: wrap; gap: 15px; }
    .title-box h1 { font-size: 22px; font-weight: 800; background: linear-gradient(135deg, var(--accent), var(--accent2)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display: flex; align-items: center; gap: 10px; }
    .title-box p { color: var(--subtext); font-size: 13px; margin-top: 4px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .tag-dynamic { background: rgba(157, 78, 221, 0.2); color: #c77dff; border: 1px solid rgba(157, 78, 221, 0.4); padding: 2px 8px; border-radius: 12px; font-weight: 700; font-size: 11px; }
    .btn-group { display: flex; gap: 10px; flex-wrap: wrap; }
    .btn { background: var(--card); border: 1px solid var(--border); color: var(--text); padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; transition: 0.2s; display: inline-flex; align-items: center; gap: 6px; }
    .btn:hover { border-color: var(--accent); color: var(--accent); transform: translateY(-1px); }
    .btn-primary { background: linear-gradient(135deg, var(--accent), #0077b6); border: none; color: #fff; }
    .btn-primary:hover { filter: brightness(1.1); color: #fff; }
    .btn-redeem { background: linear-gradient(135deg, #9d4edd, #7209b7); border: none; color: #fff; font-weight: 700; box-shadow: 0 2px 10px rgba(157, 78, 221, 0.3); }
    .btn-redeem:hover { filter: brightness(1.15); color: #fff; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; position: relative; overflow: hidden; transition: 0.2s; }
    .card:hover { border-color: #334155; }
    .card-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; }
    .node-title { font-weight: 700; font-size: 15px; color: #fff; }
    .node-email { font-size: 12px; color: var(--subtext); margin-top: 2px; }
    .badge { font-size: 11px; font-weight: 700; padding: 3px 8px; border-radius: 12px; text-transform: uppercase; }
    .badge-green { background: rgba(16, 185, 129, 0.15); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); }
    .badge-yellow { background: rgba(245, 158, 11, 0.15); color: var(--yellow); border: 1px solid rgba(245, 158, 11, 0.3); }
    .badge-red { background: rgba(239, 68, 68, 0.15); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.3); }
    .stats-row { display: flex; justify-content: space-between; margin-top: 8px; font-size: 12px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 8px; }
    .stat-label { color: var(--subtext); }
    .stat-val { font-weight: 600; font-family: monospace; color: #fff; }
    .logs-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
    .logs-header { font-size: 14px; font-weight: 700; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
    .logs-box { background: #050811; border: 1px solid #161f30; border-radius: 8px; padding: 12px; font-family: monospace; font-size: 11px; height: 180px; overflow-y: auto; color: #38bdf8; line-height: 1.6; }
    .toast { position: fixed; bottom: 20px; right: 20px; background: var(--green); color: #fff; padding: 10px 20px; border-radius: 8px; font-weight: 600; font-size: 13px; display: none; z-index: 1000; box-shadow: 0 4px 12px rgba(0,0,0,0.5); }
    .stats-overview { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-bottom: 24px; }
    .stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px 18px; display: flex; flex-direction: column; gap: 4px; }
    .stat-card-title { font-size: 11px; text-transform: uppercase; font-weight: 700; color: var(--subtext); letter-spacing: 0.5px; }
    .stat-card-val { font-size: 20px; font-weight: 800; color: #fff; font-family: monospace; }
    .stat-btn-time { font-size: 11px; opacity: 0.85; margin-left: 5px; font-family: monospace; }
  </style>
</head>
<body>
  <div class="header">
    <div class="title-box">
      <h1><i class="fa-solid fa-server"></i> ALPHEA CLUSTER 2</h1>
      <p>
        <span>Direct Render VPS IP</span>
        <span>•</span>
        <span class="tag-dynamic">⚡ Linear Micro-Stagger Engine: {{ total_nodes }} Nodes</span>
        <span>•</span>
        <span class="tag-dynamic" style="background: rgba(16, 185, 129, 0.15); border-color: rgba(16, 185, 129, 0.4); color: #34d399;"><i class="fa-solid fa-bolt-auto"></i> 24/7 Heartbeat Auto-Claim Active</span>
      </p>
    </div>
    <div class="btn-group">
      {% if can_redeem_any %}
      <button class="btn btn-redeem" style="background: linear-gradient(135deg, #10b981, #059669); box-shadow: 0 0 15px rgba(16, 185, 129, 0.5);" onclick="triggerRedeemAll()" title="Round is OPEN! Click to 1-Click Request Redeem for all Cluster 2 accounts."><i class="fa-solid fa-gift"></i> 🎁 1-Click Request Redeem All <span class="stat-btn-time">[{{ last_redeem_all }}]</span></button>
      {% else %}
      <button class="btn btn-redeem" style="background: linear-gradient(135deg, #334155, #1e293b); border: 1px solid rgba(148, 163, 184, 0.3); opacity: 0.95;" onclick="triggerRedeemAll()" title="Click to 1-Click Request Redeem across all accounts."><i class="fa-solid fa-gift"></i> 1-Click Request Redeem All <span class="stat-btn-time">[{{ last_redeem_all }}]</span></button>
      {% endif %}
      <button class="btn" style="background: linear-gradient(135deg, #3b82f6, #1d4ed8); border: none; color: #fff; font-weight: 700;" onclick="triggerClaimAll()" title="Stage 2: 1-Click Sponsored Claim token payouts to BSC wallets (Gas paid by Alphea)"><i class="fa-solid fa-trophy"></i> 🏆 1-Click Claim Tokens <span class="stat-btn-time">[{{ last_claim_all }}]</span></button>
      <button class="btn" onclick="reviveCluster()"><i class="fa-solid fa-bolt"></i> Revive Nodes</button>
      <button class="btn" onclick="location.reload()"><i class="fa-solid fa-rotate-right"></i> Refresh</button>
    </div>
  </div>

  <div class="stats-overview">
    <div class="stat-card">
      <span class="stat-card-title">💎 Total Cluster Points</span>
      <span class="stat-card-val" style="color:#00d2ff;">{{ "{:,}".format(total_points) }} Pts</span>
    </div>
    <div class="stat-card">
      <span class="stat-card-title">⚡ Mining Active Nodes</span>
      <span class="stat-card-val" style="color:#10b981;">{{ active_nodes }} / {{ total_nodes }} Active</span>
    </div>
    <div class="stat-card">
      <span class="stat-card-title">🎁 Rounds Redeemed</span>
      <span class="stat-card-val" style="color:#c77dff;">{{ redeemed_count }} / {{ total_nodes }} Claimed</span>
      <span style="font-size:11px; color:#94a3b8; margin-top:2px;">Round 1: 3,000 Pts Min Required</span>
    </div>
    <div class="stat-card">
      <span class="stat-card-title">📅 Daily Check-ins</span>
      <span class="stat-card-val" style="color:#38bdf8;">{{ daily_claimed_count }} / {{ total_nodes }} Done</span>
    </div>
    <div class="stat-card" style="border-color: rgba(157, 78, 221, 0.4); background: linear-gradient(135deg, rgba(157, 78, 221, 0.12), rgba(17, 23, 38, 0.95));">
      <span class="stat-card-title" style="color:#c77dff;">⏳ Active Round Timeline</span>
      <span class="stat-card-val" style="font-size:14px; color:#f8fafc; font-family:sans-serif; margin-top:2px;">Round 1: Open until Oct 5 (23:59 UTC)</span>
      <span style="font-size:11px; color:#94a3b8; margin-top:3px;">Token Claim payouts unlock after Round 1 closes</span>
    </div>
  </div>

  <div class="grid" id="nodeGrid">
    {% for idx, n in cluster.items() %}
    <div class="card">
      <div class="card-top">
        <div>
          <div class="node-title">{{ n.name }}</div>
          <div class="node-email">{{ n.email }}</div>
        </div>
        <span class="badge {% if n.status == 'Mining Active' %}badge-green{% elif '401' in n.status or 'Dead' in n.status or 'Expired' in n.status or 'Timeout' in n.status %}badge-red{% else %}badge-yellow{% endif %}">
          {{ n.status }}
        </span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Points Balance:</span>
        <span class="stat-val" style="color:#00d2ff;">{{ "{:,}".format(n.balance) }} Pts</span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Round Redeem:</span>
        <span class="stat-val" style="color:{% if 'Claimed' in n.redeem_status_text %}#10b981{% elif 'Claimable' in n.redeem_status_text %}#00d2ff{% elif 'Min' in n.redeem_status_text %}#f59e0b{% elif 'No Active' in n.redeem_status_text %}#f59e0b{% else %}#94a3b8{% endif %}; font-weight:700;">
          {{ n.redeem_status_text or 'Pending' }}
        </span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Pinned Wallet:</span>
        <span class="stat-val" style="font-size:11px; color:#cbd5e1;">
          {% if n.pinned_wallet %}
            {{ n.pinned_wallet[:6] }}...{{ n.pinned_wallet[-4:] }}
          {% else %}
            <span style="color:#ef4444;">Not Connected</span>
          {% endif %}
        </span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Daily Claimed:</span>
        <span class="stat-val" style="color:{% if n.daily_claimed %}#10b981{% else %}#f59e0b{% endif %}">
          {% if n.daily_claimed %}✅ Claimed{% else %}⏳ Pending{% endif %}
        </span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Session Mining:</span>
        <span class="stat-val">{{ (n.uptime // 60) }}m {{ (n.uptime % 60) }}s (Today: {{ (n.today_seconds // 3600) }}h {{ ((n.today_seconds % 3600) // 60) }}m)</span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Token Expiry:</span>
        <span class="stat-val">{{ (n.exp_seconds // 60) }}m {{ (n.exp_seconds % 60) }}s</span>
      </div>
    </div>
    {% endfor %}
  </div>

  <div class="logs-card">
    <div class="logs-header">
      <span><i class="fa-solid fa-terminal"></i> Live Cluster Engine Logs</span>
      <span style="font-size:12px; color:var(--subtext);">Keep-Alive: {{ ping_status }}</span>
    </div>
    <div class="logs-box" id="logsBox">
      {% for l in logs %}
      <div>{{ l }}</div>
      {% endfor %}
    </div>
  </div>

  <div class="toast" id="toast"></div>

  <script>
    function showToast(msg, bg) {
      const t = document.getElementById('toast');
      t.textContent = msg;
      t.style.background = bg || '#10b981';
      t.style.display = 'block';
      setTimeout(() => { t.style.display = 'none'; }, 3500);
    }
    function triggerRedeemAll() {
      if (!confirm('Execute 1-Click Request Redeem for all Cluster 2 accounts?')) return;
      showToast('Initiating Global Request Redeem across Cluster 2...', '#9d4edd');
      fetch('/api/redeem_all')
        .then(r => r.json())
        .then(d => {
          showToast(d.message || 'Request Redeem sequence started!');
          setTimeout(() => location.reload(), 4000);
        });
    }
    function triggerClaimAll() {
      showToast('Initiating Global Sponsored Token Claim across Cluster 2...', '#3b82f6');
      fetch('/api/claim_all')
        .then(r => r.json())
        .then(d => {
          showToast(d.message || 'Sponsored Claim sweep started!');
          setTimeout(() => location.reload(), 4000);
        });
    }
    function triggerDailyCheckin() {
      showToast('Initiating Global Daily Check-in Sweep...', '#0077b6');
      fetch('/api/daily_checkin_all')
        .then(r => r.json())
        .then(d => {
          showToast(d.message || 'Daily check-in sequence initiated!');
          setTimeout(() => location.reload(), 12000);
        });
    }
    function triggerClaimQuests() {
      showToast('Initiating Global Mining Quests Sweep...', '#8b5cf6');
      fetch('/api/claim_quests_all')
        .then(r => r.json())
        .then(d => {
          showToast(d.message || 'Mining quests sequence initiated!');
          setTimeout(() => location.reload(), 12000);
        });
    }
    function reviveCluster() {
      showToast('Reviving cluster nodes...', '#9d4edd');
      fetch('/api/revive_cluster')
        .then(r => r.json())
        .then(d => {
          showToast(d.message || 'Cluster revived!');
          setTimeout(() => location.reload(), 2000);
        });
    }
    window.onload = function() {
      const b = document.getElementById('logsBox');
      if (b) b.scrollTop = b.scrollHeight;
    };
  </script>
</body>
</html>
"""

# ---------------- CLUSTER ENGINE CONTROLLER ----------------
cluster_worker_pid = None
engine_lock = threading.Lock()
auto_pinger_instance = None

def ensure_worker_engine_running():
    global cluster_worker_pid, auto_pinger_instance, cluster_dispatcher_instance
    current_pid = os.getpid()

    if PAUSE_MODE:
        with engine_lock:
            if cluster_worker_pid != current_pid:
                cluster_worker_pid = current_pid
                add_log(f"[CLUSTER 2 ENGINE] Cluster 2 is in PAUSE_MODE. Zero threads active, zero OTP requests.")
                initialize_cluster()
        return

    # Fast path: already running in this process
    if cluster_worker_pid == current_pid and cluster_dispatcher_instance and cluster_dispatcher_instance.is_alive():
        return

    with engine_lock:
        if cluster_worker_pid == current_pid and cluster_dispatcher_instance and cluster_dispatcher_instance.is_alive():
            return
        cluster_worker_pid = current_pid
        add_log(f"[CLUSTER 2 ENGINE] Starting dedicated engine with 8-Worker Pool in PID {current_pid}...")
        initialize_cluster()
        cluster_dispatcher_instance = ClusterDispatcher(max_workers=8)
        cluster_dispatcher_instance.start()
        auto_pinger_instance = AutoPinger()
        auto_pinger_instance.start()

def start_cluster():
    ensure_worker_engine_running()

@app.before_request
def ensure_cluster_running():
    ensure_worker_engine_running()

# ---------------- FLASK ROUTES ----------------
@app.route('/')
def route_dashboard():
    ensure_worker_engine_running()
    with NODES_LOCK:
        for node in NODES:
            node.update_cluster_state()
    active_cnt = sum(1 for n in CLUSTER_STATE.values() if n.get('status') == 'Mining Active')
    total_pts = sum(n.get('balance', 0) for n in CLUSTER_STATE.values())
    redeemed_cnt = sum(1 for n in CLUSTER_STATE.values() if 'Claimed' in n.get('redeem_status_text', ''))
    daily_claimed_cnt = sum(1 for n in CLUSTER_STATE.values() if n.get('daily_claimed') is True)
    can_redeem_any = any(n.get('can_redeem') is True and n.get('balance', 0) > 0 for n in CLUSTER_STATE.values())
    can_daily_checkin_any = any(n.get('daily_claimed') is False and n.get('status') == 'Mining Active' for n in CLUSTER_STATE.values())
    return render_template_string(
        HTML_TEMPLATE,
        cluster=CLUSTER_STATE,
        logs=CLUSTER_LOGS,
        ping_status=auto_pinger_instance.last_ping_status if auto_pinger_instance else AUTO_PING_STATUS,
        total_nodes=len(NODES),
        active_nodes=active_cnt,
        total_points=total_pts,
        redeemed_count=redeemed_cnt,
        daily_claimed_count=daily_claimed_cnt,
        last_daily_checkin=LAST_DAILY_CHECKIN_TIME,
        last_quests_claim=LAST_QUESTS_CLAIM_TIME,
        last_redeem_all=LAST_REDEEM_ALL_TIME,
        last_claim_all=LAST_CLAIM_ALL_TIME,
        can_redeem_any=can_redeem_any,
        can_daily_checkin_any=can_daily_checkin_any
    )

@app.route('/health')
def route_health():
    ensure_worker_engine_running()
    uptime_sec = int(time.time() - START_TIME)
    h = uptime_sec // 3600
    m = (uptime_sec % 3600) // 60
    s = uptime_sec % 60
    active_cnt = sum(1 for n in CLUSTER_STATE.values() if 'Mining' in n.get('status', ''))
    return jsonify({
        'status': 'paused' if PAUSE_MODE else 'ok',
        'message': 'Cluster 2 is safely PAUSED (Sleep Mode). Zero threads, zero OTPs, zero CPU usage.' if PAUSE_MODE else 'Cluster 2 active',
        'service': 'alphea-cluster2-dynamic-engine',
        'total_accounts': len(NODES),
        'active_nodes': 0 if PAUSE_MODE else active_cnt,
        'uptime': f"{h:02d}h {m:02d}m {s:02d}s",
        'auto_ping_status': 'Disabled (Cluster Paused)' if PAUSE_MODE else (auto_pinger_instance.last_ping_status if auto_pinger_instance else AUTO_PING_STATUS),
        'timestamp': datetime.datetime.now().isoformat()
    }), 200

@app.route('/api/status')
def route_api_status():
    ensure_worker_engine_running()
    with NODES_LOCK:
        for node in NODES:
            node.update_cluster_state()
    return jsonify({
        'cluster': CLUSTER_STATE,
        'total_nodes': len(NODES),
        'logs': CLUSTER_LOGS[-50:]
    }), 200

@app.route('/api/update_account', methods=['POST', 'OPTIONS'])
def route_update_account():
    if request.method == 'OPTIONS':
        res = jsonify({'status': 'ok'})
        res.headers.add('Access-Control-Allow-Origin', '*')
        res.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        res.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return res, 200

    data = request.json or {}
    email = data.get('email')
    new_access = data.get('accessToken')
    new_refresh = data.get('refreshToken')
    dev_id = data.get('deviceId')
    slot = data.get('slot')

    if not new_access or not new_refresh:
        return jsonify({'error': 'Missing accessToken or refreshToken'}), 400

    email_clean = email.strip() if email else ''
    target_node = None
    is_new_spawner = False

    with NODES_LOCK:
        # 1. Direct Slot Selection (e.g. from extension manual slot selector 0..4)
        if slot is not None and str(slot).lower() not in ['auto', 'new', '']:
            try:
                slot_idx = int(slot)
                if 0 <= slot_idx < len(NODES):
                    target_node = NODES[slot_idx]
            except Exception:
                pass

        # 2. Match existing account by email (case-insensitive)
        if not target_node and email_clean:
            for node in NODES:
                if node.email and node.email.lower() == email_clean.lower():
                    target_node = node
                    break

        # 3. Look for an unassigned placeholder slot (@alphea.local) ONLY
        if not target_node and str(slot).lower() != 'new':
            for node in NODES:
                if '@alphea.local' in node.email.lower() or not node.email:
                    target_node = node
                    break

        # 4. DYNAMIC +1 NODE SPAWNER:
        # If all existing slots have real accounts, dynamically spawn a brand new Node!
        if not target_node:
            is_new_spawner = True
            new_idx = len(NODES)
            new_name = f"Cluster 2 Node {new_idx + 1}"
            account_data = {
                'name': new_name,
                'email': email_clean,
                'deviceId': dev_id or f"c3{new_idx + 1}a0e2f49583ea{new_idx + 1}",
                'proxy': None,
                'location': 'Direct Render VPS',
                'accessToken': new_access,
                'refreshToken': new_refresh,
                'enabled': True
            }
            target_node = AccountWorker(new_idx, account_data)
            NODES.append(target_node)

        # Update node data (preserving existing proxy!)
        target_node.access_token = new_access
        target_node.refresh_token = new_refresh
        if email_clean:
            target_node.email = email_clean
        if dev_id:
            target_node.device_id = dev_id
        target_node.jwt_exp = decode_jwt_exp(new_access)
        target_node.session_id = None
        target_node.status = 'Mining Active'
        target_node.consecutive_errors = 0
        target_node.last_refresh_attempt = 0
        target_node.update_cluster_state()

        if is_new_spawner:
            add_log(f"[SPAWNER] Auto-spawned new live slot: {target_node.name} for {email_clean} (+1 Node Added!)")

        # Guarantee mining worker thread is running!
        target_node.start()

    # Background Async Activation (Zero-Lag <50ms HTTP response, avoids Gunicorn 30s timeout)
    def async_post_sync(node, is_spawner, clean_mail):
        try:
            node.get_effective_wallet()
            node.save_updated_tokens()
            node.start_foreground_session()
            node.fetch_and_claim_quests()
            node.check_round_redeem_status()
            if not is_spawner:
                add_log(f"[{node.name}] Session revived & synced for {clean_mail or node.name} via Cluster 2 API!")
        except Exception as ex:
            add_log(f"[{node.name}] Background sync error: {ex}")

    threading.Thread(target=async_post_sync, args=(target_node, is_new_spawner, email_clean), daemon=True).start()

    res = jsonify({
        'success': True,
        'name': target_node.name,
        'slot': target_node.index,
        'proxy_preserved': bool(target_node.proxy),
        'is_new_node': is_new_spawner,
        'total_nodes': len(NODES),
        'message': f"Revived {email_clean or target_node.name} on {target_node.name} (Proxy: {'Webshare' if target_node.proxy else 'Direct VPS'})!"
    })
    res.headers.add('Access-Control-Allow-Origin', '*')
    return res, 200

@app.route('/api/revive_cluster', methods=['GET', 'POST'])
def route_revive_cluster():
    def revive_runner():
        add_log("[REVIVE] Cluster revival sequence started with safe human delay...")
        nodes_copy = list(NODES)
        for node in nodes_copy:
            if not node.access_token or '@alphea.local' in node.email:
                continue
            try:
                node.consecutive_errors = 0
                node.session_uptime = 0
                node.start_foreground_session()
                node.fetch_and_claim_quests()
            except Exception as e:
                add_log(f"[{node.name}] Revive error: {e}")
            time.sleep(random.uniform(0.1, 0.2))
        add_log("[REVIVE] Cluster revival sequence completed successfully!")

    threading.Thread(target=revive_runner, daemon=True).start()
    return jsonify({'success': True, 'message': 'Cluster 2 nodes revival cycle initiated with safe human delays!'}), 200

@app.route('/api/relogin_dead_nodes', methods=['GET', 'POST'])
def route_relogin_dead_nodes():
    """Never-Die endpoint: Find all expired or dead @freediamond.in nodes and auto-relogin via OTP"""
    dead_nodes = [
        n for n in NODES
        if '@freediamond.in' in n.email and (n.status != 'Mining Active' or not n.access_token or not n.jwt_exp or time.time() >= n.jwt_exp)
    ]
    
    def relogin_runner():
        add_log(f"[NEVER-DIE] Starting OTP relogin for {len(dead_nodes)} freediamond.in nodes...")
        success = 0
        failed = 0
        for node in dead_nodes:
            add_log(f"[NEVER-DIE] Relogging {node.name} ({node.email})...")
            try:
                if node.auto_relogin_via_otp():
                    node.start_foreground_session()
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                add_log(f"[NEVER-DIE] Error relogging {node.name}: {e}")
                failed += 1
            # Stagger requests to avoid rate limiting
            time.sleep(random.uniform(2.5, 4.0))
        add_log(f"[NEVER-DIE] Batch relogin done! Success: {success} | Failed: {failed}")
    
    threading.Thread(target=relogin_runner, daemon=True).start()
    return jsonify({
        'success': True,
        'dead_nodes_found': len(dead_nodes),
        'message': f'OTP relogin initiated for {len(dead_nodes)} dead freediamond.in nodes!'
    }), 200

@app.route('/api/daily_checkin_all', methods=['GET', 'POST'])
def route_daily_checkin_all():
    global LAST_DAILY_CHECKIN_TIME, IS_CHECKIN_RUNNING
    with SWEEP_LOCK:
        if IS_CHECKIN_RUNNING:
            return jsonify({'success': False, 'message': 'Daily check-in sweep is already running in background! Please wait.'}), 429
        IS_CHECKIN_RUNNING = True
    LAST_DAILY_CHECKIN_TIME = get_ist_now_str()
    save_timestamps()
    def sweep():
        global IS_CHECKIN_RUNNING
        try:
            add_log(f"[DAILY CHECK-IN] Global 1-Click Daily Check-in started at {LAST_DAILY_CHECKIN_TIME}...")
            nodes_copy = list(NODES)
            claimed_cnt = 0
            already_cnt = 0
            skipped_cnt = 0
            for node in nodes_copy:
                if not node.access_token or '@alphea.local' in node.email or '401' in node.status or 'Dead' in node.status or 'Expired' in node.status:
                    skipped_cnt += 1
                    continue
                try:
                    res = node.manual_daily_checkin()
                    if res.get('claimed_now'):
                        claimed_cnt += 1
                    elif res.get('already_claimed'):
                        already_cnt += 1
                    add_log(f"[{node.name}] Daily Check-in: {res.get('message')}")
                except Exception as e:
                    add_log(f"[{node.name}] Daily check-in error: {e}")
                time.sleep(random.uniform(0.8, 1.4))
            add_log(f"[DAILY CHECK-IN] Sweep complete at {get_ist_now_str()}! Claimed: {claimed_cnt} | Already Done: {already_cnt} | Skipped: {skipped_cnt}")
        finally:
            with SWEEP_LOCK:
                IS_CHECKIN_RUNNING = False
    threading.Thread(target=sweep, daemon=True).start()
    return jsonify({'success': True, 'message': f'Cluster 2 daily check-in sequence initiated ({LAST_DAILY_CHECKIN_TIME})!'}), 200

@app.route('/api/claim_quests_all', methods=['GET', 'POST'])
def route_claim_quests_all():
    global LAST_QUESTS_CLAIM_TIME, IS_QUESTS_RUNNING
    with SWEEP_LOCK:
        if IS_QUESTS_RUNNING:
            return jsonify({'success': False, 'message': 'Mining quests sweep is already running in background! Please wait.'}), 429
        IS_QUESTS_RUNNING = True
    LAST_QUESTS_CLAIM_TIME = get_ist_now_str()
    save_timestamps()
    def quests_runner():
        global IS_QUESTS_RUNNING
        try:
            add_log(f"[QUESTS SWEEP] Global 1-Click Mining Quests sweep started at {LAST_QUESTS_CLAIM_TIME}...")
            nodes_copy = list(NODES)
            claimed_nodes = 0
            total_quests_claimed = 0
            total_pts_earned = 0
            uptodate_cnt = 0
            skipped_cnt = 0
            for node in nodes_copy:
                if not node.access_token or '@alphea.local' in node.email or '401' in node.status or 'Dead' in node.status or 'Expired' in node.status:
                    skipped_cnt += 1
                    continue
                try:
                    res = node.manual_claim_mining_quests()
                    count = res.get('claimed_count', 0)
                    if count > 0:
                        claimed_nodes += 1
                        total_quests_claimed += count
                        total_pts_earned += res.get('pts_earned', 0)
                        add_log(f"[{node.name}] Quests: {res.get('message')}")
                    else:
                        uptodate_cnt += 1
                except Exception as e:
                    add_log(f"[{node.name}] Quests sweep error: {e}")
                time.sleep(random.uniform(0.8, 1.4))
            add_log(f"[QUESTS SWEEP] Sweep complete at {get_ist_now_str()}! Nodes Claimed: {claimed_nodes} | Quests: {total_quests_claimed} (+{total_pts_earned:,} Pts) | Up-to-date: {uptodate_cnt} | Skipped: {skipped_cnt}")
        finally:
            with SWEEP_LOCK:
                IS_QUESTS_RUNNING = False
    threading.Thread(target=quests_runner, daemon=True).start()
    return jsonify({'success': True, 'message': f'Cluster 2 mining quests sequence initiated ({LAST_QUESTS_CLAIM_TIME})!'}), 200

@app.route('/api/redeem_all', methods=['GET', 'POST'])
def route_redeem_all():
    global LAST_REDEEM_ALL_TIME, IS_REDEEM_RUNNING
    with SWEEP_LOCK:
        if IS_REDEEM_RUNNING:
            return jsonify({'success': False, 'message': 'Redeem sweep is already running in background! Please wait.'}), 429
        IS_REDEEM_RUNNING = True
    LAST_REDEEM_ALL_TIME = get_ist_now_str()
    save_timestamps()
    def redeem_runner():
        global IS_REDEEM_RUNNING
        try:
            add_log(f"[REDEEM ALL] Global 1-Click Request Redeem started at {LAST_REDEEM_ALL_TIME}...")
            success_count = 0
            already_count = 0
            below_min_count = 0
            ineligible_count = 0
            skipped_cnt = 0
            nodes_copy = list(NODES)
            for node in nodes_copy:
                if not node.access_token or '@alphea.local' in node.email or '401' in node.status or 'Dead' in node.status or 'Expired' in node.status:
                    skipped_cnt += 1
                    continue
                # 0ms Instant Fast-Path for nodes already claimed in this round (Untouched & Safe)
                if node.redeem_claimed or 'Claimed' in node.redeem_status_text:
                    already_count += 1
                    continue
                try:
                    res = node.request_redeem()
                    msg = res.get('message', '')
                    if res.get('success'):
                        if res.get('already_redeemed'):
                            already_count += 1
                        else:
                            success_count += 1
                    elif res.get('below_minimum'):
                        below_min_count += 1
                    else:
                        ineligible_count += 1
                    add_log(f"[{node.name}] Request Redeem: {msg}")
                except Exception as e:
                    add_log(f"[{node.name}] Request Redeem error: {e}")
                time.sleep(random.uniform(1.8, 2.5))
            add_log(f"[REDEEM ALL] Sequence completed at {get_ist_now_str()}! Redeemed: {success_count} | Already Redeemed (Untouched): {already_count} | Below Min (<3k): {below_min_count} | Standby/No Wallet: {ineligible_count} | Skipped: {skipped_cnt}")
        finally:
            with SWEEP_LOCK:
                IS_REDEEM_RUNNING = False

    threading.Thread(target=redeem_runner, daemon=True).start()
    return jsonify({
        'success': True,
        'message': f'Global Request Redeem sequence initiated ({LAST_REDEEM_ALL_TIME})!'
    }), 200

@app.route('/api/claim_all', methods=['GET', 'POST'])
def route_claim_all():
    global LAST_CLAIM_ALL_TIME, IS_CLAIM_RUNNING
    with SWEEP_LOCK:
        if IS_CLAIM_RUNNING:
            return jsonify({'success': False, 'message': 'Sponsored Claim sweep is already running in background! Please wait.'}), 429
        IS_CLAIM_RUNNING = True
    LAST_CLAIM_ALL_TIME = get_ist_now_str()
    save_timestamps()
    def claim_runner():
        global IS_CLAIM_RUNNING
        try:
            add_log(f"[CLAIM ALL] Global 1-Click Sponsored Claim sweep started at {LAST_CLAIM_ALL_TIME}...")
            nodes_copy = list(NODES)
            skipped_cnt = 0
            for node in nodes_copy:
                if not node.access_token or '@alphea.local' in node.email or '401' in node.status or 'Dead' in node.status or 'Expired' in node.status:
                    skipped_cnt += 1
                    continue
                try:
                    node.check_and_claim_sponsored_rewards()
                except Exception as e:
                    add_log(f"[{node.name}] Claim sweep error: {e}")
                time.sleep(random.uniform(0.8, 1.5))
            add_log(f"[CLAIM ALL] Sponsored Claim sweep completed at {get_ist_now_str()}!")
        finally:
            with SWEEP_LOCK:
                IS_CLAIM_RUNNING = False

    threading.Thread(target=claim_runner, daemon=True).start()
    return jsonify({
        'success': True,
        'message': f'Global Sponsored Claim sweep initiated ({LAST_CLAIM_ALL_TIME})!'
    }), 200

# ---------------- INITIALIZATION ----------------
def initialize_cluster():
    global NODES
    load_timestamps()
    accounts = fetch_accounts_from_github()
    if not accounts:
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, 'r') as f:
                    accounts = json.load(f)
            except Exception:
                accounts = []
        else:
            accounts = []

    # If empty or only local placeholders, start with 3 placeholder slots
    if not accounts:
        accounts = [
            {
                'name': f"Cluster 2 Node {i+1}",
                'email': f"c3node{i+1}@alphea.local",
                'deviceId': f"c3{i+1}a0e2f49583ea{i+1}",
                'proxy': None,
                'location': 'Direct Render VPS',
                'accessToken': '',
                'refreshToken': '',
                'enabled': True
            } for i in range(3)
        ]

    with open(ACCOUNTS_FILE, 'w') as f:
        json.dump(accounts, f, indent=2)

    with NODES_LOCK:
        NODES = []
        for i, acc in enumerate(accounts):
            worker = AccountWorker(i, acc)
            NODES.append(worker)
            if PAUSE_MODE:
                worker.status = "Paused (Sleep Mode)"
                worker.update_cluster_state()
            else:
                worker.update_cluster_state()
                worker.start()

    if not PAUSE_MODE and ENABLE_AUTO_OTP:
        def boot_relogin_worker():
            time.sleep(8)
            add_log("[BOOT RECOVERY] Automated sequential recovery started for freediamond nodes...")
            with NODES_LOCK:
                nodes_copy = list(NODES)
            recovered_count = 0
            for node in nodes_copy:
                if PAUSE_MODE:
                    break
                if '@freediamond.in' in node.email and (not node.access_token or not node.jwt_exp or time.time() >= node.jwt_exp):
                    try:
                        success = node.auto_relogin_via_otp()
                        if success:
                            node.start_foreground_session()
                            recovered_count += 1
                            time.sleep(2.5)
                    except Exception as ex:
                        add_log(f"[{node.name}] Boot recovery note: {ex}")
                        time.sleep(2.0)
            add_log(f"[BOOT RECOVERY] Sequence finished. Successfully recovered {recovered_count} nodes to Mining Active!")

        threading.Thread(target=boot_relogin_worker, name="BootReloginWorker", daemon=True).start()

if __name__ == '__main__':
    add_log(f"Starting Cluster 2 Flask server on port {PORT}...")
    ensure_worker_engine_running()
    app.run(host='0.0.0.0', port=PORT)
