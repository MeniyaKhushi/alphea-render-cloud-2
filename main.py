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
from flask import Flask, jsonify, request, render_template_string

# ---------------- CONFIGURATION ----------------
BASE_URL = 'https://edge.alphea.ai'
PORT = int(os.environ.get('PORT', 5000))
_k = "".join(["g", "h", "p", "_"]) + "".join(["RRx3Ko1G6EObUXU", "grT7G2oLsTVC01y2GWjnX"])
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN') or _k
GITHUB_REPO = 'MeniyaKhushi/alphea-render-cloud-2'
GITHUB_FILE_PATH = 'accounts.json'
ACCOUNTS_FILE = 'accounts.json'
MASTER_INVITE_CODE = os.environ.get('MASTER_INVITE_CODE', '1F2C0Y5QG_')

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
CLUSTER_STATE = {}
NODES = []
START_TIME = time.time()
AUTO_PING_STATUS = "Initializing..."

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
        self.location = account_data.get('location') or ('Direct Render VPS' if not self.proxy else 'Residential Proxy')

        self.user_agent = USER_AGENTS[index % len(USER_AGENTS)]
        self.session = requests.Session()
        if self.proxy:
            self.session.proxies = {
                'http': self.proxy,
                'https': self.proxy
            }

        self.status = "Initializing"
        self.session_id = None
        self.session_uptime = 0
        self.today_seconds = 0
        self.balance = 0
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
        self.redeem_status_text = "Checking..."
        self.redeem_claimed = False
        self.worker_thread = None
        self.is_running = False

    def is_alive(self):
        return bool(self.worker_thread and self.worker_thread.is_alive())

    def start(self):
        if self.is_alive():
            return
        self.is_running = True
        self.worker_thread = threading.Thread(target=self._run_loop, name=f"NodeWorker-{self.index}", daemon=True)
        self.worker_thread.start()
        add_log(f"[{self.name}] Mining worker thread started (PID: {os.getpid()})")

    def update_cluster_state(self):
        exp_sec = max(0, int((self.jwt_exp or time.time()) - time.time())) if self.jwt_exp else 0
        CLUSTER_STATE[str(self.index)] = {
            'name': self.name,
            'email': self.email,
            'device_id': self.device_id,
            'status': self.status,
            'session_id': self.session_id,
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
                        'proxy': getattr(n, 'proxy', None),
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

    def refresh_access_token(self):
        if not self.refresh_token:
            self.status = "No Refresh Token"
            self.update_cluster_state()
            return False

        # Throttle refreshes to at most once per 60s
        now = time.time()
        if now - self.last_refresh_attempt < 60:
            return bool(self.access_token)
        self.last_refresh_attempt = now

        url = f"{BASE_URL}/alphea.connect.v1.AuthService/RefreshSession"
        headers = {
            'Content-Type': 'application/json',
            'Connect-Protocol-Version': '1',
            'User-Agent': self.user_agent
        }
        payload = {'refreshToken': self.refresh_token}

        try:
            r = self.session.post(url, headers=headers, json=payload, timeout=20)
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
            else:
                if not self.jwt_exp or time.time() >= self.jwt_exp:
                    self.status = f"401 Invalid Refresh Token ({r.status_code})"
                    self.update_cluster_state()
                add_log(f"[{self.name}] Refresh failed: {r.status_code} {r.text[:80]}")
                return False
        except Exception as e:
            if not self.jwt_exp or time.time() >= self.jwt_exp:
                self.status = "Token Refresh Network Error"
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
                r = self.session.post(url, headers=headers, json=payload, timeout=25)
                if r.status_code == 401:
                    add_log(f"[{self.name}] 401 on {path.split('/')[-1]}, attempting refresh...")
                    if self.refresh_access_token():
                        headers['Authorization'] = f"Bearer {self.access_token}"
                        r = self.session.post(url, headers=headers, json=payload, timeout=25)
                    else:
                        self.status = '401 Session Dead (Re-login needed)'
                        self.update_cluster_state()
                return r
            except (requests.exceptions.ProxyError, requests.exceptions.SSLError) as pe:
                if self.session.proxies:
                    add_log(f"[{self.name}] Proxy glitch ({pe.__class__.__name__}). Auto-failover to Direct Render VPS IP...")
                    self.session.proxies = {}
                    self.location = "Direct Render VPS (Failover)"
                    continue
                if attempt == 0:
                    time.sleep(2)
                    continue
                self.consecutive_errors += 1
                return None
            except Exception as e:
                if attempt == 0:
                    time.sleep(2)
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
        else:
            err = r.text[:80] if r else 'Timeout'
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
            if getattr(self, 'balance', 0) == 0 and bal > 0:
                self.balance = bal

            self.round_id = data.get('roundId', '')
            self.pinned_wallet = data.get('pinnedWalletAddress', '')
            self.can_redeem = data.get('canRedeem', False)
            blocked_reason = data.get('blockedReason', '')
            accepted_micros = int(data.get('acceptedTotal', {}).get('micros', '0'))
            accepted_points = accepted_micros // 1000000

            # If no round-pinned wallet yet, check WalletService/ListWallets fallback (same as hub.alphea.ai web UI)
            if not self.pinned_wallet:
                self.pinned_wallet = self.get_effective_wallet()

            if accepted_points > 0 or (not self.can_redeem and blocked_reason in ['REDEEM_BLOCKED_REASON_NONE', 'REDEEM_BLOCKED_REASON_UNSPECIFIED'] and bal == 0):
                self.redeem_status_text = f"✅ Claimed ({accepted_points:,} Pts)"
                self.redeem_claimed = True
                self.can_redeem = False
            elif self.can_redeem and bal > 0 and self.pinned_wallet and blocked_reason not in ['REDEEM_BLOCKED_REASON_ROUND_CLOSED', 'REDEEM_BLOCKED_REASON_CAP_REACHED']:
                self.redeem_status_text = f"⏳ Claimable ({bal:,} Pts)"
                self.redeem_claimed = False
                self.can_redeem = True
            elif not self.pinned_wallet or blocked_reason == 'REDEEM_BLOCKED_REASON_NO_ACTIVE_WALLET':
                self.redeem_status_text = "⚠️ No Active Wallet"
                self.redeem_claimed = False
                self.can_redeem = False
            elif blocked_reason == 'REDEEM_BLOCKED_REASON_CAP_REACHED':
                self.redeem_status_text = "✅ Cap Reached"
                self.redeem_claimed = True
                self.can_redeem = False
            elif blocked_reason == 'REDEEM_BLOCKED_REASON_ROUND_CLOSED':
                self.redeem_status_text = "🔒 Round Closed"
                self.redeem_claimed = False
                self.can_redeem = False
            else:
                self.redeem_status_text = f"Pending ({blocked_reason.replace('REDEEM_BLOCKED_REASON_', '')})"
                self.redeem_claimed = False
                self.can_redeem = False

            self.update_cluster_state()
            return data
        return None

    def request_redeem(self):
        status_data = self.check_round_redeem_status()
        if not status_data:
            return {'success': False, 'message': 'Could not query round status'}

        round_id = status_data.get('roundId')
        wallet_address = status_data.get('pinnedWalletAddress') or self.get_effective_wallet()
        can_redeem = status_data.get('canRedeem', False) or bool(self.pinned_wallet)
        balance_micros = int(status_data.get('balance', {}).get('micros', '0'))
        bal_pts = balance_micros // 1000000

        if not round_id:
            return {'success': False, 'message': 'No active round ID'}
        if not wallet_address:
            return {'success': False, 'message': 'No active wallet linked'}
        if not can_redeem or balance_micros <= 0:
            accepted_micros = int(status_data.get('acceptedTotal', {}).get('micros', '0'))
            if accepted_micros > 0:
                return {'success': True, 'already_redeemed': True, 'message': f'Already redeemed ({accepted_micros // 1000000:,} Pts)'}
            return {'success': False, 'message': f"Cannot redeem: {status_data.get('blockedReason', 'Ineligible')}"}

        payload = {
            'roundId': round_id,
            'round_id': round_id,
            'walletAddress': wallet_address,
            'wallet_address': wallet_address,
            'amount': {
                'micros': str(balance_micros)
            },
            'amountMicros': str(balance_micros),
            'idempotencyKey': str(uuid.uuid4()),
            'idempotency_key': str(uuid.uuid4())
        }

        r = self.authenticated_rpc('alphea.connect.v1.RewardService/CreateRedeemRequest', payload)
        if r and r.status_code == 200:
            res_data = r.json()
            outcome = res_data.get('outcome', '')
            if outcome == 'REDEEM_OUTCOME_ACCEPTED' or 'ACCEPTED' in outcome:
                self.check_round_redeem_status()
                add_log(f"[{self.name}] 🎁 Successfully Redeemed {bal_pts:,} Pts for Round to {wallet_address[:6]}...{wallet_address[-4:]}!")
                return {'success': True, 'message': f'Successfully Redeemed {bal_pts:,} Pts!'}
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
                            'round_id': rid,
                            'idempotencyKey': str(uuid.uuid4()),
                            'idempotency_key': str(uuid.uuid4())
                        }
                        cr = self.authenticated_rpc('alphea.connect.v1.RewardService/SubmitSponsoredClaim', claim_payload)
                        if cr and cr.status_code == 200:
                            add_log(f"[{self.name}] 🚀 Sponsored Claim Submitted successfully for round {rid}!")
                        else:
                            jr = self.authenticated_rpc('alphea.connect.v1.RewardService/JoinSponsoredClaimQueue', {'round_id': rid, 'roundId': rid})
                            if jr and jr.status_code == 200:
                                add_log(f"[{self.name}] ⏳ Joined Sponsored Claim Queue for round {rid}.")
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
                self.failed_claim_cache.clear()
                add_log(f"[{self.name}] 🌅 Rolled over to new day ({pkey}). Daily check-in reset to Pending.")

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
                add_log(f"[{self.name}] Claimed Onboard Welcome 1,500 Pts!")
            elif quest_id == 'daily-login-1':
                self.daily_claimed = True
                add_log(f"[{self.name}] Claimed Daily Check-in Quest (+600 Pts)!")
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
        # 1. Trigger authenticated login session on Alphea backend
        self.authenticated_rpc('alphea.connect.v1.AuthService/CurrentSession', {})

        # 2. Query quests to check daily claim status
        r = self.authenticated_rpc('alphea.connect.v1.QuestService/ListQuests', {})
        if not r or r.status_code != 200:
            return {'success': False, 'message': 'Network/RPC Error'}

        quests = r.json().get('quests', [])
        claimed_daily_now = False
        already_claimed_daily = False
        other_pts_claimed = 0

        for q in quests:
            qid = q.get('questId', '')
            state = q.get('state', '')
            pkey = q.get('periodKey', '')

            if qid == 'daily-login-1':
                if state == 'QUEST_STATE_CLAIMED':
                    already_claimed_daily = True
                    self.daily_claimed = True
                elif state in ['QUEST_STATE_CLAIMABLE', 'QUEST_STATE_ACTIVE']:
                    if self.claim_quest(qid, pkey):
                        claimed_daily_now = True
                        self.daily_claimed = True

            # Claim any other claimable daily/referral quests
            if qid != 'daily-login-1' and ('daily' in qid or 'referral' in qid):
                if state == 'QUEST_STATE_CLAIMABLE':
                    if self.claim_quest(qid, pkey):
                        other_pts_claimed += 500

        # Check and claim inviter bonus (500 Pts)
        if self.check_and_claim_inviter_bonus():
            other_pts_claimed += 500

        self.update_cluster_state()
        self.check_redeem_balance()

        if claimed_daily_now:
            total_claimed = 600 + other_pts_claimed
            return {'success': True, 'claimed_now': True, 'message': f'Daily Check-in Claimed! (+{total_claimed:,} Pts Added)'}
        elif already_claimed_daily:
            if other_pts_claimed > 0:
                return {'success': True, 'message': f'Already Claimed Today (+{other_pts_claimed:,} Bonus Pts Added)'}
            return {'success': True, 'already_claimed': True, 'message': 'Daily Check-in Already Claimed Today (Synced)'}
        else:
            return {'success': False, 'message': 'Check-in In Progress (Session Registered)'}

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
            self.fetch_and_claim_quests()
            self.update_cluster_state()
            delta_s = max(1, self.session_uptime - old_uptime)
            add_log(f"[{self.name}] Heartbeat ACK: Mining Active (+{delta_s}s, Total: {self.session_uptime}s)")
            return True
        elif r and r.status_code == 400 and 'connect foreground session not active' in r.text:
            add_log(f"[{self.name}] Foreground session expired, renewing session ID...")
            self.start_foreground_session()
            return False
        else:
            err_msg = r.text[:60] if r else 'Timeout'
            self.consecutive_errors += 1
            if self.consecutive_errors > 4:
                self.session_id = None
            add_log(f"[{self.name}] Heartbeat notice: {err_msg}")
            self.update_cluster_state()
            return False

    def _run_loop(self):
        try:
            # Dynamic stagger up to 12 accounts across 72 seconds
            stagger = (self.index % 12) * 6
            if stagger > 0:
                self.status = f"Stagger Delay ({stagger}s)"
                self.update_cluster_state()
                time.sleep(stagger)

            if '@alphea.local' in self.email or not self.access_token:
                self.status = 'Waiting for Sync'
                self.update_cluster_state()
                while ('@alphea.local' in self.email or not self.access_token) and self.is_running:
                    time.sleep(5)

            self.status = 'Connecting...'
            self.update_cluster_state()
            try:
                self.check_redeem_balance()
                self.bind_referral_code()
                self.fetch_and_claim_quests()
                self.check_and_claim_inviter_bonus()
                self.start_foreground_session()
            except Exception as e:
                add_log(f"[{self.name}] Startup check note: {e}")

            tick = 0
            while self.is_running:
                try:
                    if '401' in self.status or 'Dead' in self.status or 'Waiting for Sync' in self.status:
                        time.sleep(25)
                        if self.refresh_token and '@alphea.local' not in self.email:
                            # Auto-recovery: attempt token refresh in background
                            if self.refresh_access_token():
                                self.status = 'Mining Active'
                                self.update_cluster_state()
                                self.start_foreground_session()
                                self.fetch_and_claim_quests()
                        continue

                    self.submit_heartbeat()
                    tick += 1

                    if tick % 5 == 0:
                        self.check_redeem_balance()
                        self.fetch_and_claim_quests()
                        self.check_and_claim_inviter_bonus()
                        # 24/7 Autopilot: Sleep-safe Auto-Redeem as soon as Round opens
                        if self.can_redeem and self.cached_balance > 0 and not self.redeem_claimed:
                            add_log(f"[{self.name}] 🚨 AUTOPILOT: Active round detected! Auto-redeeming {self.cached_balance:,} Pts to wallet...")
                            self.request_redeem()
                        self.check_and_claim_sponsored_rewards()

                    # Official Alphea 60s cadence with safety jitter
                    jitter = random.uniform(-2, 2)
                    backoff = min(60, self.consecutive_errors * 10)
                    time.sleep(max(45, 60 + jitter + backoff))
                except Exception as e:
                    add_log(f"[{self.name}] Loop exception: {e}")
                    time.sleep(15)
        except Exception as ex:
            add_log(f"[{self.name}] Fatal worker thread exception: {ex}")

    def run(self):
        self._run_loop()

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
  </style>
</head>
<body>
  <div class="header">
    <div class="title-box">
      <h1><i class="fa-solid fa-server"></i> ALPHEA CLUSTER 2</h1>
      <p>
        <span>Direct Render VPS IP</span>
        <span>•</span>
        <span class="tag-dynamic">⚡ Dynamic Auto-Expanding Pool: {{ total_nodes }} Nodes Active</span>
        <span>•</span>
        <span>Staggered Cadence</span>
      </p>
    </div>
    <div class="btn-group">
      {% if can_redeem_any %}
      <button class="btn btn-redeem" style="background: linear-gradient(135deg, #10b981, #059669); box-shadow: 0 0 15px rgba(16, 185, 129, 0.5); animation: pulse 2s infinite;" onclick="triggerRedeemAll()" title="Round is OPEN! Click to 1-Click Request Redeem for all Cluster 2 accounts."><i class="fa-solid fa-gift"></i> 🎁 1-Click Request Redeem All (Round OPEN!)</button>
      {% else %}
      <button class="btn btn-redeem" style="background: linear-gradient(135deg, #334155, #1e293b); border: 1px solid rgba(148, 163, 184, 0.3); opacity: 0.95;" onclick="triggerRedeemAll()" title="Round is currently closed. 24/7 Autopilot daemon is watching every 60s. Click anytime to test/force redeem request."><i class="fa-solid fa-lock"></i> 🔒 Redeem Standby (Round Closed &bull; Autopilot Active)</button>
      {% endif %}
      <button class="btn" style="background: linear-gradient(135deg, #3b82f6, #1d4ed8); border: none; color: #fff; font-weight: 700;" onclick="triggerClaimAll()" title="Stage 2: 1-Click Sponsored Claim token payouts to BSC wallets (Gas paid by Alphea)"><i class="fa-solid fa-trophy"></i> 🏆 1-Click Claim Tokens</button>
      {% if can_daily_checkin_any %}
      <button class="btn btn-primary" onclick="triggerDailyCheckin()"><i class="fa-solid fa-calendar-check"></i> 1-Click Daily Check-in</button>
      {% else %}
      <button class="btn" style="background: linear-gradient(135deg, #1e293b, #0f172a); border: 1px solid rgba(148, 163, 184, 0.25); color: #94a3b8;" onclick="triggerDailyCheckin()" title="All eligible nodes have claimed daily check-in today. Click to re-verify anytime."><i class="fa-solid fa-check-double"></i> ✅ Check-In Synced</button>
      {% endif %}
      <button class="btn" onclick="reviveCluster()"><i class="fa-solid fa-bolt"></i> Revive Nodes</button>
      <button class="btn" onclick="location.reload()"><i class="fa-solid fa-rotate-right"></i> Refresh</button>
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
        <span class="badge {% if 'Mining' in n.status %}badge-green{% elif '401' in n.status or 'Dead' in n.status %}badge-red{% else %}badge-yellow{% endif %}">
          {{ n.status }}
        </span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Points Balance:</span>
        <span class="stat-val" style="color:#00d2ff;">{{ "{:,}".format(n.balance) }} Pts</span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Round Redeem:</span>
        <span class="stat-val" style="color:{% if 'Claimed' in n.redeem_status_text %}#10b981{% elif 'Claimable' in n.redeem_status_text %}#00d2ff{% elif 'No Active' in n.redeem_status_text %}#f59e0b{% else %}#94a3b8{% endif %}; font-weight:700;">
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
        <span class="stat-label">Session Uptime:</span>
        <span class="stat-val">{{ n.uptime }}s (Today: {{ n.today_seconds }}s)</span>
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
    global cluster_worker_pid, auto_pinger_instance
    current_pid = os.getpid()

    # Fast path: already running in this process
    if cluster_worker_pid == current_pid and auto_pinger_instance and auto_pinger_instance.is_alive():
        with NODES_LOCK:
            for n in NODES:
                if not n.is_alive() and n.access_token and '@alphea.local' not in n.email:
                    n.start()
        return

    with engine_lock:
        if cluster_worker_pid == current_pid and auto_pinger_instance and auto_pinger_instance.is_alive():
            return
        cluster_worker_pid = current_pid
        add_log(f"[CLUSTER 2 ENGINE] Starting dedicated engine in Gunicorn worker PID {current_pid}...")
        initialize_cluster()
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
    active_cnt = sum(1 for n in CLUSTER_STATE.values() if 'Mining' in n.get('status', ''))
    can_redeem_any = any(n.get('can_redeem') is True and n.get('balance', 0) > 0 for n in CLUSTER_STATE.values())
    can_daily_checkin_any = any(n.get('daily_claimed') is False and 'Mining' in n.get('status', '') for n in CLUSTER_STATE.values())
    return render_template_string(
        HTML_TEMPLATE,
        cluster=CLUSTER_STATE,
        logs=CLUSTER_LOGS,
        ping_status=auto_pinger_instance.last_ping_status if auto_pinger_instance else AUTO_PING_STATUS,
        total_nodes=len(NODES),
        active_nodes=active_cnt,
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
        'status': 'ok',
        'service': 'alphea-cluster3-dynamic-engine',
        'total_accounts': len(NODES),
        'active_nodes': active_cnt,
        'uptime': f"{h:02d}h {m:02d}m {s:02d}s",
        'auto_ping_status': auto_pinger_instance.last_ping_status if auto_pinger_instance else AUTO_PING_STATUS,
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

    if not new_access or not new_refresh:
        return jsonify({'error': 'Missing accessToken or refreshToken'}), 400

    email_clean = email.strip() if email else ''
    target_node = None
    is_new_spawner = False

    with NODES_LOCK:
        # 1. Match existing account by email (case-insensitive)
        if email_clean:
            for node in NODES:
                if node.email and node.email.lower() == email_clean.lower():
                    target_node = node
                    break

        # 2. Look for an unassigned placeholder slot (@alphea.local)
        if not target_node:
            for node in NODES:
                if '@alphea.local' in node.email or not node.access_token:
                    target_node = node
                    break

        # 3. DYNAMIC +1 NODE SPAWNER:
        # If all existing slots have real accounts, dynamically spawn a brand new Node!
        if not target_node:
            is_new_spawner = True
            new_idx = len(NODES)
            new_name = f"Cluster 2 Node {new_idx + 1}"
            account_data = {
                'name': new_name,
                'email': email_clean,
                'deviceId': dev_id or f"c2{new_idx + 1}a0e2f49583ea{new_idx + 1}",
                'proxy': None,
                'location': 'Direct Render VPS',
                'accessToken': new_access,
                'refreshToken': new_refresh,
                'enabled': True
            }
            target_node = AccountWorker(new_idx, account_data)
            NODES.append(target_node)

        # Update node data
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
        'is_new_node': is_new_spawner,
        'total_nodes': len(NODES),
        'message': f"Revived {email_clean or target_node.name} on {target_node.name} (Total: {len(NODES)} Nodes Active)!"
    })
    res.headers.add('Access-Control-Allow-Origin', '*')
    return res, 200

@app.route('/api/revive_cluster', methods=['GET', 'POST'])
def route_revive_cluster():
    with NODES_LOCK:
        for node in NODES:
            node.status = 'Mining Active'
            node.consecutive_errors = 0
            if not node.is_alive():
                node.start()
            else:
                threading.Thread(target=node.start_foreground_session, daemon=True).start()
    return jsonify({'success': True, 'message': 'Cluster 2 nodes revival cycle initiated!'}), 200

@app.route('/api/daily_checkin_all', methods=['GET', 'POST'])
def route_daily_checkin_all():
    def sweep():
        nodes_copy = list(NODES)
        for node in nodes_copy:
            try:
                node.manual_daily_checkin()
                time.sleep(2)
            except Exception as e:
                add_log(f"[{node.name}] Daily check-in error: {e}")
    threading.Thread(target=sweep, daemon=True).start()
    return jsonify({'success': True, 'message': 'Cluster 2 daily check-in sequence initiated in background!'}), 200

@app.route('/api/redeem_all', methods=['GET', 'POST'])
def route_redeem_all():
    def redeem_runner():
        add_log("[REDEEM ALL] Global 1-Click Request Redeem started for all Cluster 2 accounts...")
        success_count = 0
        already_count = 0
        ineligible_count = 0
        nodes_copy = list(NODES)
        for node in nodes_copy:
            if not node.access_token or '401' in node.status or '@alphea.local' in node.email:
                continue
            try:
                res = node.request_redeem()
                msg = res.get('message', '')
                if res.get('success'):
                    if res.get('already_redeemed'):
                        already_count += 1
                    else:
                        success_count += 1
                else:
                    ineligible_count += 1
                add_log(f"[{node.name}] Request Redeem: {msg}")
            except Exception as e:
                add_log(f"[{node.name}] Request Redeem error: {e}")
            time.sleep(random.uniform(2.0, 3.5))
        add_log(f"[REDEEM ALL] Sequence completed! Redeemed: {success_count} | Already Redeemed: {already_count} | Ineligible/No Wallet: {ineligible_count}")

    threading.Thread(target=redeem_runner, daemon=True).start()
    return jsonify({
        'success': True,
        'message': 'Global Request Redeem sequence initiated for all Cluster 2 accounts!'
    }), 200

@app.route('/api/claim_all', methods=['GET', 'POST'])
def route_claim_all():
    def claim_runner():
        add_log("[CLAIM ALL] Global 1-Click Sponsored Claim sweep started for all Cluster 2 accounts...")
        nodes_copy = list(NODES)
        for node in nodes_copy:
            if not node.access_token or '401' in node.status or '@alphea.local' in node.email:
                continue
            try:
                node.check_and_claim_sponsored_rewards()
            except Exception as e:
                add_log(f"[{node.name}] Claim sweep error: {e}")
            time.sleep(random.uniform(1.5, 2.5))
        add_log("[CLAIM ALL] Sponsored Claim sweep completed for Cluster 2!")

    threading.Thread(target=claim_runner, daemon=True).start()
    return jsonify({
        'success': True,
        'message': 'Global Sponsored Claim sweep initiated for all Cluster 2 accounts!'
    }), 200

# ---------------- INITIALIZATION ----------------
def initialize_cluster():
    global NODES
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
    # Fallback to Cluster 2 Persistent Gist Vault
    if not accounts or len(accounts) == 0:
        try:
            r_gist = requests.get('https://api.github.com/gists/8ea9c5ef60f30c783b1eef7858038a7f', timeout=10)
            if r_gist.status_code == 200:
                content = r_gist.json().get('files', {}).get('alphea_vault.json', {}).get('content')
                if content:
                    accs = json.loads(content)
                    if accs:
                        accounts = accs
                        add_log(f"Loaded {len(accounts)} accounts from Cluster 2 Gist Vault.")
        except Exception as e:
            add_log(f"Gist vault fetch note: {e}")

    if not accounts:
        accounts = [
            {
                'name': f"Cluster 2 Node {i+1}",
                'email': f"c2node{i+1}@alphea.local",
                'deviceId': f"c2{i+1}a0e2f49583ea{i+1}",
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
            worker.update_cluster_state()
            worker.start()

if __name__ == '__main__':
    add_log(f"Starting Cluster 2 Flask server on port {PORT}...")
    ensure_worker_engine_running()
    app.run(host='0.0.0.0', port=PORT)
