import imaplib
import socket
import email
import time
import configparser
import os
import json
import requests
import logging
import threading
import base64
from email.header import decode_header
from groq import Groq
from datetime import datetime, timedelta
import sys
import io
import re
import ssl

# ── Load .env automatically ───────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # pip install python-dotenv if needed

# ── Microsoft OAuth2 (XOAUTH2) for IMAP ──────────────────────────────────────
try:
    import msal
    _MSAL_AVAILABLE = True
except ImportError:
    msal = None
    _MSAL_AVAILABLE = False

# Azure credentials from .env
_AZURE_CLIENT_ID     = os.environ.get("AZURE_CLIENT_ID",     "")
_AZURE_CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "")
_AZURE_TENANT_ID     = os.environ.get("AZURE_TENANT_ID",     "")
_AZURE_EMAIL         = os.environ.get("AZURE_EMAIL",         "")   # primary
_AZURE_EMAIL2        = os.environ.get("AZURE_EMAIL2",        "")   # secondary (optional)

_OAUTH_READY = bool(_AZURE_CLIENT_ID and _AZURE_CLIENT_SECRET and _AZURE_TENANT_ID)

_oauth_token_cache = {}
_oauth_token_lock  = threading.Lock()


def _get_oauth_access_token(email_address: str) -> str:
    """Acquire (or return cached) OAuth2 access token for IMAP XOAUTH2."""
    if not _MSAL_AVAILABLE:
        raise RuntimeError("msal not installed. Run: pip install msal")
    if not _OAUTH_READY:
        raise RuntimeError("Azure credentials missing in .env")

    with _oauth_token_lock:
        cached = _oauth_token_cache.get(email_address)
        if cached and time.time() < cached["expires_at"] - 300:
            return cached["access_token"]

        logger.info(f"🔑 Acquiring OAuth2 token for {email_address}...")
        app = msal.ConfidentialClientApplication(
            client_id=_AZURE_CLIENT_ID,
            client_credential=_AZURE_CLIENT_SECRET,
            authority=f"https://login.microsoftonline.com/{_AZURE_TENANT_ID}",
        )
        
        # For client credentials flow, use application permissions (not delegated)
        # The correct scope for IMAP with app-only authentication
        result = app.acquire_token_for_client(
            scopes=["https://outlook.office365.com/.default"]
        )
        
        if "access_token" not in result:
            err = result.get("error_description", result.get("error", "unknown"))
            raise RuntimeError(f"OAuth2 token acquisition failed: {err}")

        expires_in = result.get("expires_in", 3600)
        _oauth_token_cache[email_address] = {
            "access_token": result["access_token"],
            "expires_at":   time.time() + expires_in,
        }
        logger.info(f"✅ OAuth2 token obtained for {email_address} (expires in {expires_in}s)")
        return result["access_token"]

def _build_xoauth2_string(email_address: str, access_token: str) -> bytes:
    """Build the raw XOAUTH2 bytes for IMAP AUTHENTICATE.

    CRITICAL: imaplib._Authenticator.encode() base64-encodes whatever
    the callback returns. Therefore the callback must return the RAW
    bytes — NOT already-base64-encoded data. imaplib does the encoding.

    Raw format:  b"user=<email>\x01auth=Bearer <token>\x01\x01"
    Uses chr(1) to guarantee the real SOH byte (0x01) is embedded,
    regardless of how the file is saved on Windows.
    """
    sep = chr(1)  # real SOH byte — chr(1) always works, \x01 in f-strings may not
    raw = f"user={email_address}{sep}auth=Bearer {access_token}{sep}{sep}"
    return raw.encode("ascii")  # return RAW bytes — imaplib base64-encodes this
# ===================== ROUND-ROBIN ROLE OWNER ASSIGNMENT =====================
ROLE_OWNER_IDS = [4, 5, 6]  # IDs to rotate through (Team Leads)
RECRUITER_IDS = [ 8, 9, 11, 40, 43,48,58,59]  # Active recruiters
RECRUITERS_PER_ROLE = 1  # Assign 1 recruiter per role

# File to store counter state
COUNTER_STATE_FILE = 'round_robin_state.json'

# Initialize counters (will be loaded properly after logger is ready)
current_owner_index = 0
current_recruiter_index = 0

def load_counter_state():
    """
    Load the round-robin counter state from file.
    Returns dict with 'role_owner_index' and 'recruiter_index'.
    """
    if os.path.exists(COUNTER_STATE_FILE):
        try:
            with open(COUNTER_STATE_FILE, 'r') as f:
                state = json.load(f)
                if logger:  # Only log if logger exists
                    logger.info(f"📂 Loaded counter state: Role Owner Index={state.get('role_owner_index', 0)}, Recruiter Index={state.get('recruiter_index', 0)}")
                return state
        except Exception as e:
            if logger:  # Only log if logger exists
                logger.error(f"❌ Error loading counter state: {e}")
            print(f"Warning: Error loading counter state: {e}")  # Fallback to print
    
    # Default state if file doesn't exist
    if logger:  # Only log if logger exists
        logger.info("📂 No existing counter state found, starting fresh")
    else:
        print("Info: No existing counter state found, starting fresh")
    return {'role_owner_index': 0, 'recruiter_index': 0}

def save_counter_state(role_owner_index, recruiter_index):
    """
    Save the current round-robin counter state to file.
    """
    try:
        state = {
            'role_owner_index': role_owner_index,
            'recruiter_index': recruiter_index
        }
        with open(COUNTER_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
        if logger:  # Only log if logger exists
            logger.info(f"💾 Saved counter state: Role Owner Index={role_owner_index}, Recruiter Index={recruiter_index}")
    except Exception as e:
        if logger:  # Only log if logger exists
            logger.error(f"❌ Error saving counter state: {e}")
        else:
            print(f"Warning: Error saving counter state: {e}")

def initialize_round_robin():
    """
    Initialize round-robin counters after logger is ready.
    Call this from main after logger is initialized.
    """
    global current_owner_index, current_recruiter_index
    
    _counter_state = load_counter_state()
    current_owner_index = _counter_state.get('role_owner_index', 0)
    current_recruiter_index = _counter_state.get('recruiter_index', 0)
    
    # Validate indices are within bounds
    if current_owner_index >= len(ROLE_OWNER_IDS):
        logger.warning(f"⚠️ Role owner index {current_owner_index} out of bounds, resetting to 0")
        current_owner_index = 0
    
    if current_recruiter_index >= len(RECRUITER_IDS):
        logger.warning(f"⚠️ Recruiter index {current_recruiter_index} out of bounds, resetting to 0")
        current_recruiter_index = 0
    
    logger.info(f"✅ Round-robin initialized: Owner Index={current_owner_index}, Recruiter Index={current_recruiter_index}")

def get_next_role_owner_id():
    """
    Get the next role owner ID in round-robin fashion.
    Returns IDs 4, 5, 6, 4, 5, 6, ... in sequence
    """
    global current_owner_index
    
    # Get current owner
    owner_id = ROLE_OWNER_IDS[current_owner_index]
    
    # Advance counter for next time
    current_owner_index = (current_owner_index + 1) % len(ROLE_OWNER_IDS)
    
    # Save state immediately
    save_counter_state(current_owner_index, current_recruiter_index)
    
    logger.info(f"🔄 Role Owner assigned: ID {owner_id} (Next index: {current_owner_index})")
    return owner_id

def get_next_recruiter_ids(count=1):
    """
    Get the next N recruiter IDs in round-robin fashion.
    Ensures no duplicates in the same assignment.
    """
    global current_recruiter_index
    
    selected_ids = []
    
    # Ensure we don't request more recruiters than available
    count = min(count, len(RECRUITER_IDS))
    
    for i in range(count):
        recruiter_id = RECRUITER_IDS[current_recruiter_index]
        selected_ids.append(recruiter_id)
        
        # Advance counter
        current_recruiter_index = (current_recruiter_index + 1) % len(RECRUITER_IDS)
    
    # Save state immediately
    save_counter_state(current_owner_index, current_recruiter_index)
    
    logger.info(f"🔄 Recruiters assigned: IDs {selected_ids} (Next index: {current_recruiter_index})")
    return selected_ids

def get_role_owner_name(owner_id):
    """
    Fetch the role owner's name from the Recruiters table.
    Returns the name or None if not found.
    """
    try:
        cfg = load_config()
        token = get_auth_token()
        if not token:
            logger.error("Cannot fetch role owner name: No auth token")
            return None
        
        # Query the correct recruiters endpoint
        url = f"{cfg['api']['base_url']}/api/recruitment/recruiters"
        headers = {"Authorization": f"Bearer {token}"}
        
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            recruiters = r.json()
            # Find the recruiter with matching ID
            for recruiter in recruiters:
                if recruiter.get('id') == owner_id:
                    name = recruiter.get('name')
                    logger.info(f"✓ Found role owner: ID {owner_id} = {name}")
                    return name
            logger.warning(f"⚠ Role owner ID {owner_id} not found in recruiters list")
        else:
            logger.error(f"Failed to fetch recruiters: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Error fetching role owner name: {e}")
    
    return None

def get_recruiter_names(recruiter_ids):
    """
    Fetch recruiter names for given IDs.
    Returns dict mapping ID -> name
    """
    try:
        cfg = load_config()
        token = get_auth_token()
        if not token:
            logger.error("Cannot fetch recruiter names: No auth token")
            return {}
        
        url = f"{cfg['api']['base_url']}/api/recruitment/recruiters"
        headers = {"Authorization": f"Bearer {token}"}
        
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            recruiters = r.json()
            name_map = {}
            for recruiter in recruiters:
                if recruiter.get('id') in recruiter_ids:
                    name_map[recruiter.get('id')] = recruiter.get('name')
            
            logger.info(f"✓ Fetched {len(name_map)} recruiter names")
            return name_map
        else:
            logger.error(f"Failed to fetch recruiters: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Error fetching recruiter names: {e}")
    
    return {}

def reset_round_robin_counters():
    """
    Utility function to manually reset counters to 0.
    Call this if you need to restart the rotation.
    """
    global current_owner_index, current_recruiter_index
    current_owner_index = 0
    current_recruiter_index = 0
    save_counter_state(0, 0)
    logger.info("🔄 Round-robin counters reset to 0")

def get_round_robin_status():
    """
    Get current status of round-robin assignment.
    Returns dict with current state information.
    """
    status = {
        'current_owner_index': current_owner_index,
        'current_recruiter_index': current_recruiter_index,
        'next_owner_id': ROLE_OWNER_IDS[current_owner_index],
        'next_recruiter_id': RECRUITER_IDS[current_recruiter_index],
        'total_owners': len(ROLE_OWNER_IDS),
        'total_recruiters': len(RECRUITER_IDS),
        'recruiters_per_role': RECRUITERS_PER_ROLE
    }
    return status

# ===================== REST OF THE CODE =====================

def extract_experience(text):
    """
    Extract numeric experience from text with improved patterns.
    UPDATED: Now supports '10 & Above', '10 and Above' formats.
    """
    if not text:
        return "N/A"
    
    # Convert to lowercase for case-insensitive matching
    text_lower = text.lower()
    
    # Pattern 1: Context-aware patterns (Look for "Experience:" label first)
    experience_patterns = [
        # ✅ NEW: Handle "10 & Above" or "10 and Above" with label
        r'experience\s*(?:requested|required)?\s*:\s*(\d+\s*(?:&|and)\s*above)',
        
        # Existing patterns
        r'experience\s*(?:requested|required)?\s*:\s*(\d+\s*(?:\+\s*)?(?:\s*to\s*-\s*\d+\s*)?(?:\s*years?|\s*yrs?)?)',
        r'experience\s*(?:requested|required)?\s*:\s*(\d+\s*-\s*\d+)',
        r'experience\s*(?:requested|required)?\s*:\s*(\d+\s*to\s*\d+)',
    ]
    
    for pattern in experience_patterns:
        match = re.search(pattern, text_lower)
        if match:
            experience_text = match.group(1).strip()
            # Clean up: remove "years" but KEEP "& above"
            experience_text = re.sub(r'\s*years?|\s*yrs?', '', experience_text, flags=re.IGNORECASE)
            experience_text = re.sub(r'\s+', ' ', experience_text).strip()
            
            # Formatting: Ensure "10 & above" looks clean
            experience_text = experience_text.replace('and above', '& Above').replace('& above', '& Above')
            
            logger.info(f"✓ Experience extracted (Context match): '{experience_text}'")
            return experience_text
    
    # Pattern 2: Standalone patterns (No label required)
    general_patterns = [
        # ✅ NEW: Handle "10 & Above" without label
        r'\b(\d+\s*(?:&|and)\s*above)\b',
        
        # Existing patterns
        r'(\d+\s*\+\s*yr?s?)',
        r'(\d+\s*-\s*\d+\s*yr?s?)',
        r'(\d+\s*to\s*\d+\s*yr?s?)',
        r'\b(\d+\s*-\s*\d+)\b',
        r'\b(\d+\s*to\s*\d+)\b',
        r'\b(\d+\s*\+\s*)\b',
        r'\b(\d+\s*years?)\b',
        r'\b(\d+\s*yrs?)\b',
        r'\b(\d+)\s*\+\s*years?\b'
    ]
    
    for pattern in general_patterns:
        match = re.search(pattern, text_lower)
        if match:
            experience_text = match.group(1).strip()
            # Clean up
            experience_text = re.sub(r'\s*years?|\s*yrs?', '', experience_text, flags=re.IGNORECASE)
            experience_text = re.sub(r'\s+', ' ', experience_text).strip()
            
            # Formatting
            experience_text = experience_text.replace('and above', '& Above').replace('& above', '& Above')
            
            logger.info(f"✓ Experience extracted (General match): '{experience_text}'")
            return experience_text
    
    logger.warning(f"⚠ No experience pattern found in text")
    return "N/A"

def truncate_field(value, max_len=100):
    import re
    if not value:
        return ""
    # Remove extra spaces/newlines
    value = re.sub(r"\s+", " ", value).strip()
    # Truncate to max_len
    return value[:max_len]

def format_rate_display(rate):
    """Format rate for display without .0 for whole numbers"""
    if rate == int(rate):
        return f"${int(rate)}"
    else:
        return f"${rate}"

# Fix UTF-8 console on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('email_automation.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Global config & token cache
CONFIG = None
AUTH_TOKEN = None
TOKEN_EXPIRY = 0
PROCESSED_UIDS = set()  # Kept for backwards compat but no longer used by listeners
PROCESSED_REQUEST_IDS = set()  # Cross-mailbox duplicate prevention by requestId

# ✅ Thread lock: prevents race condition when both mailboxes receive the same
# job email at nearly the same time. Only one thread can run the check+create
# block at a time, so the second thread will always see the role already in DB.
import threading
_ROLE_CREATION_LOCK = threading.Lock()

# ✅ Vendor watchdog: tracks {role_id: vendor_string} for every role this session created.
# A background thread periodically checks that the vendor field hasn't been wiped
# (e.g. by a frontend save or another process) and restores it if needed.
_VENDOR_REGISTRY = {}          # {role_id: "mailbox1, mailbox2"}
_VENDOR_REGISTRY_LOCK = threading.Lock()

def register_role_vendor(role_id, vendor_string):
    """Register a role's vendor so the watchdog can monitor and restore it."""
    with _VENDOR_REGISTRY_LOCK:
        _VENDOR_REGISTRY[str(role_id)] = vendor_string
    logger.info(f"📋 Vendor watchdog registered: role {role_id} → '{vendor_string}'")

def _watchdog_host_reachable():
    """
    Quick connectivity check before the watchdog runs.
    Tries a socket connection to the API host so we don't spam DNS errors
    when the network is temporarily down.
    Returns True if reachable, False otherwise.
    """
    try:
        cfg = load_config()
        base_url = cfg['api']['base_url']          # e.g. https://prophechyerp.duckdns.org
        # Parse host and port from base_url
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        return True
    except Exception:
        return False


def vendor_watchdog(check_interval_seconds=120):
    """
    Background thread: every check_interval_seconds, scans all roles we created
    this session and restores vendor if it has been wiped to null/N/A.

    - Skips the entire cycle if the API host is unreachable (DNS/network down)
      so it never spams NameResolutionError logs.
    - Uses exponential backoff (up to 10 min) when connectivity fails,
      resetting back to normal interval once the host is reachable again.
    """
    logger.info("🐕 Vendor watchdog started — checks every %ds", check_interval_seconds)
    backoff = check_interval_seconds  # Current wait time; grows on failures
    max_backoff = 600                 # Cap at 10 minutes

    while True:
        time.sleep(backoff)

        with _VENDOR_REGISTRY_LOCK:
            registry_snapshot = dict(_VENDOR_REGISTRY)

        if not registry_snapshot:
            backoff = check_interval_seconds  # Reset — nothing to watch
            continue

        # ✅ Connectivity check — skip silently if host is down
        if not _watchdog_host_reachable():
            backoff = min(backoff * 2, max_backoff)
            logger.warning(f"🐕 Watchdog: API host unreachable — skipping cycle, next check in {backoff}s")
            continue

        # Host is reachable — reset backoff to normal
        backoff = check_interval_seconds

        try:
            token = get_auth_token()
            if not token:
                logger.warning("🐕 Watchdog: could not get auth token — skipping cycle")
                continue

            cfg = load_config()
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            restored_count = 0
            ok_count = 0

            for role_id, expected_vendor in registry_snapshot.items():
                try:
                    url = f"{cfg['api']['base_url']}/api/recruitment/roles/{role_id}"
                    resp = requests.get(url, headers=headers, timeout=10)
                    if resp.status_code != 200:
                        logger.warning(f"🐕 Watchdog: GET role {role_id} returned {resp.status_code} — skipping")
                        continue

                    role_data = resp.json()
                    current_vendor = (role_data.get('vendor') or '').strip()

                    if current_vendor.lower() in ('', 'null', 'none', 'n/a') or current_vendor != expected_vendor:
                        logger.warning(f"🐕 Watchdog: role {role_id} vendor is '{current_vendor}', expected '{expected_vendor}' — RESTORING")
                        role_data['vendor'] = expected_vendor
                        put_resp = requests.put(url, json=role_data, headers=headers, timeout=10)
                        if put_resp.status_code in [200, 204]:
                            logger.info(f"🐕 Watchdog: ✅ restored vendor for role {role_id} → '{expected_vendor}'")
                            restored_count += 1
                        else:
                            logger.error(f"🐕 Watchdog: PUT failed for role {role_id}: {put_resp.status_code} {put_resp.text}")
                    else:
                        ok_count += 1

                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        socket.error) as net_err:
                    # Network error on individual role — log once and move on, don't spam
                    logger.warning(f"🐕 Watchdog: network error for role {role_id} — {type(net_err).__name__}: {net_err}")
                except Exception as e:
                    logger.error(f"🐕 Watchdog: unexpected error for role {role_id}: {e}")

            if restored_count > 0 or ok_count > 0:
                logger.info(f"🐕 Watchdog cycle done — OK: {ok_count}, Restored: {restored_count}, Total watched: {len(registry_snapshot)}")

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                socket.error) as net_err:
            logger.warning(f"🐕 Watchdog: network error in cycle — {type(net_err).__name__}. Will retry in {backoff}s")
        except Exception as e:
            logger.error(f"🐕 Watchdog outer error: {e}")

# ===================== LOAD CONFIG =====================
def load_config():
    global CONFIG
    if CONFIG is None:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        if not os.path.exists(config_path):
            logger.error("config.ini not found!")
            exit(1)
        config.read(config_path, encoding='utf-8')

        def _env(key, fallback=''):
            """Read from env/.env first, then config.ini fallback."""
            return os.environ.get(key, '').strip() or fallback

        def _mb(section: str):
            """Return a mailbox sub-dict from config.ini, or None if missing/empty."""
            if not config.has_section(section):
                return None
            username = config.get(section, 'username', fallback='').strip()
            if not username:
                return None
            return {
                "server":          config.get(section, 'server',       fallback='outlook.office365.com'),
                "port":            config.getint(section, 'port',      fallback=993),
                "username":        username,
                "password":        config.get(section, 'password',     fallback=''),
                "ssl_required":    config.getboolean(section, 'ssl_required', fallback=True),
                "display_name":    config.get(section, 'display_name', fallback=username),
                "allowed_senders": config.get(section, 'allowed_senders', fallback=''),
            }

        # ── API credentials: .env vars take priority over config.ini ─────────
        api_base_url = _env("API_BASE_URL",
                            config.get("api", "base_url", fallback="").rstrip("/"))
        api_username = _env("API_SERVICE_USERNAME",
                            config.get("api", "service_username", fallback=""))
        api_password = _env("API_SERVICE_PASSWORD",
                            config.get("api", "service_password", fallback=""))

        # ── GROQ key: .env takes priority ────────────────────────────────────
        groq_key = _env("GROQ_API_KEY",
                        config.get("groq", "api_key", fallback=""))

        CONFIG = {
            "email":  _mb("email") or {
                "server": "outlook.office365.com", "port": 993,
                "username": _AZURE_EMAIL, "password": "",
                "ssl_required": True, "display_name": _AZURE_EMAIL,
                "allowed_senders": "",
            },
            "email2": _mb("email2"),
            "api": {
                "base_url":         api_base_url,
                "service_username": api_username,
                "service_password": api_password,
            },
            "groq_key": groq_key,
            "alert_emails": {
                "onshore":  config.get("alert_emails", "onshore_email",  fallback=""),
                "offshore": config.get("alert_emails", "offshore_email", fallback=""),
            },
        }

        logger.info("✅ Configuration loaded (config.ini + .env)")
        logger.info(f"   OAuth2 : {'✅ YES — ' + _AZURE_EMAIL if _OAUTH_READY and _AZURE_EMAIL else '❌ NO'}")
        logger.info(f"   API URL: {api_base_url or '❌ NOT SET'}")
        logger.info(f"   API usr: {'✅ set' if api_username else '❌ NOT SET — add API_SERVICE_USERNAME to .env'}")
    return CONFIG


def is_sender_allowed(from_address: str, allowed_raw: str) -> bool:
    """
    Check whether from_address is in the allowed_senders list.
    allowed_raw is a comma-separated string of permitted email addresses/domains.
    Returns True (allow) when the list is empty (no filter configured).
    """
    if not allowed_raw or not allowed_raw.strip():
        return True  # No filter configured — allow all
    allowed = [s.strip().lower() for s in allowed_raw.split(',') if s.strip()]
    addr_lower = from_address.lower()
    for entry in allowed:
        if entry in addr_lower:
            return True
    return False

def parse_email_fallback(subject: str, body: str) -> dict:
    """
    Fallback parser using regex when AI fails.
    Extracts basic info from the email structure.
    """
    logger.info("Using fallback email parser...")
    
    data = {
        "client": "",
        "jobTitle": "",
        "city": "",
        "state": "",
        "country": "",
        "workMode": "Onsite",
        "billRateMax": 0,
        "billRateMin": 0,
        "experience": "",
        "requestId": "",
        "gbamsId": "",
        "clientPOC": "",
        "startDate": "",
        "endDate": "",
        "jobDescriptionBullets": []
    }
    
    try:
        # Extract client from subject (e.g., "TCS - New Request")
        client_match = re.search(r'^(?:Fwd:\s*)?([A-Z][A-Za-z0-9&\s]+?)\s*-\s*New Request', subject, re.IGNORECASE)
        if client_match:
            data["client"] = client_match.group(1).strip()
        
        # Extract job title from subject (text in parentheses)
        title_match = re.search(r'\(([^)]+)\)', subject)
        if title_match:
            title_text = title_match.group(1)
            # Clean up the job title (remove location prefixes like "Information Technology_USA - USA_")
            title_text = re.sub(r'^[^-]+-\s*[^_]+_', '', title_text)
            data["jobTitle"] = title_text.strip()
        
        # Extract country from subject or body
        if 'USA' in subject or 'United States' in body:
            data["country"] = "United States"
        elif 'Canada' in subject or 'Canada' in body:
            data["country"] = "Canada"
        elif 'UK' in subject or 'United Kingdom' in body:
            data["country"] = "United Kingdom"
        elif 'India' in subject or 'India' in body:
            data["country"] = "India"
        
        # Extract dates from subject or body
        date_match = re.search(r'starting\s+(\d{1,2}/\d{1,2}/\d{4})', subject)
        if date_match:
            start_date_str = date_match.group(1)
            try:
                start_date = datetime.strptime(start_date_str, '%m/%d/%Y')
                data["startDate"] = start_date.strftime('%Y-%m-%d')
            except:
                pass
        
        # Extract Request ID
        req_id_match = re.search(r'Request ID:\s*(\S+)', body)
        if req_id_match:
            data["requestId"] = req_id_match.group(1).strip()
        
        # 🆕 FIX: Extract GBaMS ID OR REQUIREMENT_ID (BOTH PATTERNS)
        # Pattern 1: GBaMS ReqID: 
        gbams_match = re.search(r'GBaMS\s*(?:ReqID|ID)[:\-\s]*(\d+)', body, re.IGNORECASE)
        if gbams_match:
            data["gbamsId"] = gbams_match.group(1).strip()
            logger.info(f"✓ GBaMS ID found: {data['gbamsId']}")
        else:
            # Pattern 2: REQUIREMENT_ID (with dash or colon)
            requirement_match = re.search(r'REQUIREMENT_ID[:\-\s]*(\d+)', body, re.IGNORECASE)
            if requirement_match:
                data["gbamsId"] = requirement_match.group(1).strip()
                logger.info(f"✓ REQUIREMENT_ID found: {data['gbamsId']}")
            else:
                # Pattern 3: Look for numeric ID in GBaMS format elsewhere
                gbams_alt_match = re.search(r'(?:GBaMS|GBAMS)[:\-\s]*(\d+)', body, re.IGNORECASE)
                if gbams_alt_match:
                    data["gbamsId"] = gbams_alt_match.group(1).strip()
                    logger.info(f"✓ Alternative GBaMS ID found: {data['gbamsId']}")
                else:
                    logger.info("⚠ No GBaMS ID or REQUIREMENT_ID found")
        
        # Extract location (city, state)
        # Extract location - check for multiple or single
        combined_text = f"{subject}\n{body}"
        
        # Extract country first (for fallback)
        raw_country = ""
        if 'USA' in subject or 'United States' in body:
            raw_country = "United States"
        elif 'Canada' in subject or 'Canada' in body:
            raw_country = "Canada"
        elif 'UK' in subject or 'United Kingdom' in body:
            raw_country = "United Kingdom"
        elif 'India' in subject or 'India' in body:
            raw_country = "India"
        
        if detect_multiple_locations(combined_text):
            # Multiple locations found
            logger.info("🌍 Multiple locations detected in fallback parser")
            try:
                normalized = normalize_locations_with_ai(text=combined_text)
                data["city"] = normalized["city"]
                data["state"] = normalized["state"]
                data["country"] = normalized["country"]
                
                logger.info(f"✅ Fallback - AI normalized: {data['city']}, {data['state']}, {data['country']}")
            except Exception as e:
                logger.error(f"❌ Fallback location normalization failed: {e}")
                data["city"] = ""
                data["state"] = ""
                data["country"] = raw_country if raw_country else ""
        else:
            # Single location - extract basic info
            raw_city = ""
            raw_state = ""
            
            # Extract location (city, state)
            location_match = re.search(r'(?:Location|Tax Work Location):\s*([^,\n]+),\s*([A-Z]{2,})', body, re.IGNORECASE)
            if location_match:
                raw_city = location_match.group(1).strip()
                raw_state = location_match.group(2).strip()
            
            # Normalize with unified AI function
            if raw_city or raw_state or raw_country:
                logger.info("📍 Single location detected in fallback parser")
                try:
                    normalized = normalize_locations_with_ai(city=raw_city, state=raw_state, country=raw_country)
                    data["city"] = normalized["city"]
                    data["state"] = normalized["state"]
                    data["country"] = normalized["country"]
                    
                    logger.info(f"✅ Fallback - AI normalized: {data['city']}, {data['state']}, {data['country']}")
                except Exception as e:
                    logger.error(f"❌ Fallback normalization failed: {e}")
                    # Simple fallback
                    data["city"] = ' '.join(w.capitalize() for w in raw_city.split()) if raw_city else ""
                    data["state"] = raw_state[:2].upper() if raw_state and len(raw_state) >= 2 else ""
                    data["country"] = raw_country[:2].upper() if raw_country and len(raw_country) >= 2 else ""
            else:
                data["city"] = ""
                data["state"] = ""
                data["country"] = ""
        # Extract bill rate - IMPROVED VERSION
        rate_patterns = [
            r'Bill Rate[:\-\s]*\$?([\d.]+)\s*-\s*\$?([\d.]+)',  # Range: $70-80 or $70 - $80
            r'Bill Rate[:\-\s]*\$?([\d.]+)',  # Single value: $65
            r'Rate[:\-\s]*\$?([\d.]+)\s*-\s*\$?([\d.]+)',  # Alternative range format
            r'Rate[:\-\s]*\$?([\d.]+)'  # Alternative single value
        ]
        
        bill_rate_found = False
        for pattern in rate_patterns:
            rate_match = re.search(pattern, body, re.IGNORECASE)
            if rate_match:
                try:
                    if len(rate_match.groups()) >= 2:
                        # Range format found (min-max)
                        min_rate = float(rate_match.group(1))
                        max_rate = float(rate_match.group(2))
                        data["billRateMin"] = min_rate
                        data["billRateMax"] = max_rate
                        logger.info(f"✓ Bill rate range found: ${min_rate}-${max_rate}")
                    else:
                        # Single rate format found
                        single_rate = float(rate_match.group(1))
                        data["billRateMax"] = single_rate
                        data["billRateMin"] = 0  # No min rate specified
                        logger.info(f"✓ Single bill rate found: ${single_rate} (min set to 0)")
                    bill_rate_found = True
                    break
                except (ValueError, IndexError) as e:
                    logger.warning(f"Rate parsing failed for pattern {pattern}: {e}")
                    continue
        
        # If no bill rate found, set both to 0
        if not bill_rate_found:
            data["billRateMin"] = 0
            data["billRateMax"] = 0
            logger.info("✓ No bill rate found, setting both to 0")
        
        # Extract start and end dates from body
        dates_match = re.search(r'Start/End Dates:\s*(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', body)
        if dates_match:
            try:
                start = datetime.strptime(dates_match.group(1), '%m/%d/%Y')
                end = datetime.strptime(dates_match.group(2), '%m/%d/%Y')
                data["startDate"] = start.strftime('%Y-%m-%d')
                data["endDate"] = end.strftime('%Y-%m-%d')
            except:
                pass
        
        # Extract MSP Owner (Client POC)
        poc_match = re.search(r'MSP Owner:\s*([^\n]+)', body)
        if poc_match:
            data["clientPOC"] = poc_match.group(1).strip()
        
        # Extract experience using improved function
        data["experience"] = extract_experience(body)
        
        # Extract duration and estimate experience
        duration_match = re.search(r'Duration:\s*(\d+)\s*months?', body, re.IGNORECASE)
        if duration_match:
            data["jobDescriptionBullets"].append(f"Duration: {duration_match.group(1)} months")
        
        # Extract job description keywords
        if 'Job Description:' in body:
            desc_start = body.find('Job Description:')
            desc_section = body[desc_start:desc_start+500]
            # Add as bullet point
            desc_clean = re.sub(r'\s+', ' ', desc_section).strip()
            if desc_clean:
                data["jobDescriptionBullets"].append(desc_clean[:200])
        
        logger.info(f"Fallback parsed: {data['jobTitle']} at {data['client']}")
        
    except Exception as e:
        logger.error(f"Fallback parser error: {e}")
    
    return data

# ===================== IMPROVED HELPER FUNCTIONS =====================

def extract_explicit_job_title(body: str) -> str:
    """
    Extract explicit job title from email body BEFORE Groq processing.
    
    Looks for patterns like:
    - "Job Title: Micro-services Architect"
    - "Role Description: Oracle DBA"
    - "Position: Senior Java Developer"
    - Standalone clear titles like "Oracle DBA", "Micro-services Architect"
    
    Returns the EXACT title found, or empty string if not found.
    """
    if not body:
        return ""
    
    # Pattern 1: Explicit "Job Title:" or "Role Description:" fields
    explicit_patterns = [
        r'(?:^|\n)\s*Job\s+Title\s*:\s*([^\n]+?)(?:\n|$)',
        r'(?:^|\n)\s*Role\s+Description\s*:\s*([^\n]+?)(?:\n|$)',
        r'(?:^|\n)\s*Position\s*:\s*([^\n]+?)(?:\n|$)',
        r'(?:^|\n)\s*Title\s*:\s*([^\n]+?)(?:\n|$)',
    ]
    
    for pattern in explicit_patterns:
        match = re.search(pattern, body, re.IGNORECASE | re.MULTILINE)
        if match:
            title = match.group(1).strip()
            # Clean up common suffixes
            title = re.sub(r'Job Description.*', '', title, flags=re.IGNORECASE).strip()
            title = re.sub(r'\s*\-.*', '', title).strip()
            title = re.sub(r'\s*\(.*', '', title).strip()
            
            # Validate: must be SPECIFIC and NOT coded/generic
            if title and len(title) > 3:
                # Check if this is a coded/generic title that should be ignored
                if is_generic_title(title):
                    logger.warning(f"⚠ Found 'Job Title:' field but it's generic/coded: '{title}' - will analyze description instead")
                    return ""  # Return empty to trigger description analysis
                else:
                    logger.info(f"✓ Explicit job title found: '{title}'")
                    return title
    
    # Pattern 2: Look for standalone job titles in the first 500 chars
    # Common patterns: "Micro-services Architect", "Oracle DBA", "Senior Python Developer"
    # These appear as standalone lines or after bullets
    standalone_patterns = [
        # With technology/domain prefix
        r'(?:^|\n)\s*(?:•|-|·)?\s*([A-Z][A-Za-z0-9\s\-\/]+(?:Architect|Developer|Engineer|DBA|Administrator|Consultant|Lead|Specialist))\s*(?:\n|$)',
        # Standalone job titles (all caps or title case)
        r'(?:^|\n)\s*([A-Z][A-Za-z\s\-]+?(?:DBA|Architect|Developer|Engineer|Administrator|Consultant))\s*(?:\n|$)',
    ]
    
    # Look in the first 1500 chars (usually where job title appears)
    search_text = body[:1500]
    
    for pattern in standalone_patterns:
        match = re.search(pattern, search_text, re.MULTILINE)
        if match:
            title = match.group(1).strip()
            # Filter out very long lines or description text
            if title and 3 < len(title) < 100 and not any(word in title.lower() for word in ['please', 'this email', 'below', 'description']):
                if not is_generic_title(title):
                    logger.info(f"✓ Standalone job title found: '{title}'")
                    return title
    
    logger.info("⚠ No explicit job title found in body")
    return ""

def is_generic_title(title: str) -> bool:
    """
    Check if a title is generic or a coded placeholder.
    Returns True if generic/invalid, False if specific.
    """
    if not title:
        return True
    
    title_lower = title.lower().strip()
    
    # Coded/placeholder patterns - REJECT THESE
    coded_patterns = [
        'information technology',
        '_usa',
        'usa_',
        'default',
        'refer jd',
        'unknown',
        'tbd',
        'to be determined'
    ]
    
    for pattern in coded_patterns:
        if pattern in title_lower:
            return True
    
    # Generic titles - REJECT if ONLY these keywords without technology/domain
    # "Senior Engineer" = GENERIC (no technology)
    # "Senior Java Engineer" = SPECIFIC (has technology)
    generic_keywords = ['developer', 'engineer', 'consultant', 'analyst', 'architect', 'lead', 'manager', 'support', 'specialist']
    
    # Check if title contains ONLY generic keywords and seniority levels
    # Remove seniority prefixes
    temp_title = re.sub(r'^\s*(Senior|Junior|Lead|Principal|Staff|Principal)\s+', '', title_lower, flags=re.IGNORECASE)
    
    # If after removing seniority, we're left with ONLY a generic keyword, it's generic
    if temp_title in generic_keywords:
        return True
    
    # Check if title is a generic keyword without any technology/domain
    # e.g., "Senior Engineer" (generic) vs "Senior Java Engineer" (specific)
    for keyword in generic_keywords:
        if title_lower == keyword or title_lower == f"senior {keyword}" or title_lower == f"junior {keyword}":
            return True
    
    return False

def generate_job_title_from_description(body: str) -> str:
    """
    If no explicit job title found, analyze job description to generate suitable title.
    This is called when is_generic_title returns True or title is empty.
    """
    if not body:
        return "Technical Professional"
    
    # Technology patterns (in priority order)
    tech_patterns = {
        r'\b(Oracle|Postgres|SQL Server|MySQL|MongoDB)\s+DBA\b': '{} DBA',
        r'\bOracle\s+DBA\b': 'Oracle DBA',
        r'\b(Micro-?services|Microservices)\s+Architect': 'Micro-services Architect',
        r'\bSnowflake.*AWS.*(?:Engineer|Developer)': 'Snowflake AWS Engineer',
        r'\bSnowflake\b': 'Snowflake Engineer',
        r'\bAWS\b.*(?:Engineer|Developer)': 'AWS Engineer',
        r'\bAzure\b.*(?:Architect|Engineer)': 'Azure Architect',
        r'\bSAP\s+FICO': 'SAP FICO Consultant',
        r'\bJava\b.*(?:Developer|Engineer)': 'Java Developer',
        r'\bPython\b.*(?:Developer|Engineer)': 'Python Developer',
    }
    
    for pattern, title_template in tech_patterns.items():
        if re.search(pattern, body, re.IGNORECASE):
            return title_template
    
    # Fallback: look for any technology keyword
    technologies = ['Oracle', 'Snowflake', 'AWS', 'Azure', 'Java', 'Python', 'Kubernetes', 'Docker']
    for tech in technologies:
        if re.search(rf'\b{tech}\b', body, re.IGNORECASE):
            # Detect job function
            if re.search(r'(dba|database|database admin)', body, re.IGNORECASE):
                return f"{tech} DBA"
            elif re.search(r'(architect|design)', body, re.IGNORECASE):
                return f"{tech} Architect"
            else:
                return f"{tech} Engineer"
    
    return "Technical Professional"
# Add this function after the existing helper functions (around line 400)
# This replaces the manual normalize functions with AI-powered normalization

# ===================== UNIFIED AI LOCATION NORMALIZER =====================
# Add this function after extract_experience function (around line 400)
# This REPLACES both normalize_location_with_ai() and extract_and_normalize_multiple_locations()

# ===================== IMPROVED REMOTE LOCATION DETECTION =====================
# Replace the detect_and_normalize_remote function with this enhanced version:

def detect_and_normalize_remote(text):
    """
    Detect if location is REMOTE and handle it specially.
    Returns normalized location dict if remote, None otherwise.
    """
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Remote indicators
    remote_patterns = [
        r'\bremote\b',
        r'\bwork from home\b',
        r'\bwfh\b',
        r'\bhome office\b',
        r'\bfully remote\b',
        r'\b100%\s*remote\b'
    ]
    
    # Check if text contains remote indicator
    is_remote = any(re.search(pattern, text_lower) for pattern in remote_patterns)
    
    if is_remote:
        logger.info("🏠 REMOTE location detected")
        
        # ✅ IMPROVED COUNTRY DETECTION
        country = "US"  # Default to US
        
        # EXPLICIT COUNTRY PATTERNS (more accurate)
        # Check for explicit "Tax Work Location:" or "Location:" fields first
        explicit_location_match = re.search(r'(?:Tax Work Location|Location):\s*([A-Z]{2,})', text, re.IGNORECASE)
        if explicit_location_match:
            location_value = explicit_location_match.group(1).strip().upper()
            if location_value in ['US', 'USA']:
                country = "US"
                logger.info("✅ Found explicit location field: US")
            elif location_value in ['INDIA', 'IND', 'IN']:
                country = "IN"
                logger.info("✅ Found explicit location field: India")
            elif location_value in ['CANADA', 'CAN', 'CA']:
                country = "CA"
                logger.info("✅ Found explicit location field: Canada")
        else:
            # Fallback: Check for country keywords in context
            # IMPORTANT: Use word boundaries to avoid false matches
            
            # Check for US indicators (with context)
            us_patterns = [
                r'\bUS\s+Default\b',
                r'\bUSA\s+Default\b',
                r'\bUnited\s+States\b',
                r'\bUS\s+location\b',
                r'\bUSA\s+location\b',
                r'\b_USA\b',
                r'\bUSA_\b'
            ]
            
            if any(re.search(pattern, text, re.IGNORECASE) for pattern in us_patterns):
                country = "US"
                logger.info("✅ Detected US from context patterns")
            
            # Check for India indicators (with context)
            elif re.search(r'\bIndia\b(?!\s+rubber)', text, re.IGNORECASE):
                # Exclude false positives like "India rubber" 
                country = "IN"
                logger.info("✅ Detected India from country name")
            
            # Check for Indian cities
            elif any(city in text_lower for city in ['bangalore', 'mumbai', 'hyderabad', 'chennai', 'pune', 'delhi', 'kolkata']):
                country = "IN"
                logger.info("✅ Detected India from city names")
            
            # Check for Canada indicators
            elif re.search(r'\bCanada\b', text, re.IGNORECASE):
                country = "CA"
                logger.info("✅ Detected Canada from country name")
            
            # Check for UK indicators
            elif re.search(r'\b(?:UK|United Kingdom|Great Britain)\b', text, re.IGNORECASE):
                country = "GB"
                logger.info("✅ Detected UK from country name")
        
        return {
            "city": "Remote",
            "state": "",
            "country": country
        }
    
    return None

def extract_location_field(text):
    """
    Extract ONLY the actual Location field from email, ignoring noise.
    Finds ALL location fields and returns the first one that's NOT a system code.
    Preserves the order they appear in the text.
    """
    if not text:
        return None
    
    # Find ALL location fields in the text with their positions
    all_locations = []
    
    # Pattern to find any location field
    location_patterns = [
        r'Tax Work Location\s*:\s*([^\n]+)',
        r'Work Location\s*:\s*([^\n]+)',
        r'Job Location\s*:\s*([^\n]+)',
        r'Location\s*:\s*([^\n]+)',
    ]
    
    for pattern in location_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE):
            location_value = match.group(1).strip()
            position = match.start()
            
            # Clean it
            # Remove ONSITE prefix
            location_value = re.sub(r'^ONSITE[-\s]*', '', location_value, flags=re.IGNORECASE)
            # Remove parenthetical work mode mentions
            location_value = re.sub(r'\s*\([^)]*(?:onsite|hybrid|remote|week)[^)]*\)', '', location_value, flags=re.IGNORECASE)
            # Remove work mode keywords after slash (e.g., "Dallas/ Remote" -> "Dallas")
            location_value = re.sub(r'\s*/\s*(?:Remote|Hybrid|Onsite)(?:\s|$)', '', location_value, flags=re.IGNORECASE)
            # Remove trailing work mode keywords (e.g., "Dallas Remote" -> "Dallas")
            location_value = re.sub(r'\s+(?:Remote|Hybrid|Onsite)(?:\s|$)', '', location_value, flags=re.IGNORECASE)
            # Convert tildes to pipes (they're location separators like / or |)
            location_value = location_value.replace(' ~ ', ' | ').replace('~', ' | ').strip()
            
            all_locations.append((position, location_value))
    
    # Sort by position in text (earliest first)
    all_locations.sort(key=lambda x: x[0])
    
    # Now find the first location that's NOT a system code
    for position, location_value in all_locations:
        if not re.match(r'^US\s+Default$|^USA\s+Default$|^Default$', location_value, re.IGNORECASE):
            logger.info(f"✅ Extracted Location field: '{location_value}'")
            return location_value
        else:
            logger.info(f"⚠️ Skipping system code location: '{location_value}'")
    
    # If we only found system codes, return None
    logger.warning("⚠️ No explicit Location field found (only system codes)")
    return None
# ===================== UPDATED AI PROMPT WITH BETTER COUNTRY DETECTION =====================
# Update the normalize_locations_with_ai function's prompt section:

# ===================== FINAL ACCURATE SOLUTION =====================
# Replace ONLY the normalize_locations_with_ai function

def normalize_locations_with_ai(text=None, city=None, state=None, country=None):
    """
    Universal AI-powered location normalizer with ACCURATE detection.
    Handles SINGLE (Raritan, NJ) and MULTIPLE (NJ | FL | AZ) locations.
    """
    try:
        cfg = load_config()
        
        # ================= DEFINITIONS =================
        # 1. Valid US State Codes (Optional strict check)
        # 2. States to blacklist from City field
        US_STATE_NAMES = {
            "alabama", "alaska", "arizona", "arkansas", "california", "colorado", 
            "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho", 
            "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", 
            "maine", "maryland", "massachusetts", "michigan", "minnesota", 
            "mississippi", "missouri", "montana", "nebraska", "nevada", 
            "new hampshire", "new jersey", "new mexico", "new york", 
            "north carolina", "north dakota", "ohio", "oklahoma", "oregon", 
            "pennsylvania", "rhode island", "south carolina", "south dakota", 
            "tennessee", "texas", "utah", "vermont", "virginia", "washington", 
            "west virginia", "wisconsin", "wyoming"
        }

        # 3. Countries to blacklist from City field
        COUNTRY_NAMES = {
            "united states", "usa", "us", "united kingdom", "uk", 
            "india", "canada", "mexico", "great britain", "america"
        }

        # Determine mode & Prepare Input
        if text:
            # Check if this is already a cleaned location field (no "Location:" prefix) or full email text
            if "Location:" in text or "location:" in text.lower():
                # This is full email text, extract the location field
                location_field = extract_location_field(text)
                
                # ✅ FIX: Explicitly handle "US Default" BEFORE AI Call
                if not location_field or re.match(r'^(US\s*Default|USA\s*Default|Default)$', location_field, re.IGNORECASE):
                    logger.info(f"✅ System Code Detected ('{location_field}') -> Defaulting to Statewide/US")
                    return {"city": "Statewide", "state": "", "country": "US"}
            else:
                # This is already a cleaned location field, use it directly
                location_field = text.strip()
                logger.info(f"✅ Using pre-cleaned location field: '{location_field}'")
            
            # Pre-process simple pattern with city name only (no state)
            # Example: "CHICAGO" -> Need to determine state
            if re.match(r'^[A-Z\s]+$', location_field) and len(location_field.split()) <= 3:
                # City name in all caps with no state - let AI handle it
                logger.info(f"🔍 City-only location detected: '{location_field}' - will use AI to determine state")
                input_data = f"Location: {location_field}"
                logger.info(f"🔍 Processing: '{location_field}'")
            else:
                # ✅ CHECK: Is this multiple locations? (contains // or | or / or multiple state codes)
                has_multiple_separators = '//' in location_field or '|' in location_field or ('/' in location_field and location_field.count('/') >= 1)
                state_codes = re.findall(r'\b[A-Z]{2}\b', location_field)
                has_multiple_states = len(set(state_codes)) > 1
                
                logger.info(f"🔍 Multiple location check:")
                logger.info(f"   - Has //, |, or /: {has_multiple_separators}")
                logger.info(f"   - State codes found: {state_codes}")
                logger.info(f"   - Has multiple states: {has_multiple_states}")
                
                # Only try simple pattern if it's clearly a single location
                if not has_multiple_separators and not has_multiple_states:
                    # ✅ NEW: PRE-PROCESS SIMPLE PATTERNS (City, ST) - Skip AI for efficiency
                    # Try Pattern 1: City, ST (with comma)
                    simple_pattern = re.match(r'^([A-Za-z\s]+),\s*([A-Z]{2})$', location_field.strip())
                    
                    # Try Pattern 2: City ST (no comma) - like "Roseville CA"
                    if not simple_pattern:
                        simple_pattern = re.match(r'^([A-Za-z\s]+)\s+([A-Z]{2})$', location_field.strip())
                    
                    if simple_pattern:
                        city_name = simple_pattern.group(1).strip()
                        state_code = simple_pattern.group(2).strip().upper()
                        
                        # Validate it's a real city (not a state name)
                        if city_name.lower() not in US_STATE_NAMES:
                            formatted_city = ' '.join(w.capitalize() for w in city_name.split())
                            logger.info(f"✅ Simple pattern detected - skipping AI: {formatted_city}, {state_code}")
                            return {"city": formatted_city, "state": state_code, "country": "US"}
                else:
                    logger.info(f"🌍 Multiple locations detected - will use AI")
                
                # Multiple locations or complex pattern - clean and send to AI
                # Clean: Replace double-slashes and slashes with pipes
                location_clean = location_field.replace('//', ' | ').replace('/ ', ' | ').replace('/', ' | ')
                location_clean = re.sub(r'\bONSITE[-\s]*', '', location_clean, flags=re.IGNORECASE)
                location_clean = re.sub(r'[-\s]*100%\s*ONSITE.*$', '', location_clean, flags=re.IGNORECASE)
                location_clean = re.sub(r'[-\s]*ONSITE.*$', '', location_clean, flags=re.IGNORECASE)
                # Remove work mode keywords (Remote, Hybrid, Onsite) that appear as separate locations
                location_clean = re.sub(r'\|\s*(?:Remote|Hybrid|Onsite)\s*(?:\||$)', '', location_clean, flags=re.IGNORECASE)
                location_clean = re.sub(r'^\s*(?:Remote|Hybrid|Onsite)\s*\|', '', location_clean, flags=re.IGNORECASE)
                location_clean = location_clean.strip()
                
                input_data = f"Location: {location_clean}"
                logger.info(f"🔍 Processing: '{location_clean}'")
        else:
            # Fallback mode check
            if city and re.match(r'^(US\s*Default|USA\s*Default|Default)$', city, re.IGNORECASE):
                 return {"city": "Statewide", "state": "", "country": "US"}
                 
            input_data = f"City: \"{city}\"\nState: \"{state}\"\nCountry: \"{country}\""
            logger.info(f"🔍 Normalizing fields")
        
        # Initialize Client
        client = Groq(api_key=cfg['groq_key'])

        # ================= BALANCED PROMPT =================
        prompt = f"""You are a strict data extraction engine. Normalize the location data.

INPUT:
{input_data}


CORE RULES:
1. Extract ONLY what is explicitly present in the input
2. DO NOT infer, guess, or add information not in the input
3. For single location: Return city and state separately
4. For multiple locations: Return cities as comma-separated, states as comma-separated

PARSING PATTERNS:

Single Location Formats:
- "City, ST" → Extract city and 2-letter state code
  Example: "Austin, TX" → {{"city": "Austin", "state": "TX", "country": "US"}}
  
- "City ST" (no comma) → Extract city and 2-letter state code
  Example: "Roseville CA" → {{"city": "Roseville", "state": "CA", "country": "US"}}
  
- "CITY" (city name only, all caps) → Use city-to-state mapping below
  Example: "CHICAGO" → {{"city": "Chicago", "state": "IL", "country": "US"}}

Multiple Location Formats (CRITICAL):
- Split on "/" or "|" separators
- Each segment is one location
- Join ALL cities with COMMAS: "City1, City2, City3"
- Join ALL states with COMMAS: "ST1, ST2, ST3"
  
  Example 1 (with slash): "Cleveland, OH/Buffalo, NY/Albany, NY"
  → Parse 3 locations: (Cleveland,OH), (Buffalo,NY), (Albany,NY)
  → Extract cities: Cleveland, Buffalo, Albany
  → Extract states: OH, NY, NY
  → {{"city": "Cleveland, Buffalo, Albany", "state": "OH, NY, NY", "country": "US"}}
  
  Example 2 (with pipe): "Chicago, IL | Dallas, TX"
  → Parse 2 locations: (Chicago,IL), (Dallas,TX)
  → Extract cities: Chicago, Dallas
  → Extract states: IL, TX
  → {{"city": "Chicago, Dallas", "state": "IL, TX", "country": "US"}}

State-Only Format:
- If input is ONLY state codes (e.g., "NJ | FL | AZ")
- Set each city as "Statewide"
  Example: "NJ | FL | AZ" → {{"city": "Statewide, Statewide, Statewide", "state": "NJ, FL, AZ", "country": "US"}}

CITY-TO-STATE MAPPING (use only when city appears without state):
Chicago→IL, Boston→MA, New York→NY, Los Angeles→CA, San Francisco→CA,
Seattle→WA, Atlanta→GA, Denver→CO, Phoenix→AZ, Dallas→TX, Houston→TX,
Philadelphia→PA, Pittsburgh→PA, Cleveland→OH, Buffalo→NY, Albany→NY, Columbus→OH

CLEANING RULES:
- Remove these suffixes: "100% ONSITE", "ONSITE FROM DAY 1", "(onsite)", "(remote)", "(hybrid)", "(5x/week onsite)"
- Remove parenthetical location modifiers before parsing

VALIDATION CHECKS:
✓ State codes MUST be 2 uppercase letters (e.g., TX, NY, CA)
✓ State codes MUST be valid US states (NOT "US" or "UN")
✓ Country code MUST be 2 uppercase letters (US, CA, IN, UK)
✓ City field MUST contain city names, NEVER state codes
✓ If multiple locations, number of cities MUST equal number of states
✓ Cities separated by COMMA AND SPACE: ", "
✓ States separated by COMMA AND SPACE: ", "

OUTPUT FORMAT (strict JSON):
{{
  "city": "string",
  "state": "string", 
  "country": "string"
}}

For single location:
- city: "CityName"
- state: "ST"
- country: "US"

For multiple locations:
- city: "City1, City2, City3" (COMMA-SEPARATED)
- state: "ST1, ST2, ST3" (COMMA-SEPARATED)
- country: "US"

STEP-BY-STEP PROCESS:
1. Read the input
2. Remove any suffixes (ONSITE, remote, etc.)
3. Check if multiple locations (contains "/" or "|" between city-state pairs)
4. If multiple: 
   - Split into individual locations
   - Extract city from each location
   - Extract state from each location
   - Join all cities with ", " (comma-space)
   - Join all states with ", " (comma-space)
5. If single: return single city and single state
6. Validate: city names in city field, 2-letter codes in state field
7. Return JSON

EXAMPLES:

Input: "Austin, TX"
Output: {{"city": "Austin", "state": "TX", "country": "US"}}
Final Display: "Austin, TX"

Input: "CHICAGO"
Output: {{"city": "Chicago", "state": "IL", "country": "US"}}
Final Display: "Chicago, IL"

Input: "Cleveland, OH/Buffalo, NY/Albany, NY/Columbus OH"
Process: 
  Step 1: Split by "/" → ["Cleveland, OH", "Buffalo, NY", "Albany, NY", "Columbus OH"]
  Step 2: Extract cities → ["Cleveland", "Buffalo", "Albany", "Columbus"]
  Step 3: Extract states → ["OH", "NY", "NY", "OH"]
  Step 4: Join cities with ", " → "Cleveland, Buffalo, Albany, Columbus"
  Step 5: Join states with ", " → "OH, NY, NY, OH"
Output: {{"city": "Cleveland, Buffalo, Albany, Columbus", "state": "OH, NY, NY, OH", "country": "US"}}
Final Display: "Cleveland, Buffalo, Albany, Columbus | OH, NY, NY, OH | US"

Input: "Roseville CA"
Output: {{"city": "Roseville", "state": "CA", "country": "US"}}
Final Display: "Roseville, CA"

Input: "Chicago, IL | Dallas, TX"
Process:
  Step 1: Split by "|" → ["Chicago, IL", "Dallas, TX"]
  Step 2: Extract cities → ["Chicago", "Dallas"]
  Step 3: Extract states → ["IL", "TX"]
  Step 4: Join cities with ", " → "Chicago, Dallas"
  Step 5: Join states with ", " → "IL, TX"
Output: {{"city": "Chicago, Dallas", "state": "IL, TX", "country": "US"}}
Final Display: "Chicago, Dallas | IL, TX | US"

Input: "Pittsburgh, PA- 100% ONSITE FROM DAY 1"
Process: Remove "- 100% ONSITE FROM DAY 1"
Output: {{"city": "Pittsburgh", "state": "PA", "country": "US"}}
Final Display: "Pittsburgh, PA"

Input: "Dallas, TX | Miami, FL | Phoenix, AZ"
Process:
  Step 1: Split by "|" → ["Dallas, TX", "Miami, FL", "Phoenix, AZ"]
  Step 2: Extract cities → ["Dallas", "Miami", "Phoenix"]
  Step 3: Extract states → ["TX", "FL", "AZ"]
  Step 4: Join cities with ", " → "Dallas, Miami, Phoenix"
  Step 5: Join states with ", " → "TX, FL, AZ"
Output: {{"city": "Dallas, Miami, Phoenix", "state": "TX, FL, AZ", "country": "US"}}
Final Display: "Dallas, Miami, Phoenix | TX, FL, AZ | US"

Input: "NJ | FL | AZ"
Output: {{"city": "Statewide, Statewide, Statewide", "state": "NJ, FL, AZ", "country": "US"}}
Final Display: "Statewide, Statewide, Statewide | NJ, FL, AZ | US"

CRITICAL FORMAT RULES:
✓ Single location: city and state as simple strings
✓ Multiple locations: 
  - Cities joined with ", " (comma-space) → "City1, City2, City3"
  - States joined with ", " (comma-space) → "ST1, ST2, ST3"
  - NO PIPES in the JSON values themselves
  - The system will add pipes: Cities | States | Country

REMEMBER:
- Use COMMA-SPACE (", ") to separate multiple cities
- Use COMMA-SPACE (", ") to separate multiple states
- DO NOT use pipes ("|") inside the city or state fields
- Return ONLY ONE JSON object (not multiple objects)
- No explanation, no markdown, no code blocks
- Start with {{ and end with }}

CRITICAL: For multiple locations like "Chicago, IL | Dallas, TX", you MUST return:
{{
  "city": "Chicago, Dallas",
  "state": "IL, TX",
  "country": "US"
}}

NOT multiple separate objects like:
{{"city": "Chicago, IL", "state": "IL", "country": "US"}}
{{"city": "Dallas, TX", "state": "TX", "country": "US"}}

RESPOND WITH EXACTLY ONE JSON OBJECT:
{{
  "city": "City or City1, City2, City3",
  "state": "ST or ST1, ST2, ST3",
  "country": "US"
}}
"""

        # Call Groq
        logger.info(f"🤖 Sending to AI: {prompt[:200]}...")  # Log first 200 chars of prompt
        
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a deterministic data extractor. You MUST return EXACTLY ONE JSON object, even for multiple locations. For multiple locations, join cities with commas in ONE field, join states with commas in ONE field. NEVER return multiple separate JSON objects. State codes must be real states (e.g. NY, CA), NOT 'US'."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.0,
            max_tokens=300,
            timeout=15
        )
        
        raw_response = chat_completion.choices[0].message.content.strip()
        logger.info(f"🤖 AI Raw Response: {raw_response}")  # Log what AI returned
        
        # Robust JSON cleaning
        raw_response = raw_response.replace("```json", "").replace("```", "").strip()
        if '{' in raw_response and '}' in raw_response:
            start_idx = raw_response.index('{')
            end_idx = raw_response.rindex('}') + 1
            raw_response = raw_response[start_idx:end_idx]
        
        logger.info(f"🧹 Cleaned JSON: {raw_response}")  # Log cleaned JSON
        
        result = json.loads(raw_response)
        logger.info(f"📊 Parsed Result: {result}")  # Log parsed result

        normalized = {
            "city": result.get("city", "").strip(),
            "state": result.get("state", "").strip().upper(),
            "country": result.get("country", "").strip().upper()
        }
        
        # ================= ROBUST POST-PROCESSING =================
        
        # 1. Flexible Splitter
        # ================= ROBUST POST-PROCESSING =================
        
        # 1. Flexible Splitter
        def robust_split(text_val):
            if not text_val: return []
            return [x.strip() for x in re.split(r'[|,]', str(text_val)) if x.strip()]

        clean_cities = [(' '.join(w.capitalize() for w in c.split())) for c in robust_split(result.get("city", ""))]
        clean_states = [s.upper() for s in robust_split(result.get("state", "")) if len(s.strip()) == 2]

        # Handle Count Mismatch (Safety)
        if len(clean_states) == 1 and len(clean_cities) > 1:
            clean_states = [clean_states[0]] * len(clean_cities)
        elif len(clean_cities) == 1 and len(clean_states) > 1:
            clean_states = [clean_states[0]]

        # 1. Final Formatting: Join with commas inside the blocks
        final_cities = ", ".join(clean_cities) if clean_cities else "Statewide"
        final_states = ", ".join(clean_states)
        
        country_raw = str(result.get("country", "US")).upper()
        final_country = "US" if "US" in country_raw or "AMERICA" in country_raw else country_raw

        # 2. THE FINAL LOGGER: verified pipe separation
        # Output: City 1, City 2 | State 1, State 2 | Country
        logger.info(f"📍 FINAL LOCATION OUTPUT: {final_cities} | {final_states} | {final_country}")

        # 3. Return: Combined string in 'city' field for database compatibility
        return {
            "city": f"{final_cities} | {final_states} | {final_country}",
            "state": "", # Set to empty because combined string is in the city field
            "country": final_country
        }
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return fallback_location_normalization(text, city, state, country)
def fallback_location_normalization(text=None, city=None, state=None, country=None):
    """
    Simple fallback location normalization when AI fails.
    Uses basic string cleaning without AI.
    """
    logger.info("⚠️ Using fallback location normalization (no AI)")
    
    result = {
        "city": "",
        "state": "",
        "country": "US"
    }
    
    try:
        if text:
            # Extract from text
            location_field = extract_location_field(text)
            
            if location_field:
                # Check for REMOTE
                if re.search(r'\bREMOTE\b', location_field, re.IGNORECASE):
                    result["city"] = "Remote"
                    result["state"] = ""
                    result["country"] = "US"
                    logger.info("✅ Fallback: Detected REMOTE")
                    return result
                
                # Try to extract city, state patterns
                # Pattern: "City, ST" or "City1, ST1 / City2, ST2"
                matches = re.findall(r'([A-Za-z\s]+),\s*([A-Z]{2})', location_field)
                
                if matches:
                    if len(matches) > 1:
                        # Multiple locations
                        cities = [m[0].strip() for m in matches]
                        states = [m[1].strip().upper() for m in matches]
                        
                        # Format cities with proper capitalization
                        formatted_cities = [' '.join(w.capitalize() for w in c.split()) for c in cities]
                        
                        # Remove duplicate states while preserving order
                        unique_states = []
                        for state in states:
                            if state not in unique_states:
                                unique_states.append(state)
                        
                        result["city"] = " | ".join(formatted_cities)
                        result["state"] = " | ".join(unique_states)
                        result["country"] = "US"
                        
                        logger.info(f"✅ Fallback: Multiple locations - {result['city']} ({result['state']})")
                    else:
                        # Single location
                        result["city"] = ' '.join(w.capitalize() for w in matches[0][0].strip().split())
                        result["state"] = matches[0][1].strip().upper()
                        result["country"] = "US"
                        logger.info(f"✅ Fallback: Single location - {result['city']}, {result['state']}")
                else:
                    logger.warning("⚠️ Fallback: No city-state pattern found")
        else:
            # Use provided fields
            if city:
                result["city"] = ' '.join(w.capitalize() for w in city.split())
            if state:
                result["state"] = state[:2].upper() if len(state) >= 2 else state.upper()
            if country:
                result["country"] = country[:2].upper() if len(country) >= 2 else "US"
            
            logger.info(f"✅ Fallback: Using provided fields")
    
    except Exception as e:
        logger.error(f"❌ Fallback normalization error: {e}")
    
    return result
def detect_multiple_locations(text):
    """
    Check if text contains multiple locations.
    NOW extracts ONLY the Location field first.
    """
    if not text:
        return False
    
    # ✅ STEP 1: Extract ONLY the Location field
    location_field = extract_location_field(text)
    
    if not location_field:
        return False
    
    # ✅ STEP 2: Check for separators
    # Added comma (,) to indicators because "City State, City State" uses it
    indicators = [';', ' and ', ' or ', ' / ', '/ ', '|', ',']
    
    has_indicators = any(indicator in location_field for indicator in indicators)
    
    # Check for multiple state codes (e.g., "NJ" and "FL")
    # Matches 2-letter uppercase words that are likely states
    state_matches = re.findall(r'\b[A-Z]{2}\b', location_field)
    unique_states = len(set(state_matches))
    
    result = has_indicators or unique_states > 1
    
    if result:
        logger.info(f"✅ Multiple locations detected in: '{location_field}'")
    else:
        logger.info(f"📍 Single location detected: '{location_field}'")
    
    return result

# ===================== END OF UNIFIED LOCATION NORMALIZER =====================
# ===================== IMPROVED GROQ PARSER - JOB TITLE FIX =====================
def parse_with_groq(subject: str, body: str) -> dict:
    """
    Enhanced Groq parser with HIGHLY ACCURATE job title extraction and robust error handling.
    """
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Initialize Groq client
            cfg = load_config()
            client = Groq(api_key=cfg['groq_key'])

            # Extract explicit job titles BEFORE calling Groq
            explicit_job_title = extract_explicit_job_title(body)
            logger.info(f"📋 Pre-extracted explicit job title from body: '{explicit_job_title}'")
            
            # Truncate body if too long (keep most relevant parts)
            body_truncated = body[:20000]
            
            system_prompt = """You are a specialized recruitment data extraction expert with DEEP EXPERTISE in identifying accurate, specific job titles.
Your PRIMARY SKILL is analyzing job descriptions and extracting the MOST SPECIFIC, ACCURATE job title that reflects the actual role.

CRITICAL RULES FOR JOB TITLE EXTRACTION:
1. NEVER use generic titles like "Developer", "Engineer", "Consultant" alone
2. ALWAYS include the primary technology/platform in the title (e.g., "Java Developer", "Oracle DBA", "AWS Architect")
3. Prefer SPECIFIC titles over GENERAL ones
4. If multiple technologies mentioned, use the PRIMARY/DOMINANT one in the title
5. Include seniority level if clearly specified (Senior, Lead, Principal)

You are also a professional job description writer who can expand limited information into complete 400-600 word job descriptions.
You MUST respond with valid JSON only. No markdown, no code blocks, no preambles."""
            
            user_prompt = f"""
Extract job details from this recruitment email and return ONLY valid JSON.

🚨 ABSOLUTE REQUIREMENT - JOB TITLE MUST BE SPECIFIC:
YOU MUST NEVER return a job title that is just "Analyst", "Developer", "Engineer", "Architect", or "Consultant" WITHOUT a technology/domain prefix.
IF you find yourself wanting to return just "Analyst" or "Engineer", STOP and analyze the job description to find the SPECIFIC technology/domain.

Examples of FORBIDDEN job titles: "Analyst", "Senior Analyst", "Engineer", "Developer", "Lead Engineer"
Examples of REQUIRED job titles: "Data Analyst", "Java Developer", "DevOps Engineer", "Cloud Architect"

🎯 CRITICAL INSTRUCTION FOR JOB TITLE - READ CAREFULLY:

**Pre-extracted title from email**: "{explicit_job_title}"

**YOUR JOB TITLE EXTRACTION LOGIC:**

STEP 1: Check the pre-extracted title
- If it's SPECIFIC (contains technology/platform like "Oracle DBA", "Java Developer", "Salesforce Architect"), USE IT EXACTLY
- If it's GENERIC/CODED (like "Information Technology_USA", "Developer", "Engineer" alone), IGNORE IT and go to STEP 2

STEP 2: If pre-extracted title is generic/empty, ANALYZE the job description to identify:

A) **Primary Technology/Platform** (pick the MOST IMPORTANT one):
   - Cloud: AWS, Azure, GCP, Snowflake
   - Database: Oracle, PostgreSQL, MySQL, MongoDB, SQL Server
   - Programming: Java, Python, .NET, Node.js, React, Angular
   - Enterprise: SAP, Salesforce, ServiceNow, Workday
   - Infrastructure: Kubernetes, Docker, Terraform, Ansible
   - Architecture: Microservices, Cloud Architecture, Solutions Architecture

B) **Core Role Function**:
   - DBA / Database Administrator
   - Solutions Architect / Cloud Architect
   - Developer / Software Engineer
   - DevOps Engineer
   - Data Engineer
   - Full Stack Developer
   - Consultant
   - Administrator

C) **Seniority Level (if mentioned)**:
   - Senior, Lead, Principal, Staff, Junior

**COMBINE THEM**: [Seniority] + [PRIMARY Technology] + [Role Function]

⚠️ **CRITICAL LENGTH RULE**: Job title MUST be SHORT and CONCISE (max 4-6 words)
- Pick ONLY the PRIMARY/MOST IMPORTANT technology
- Do NOT list multiple technologies in the title
- Do NOT include detailed requirements or expertise areas
- Keep it simple and searchable

**EXAMPLES OF GOOD vs BAD JOB TITLES:**

✅ GOOD (Specific AND Concise):
- "Senior Java Developer" ← Perfect length
- "Oracle Database Administrator" ← Good
- "AWS Solutions Architect" ← Excellent
- "Salesforce Technical Consultant" ← Good
- "Snowflake Data Engineer" ← Perfect
- "Cloud Data Architect" ← Concise, clear
- "Azure DevOps Engineer" ← Good length
- "PostgreSQL DBA" ← Perfect
- "React Frontend Developer" ← Good

❌ BAD (Too Generic OR Too Long):
- "Developer" ← TOO GENERIC: Missing technology
- "Senior Engineer" ← TOO GENERIC: Missing technology/domain
- "Cloud Data Architect with MS SQL, SSIS, PowerBI, MongoDB, and AWS/Azure expertise" ← TOO LONG: Should be "Cloud Data Architect"
- "Senior Full Stack Developer with React, Node.js, and AWS experience" ← TOO LONG: Should be "Senior Full Stack Developer"
- "Information Technology_USA_Developer" ← System code, not a title
- "Java Developer with Spring Boot and Microservices" ← TOO LONG: Should be "Java Developer"

**STEP 3: Title Accuracy AND Length Check**
Before finalizing, ask yourself:
- Does this title tell me WHAT TECHNOLOGY/PLATFORM the person will work with?
- Is this title SPECIFIC enough that a recruiter knows what skills to look for?
- Would a candidate understand EXACTLY what role this is?
- **IS THIS TITLE SHORT AND CONCISE (4-6 words maximum)?**

If answer to any is NO, make the title MORE SPECIFIC or SHORTER.

📋 **SPECIAL RULES**:
- **MAXIMUM 4-6 WORDS for job title** - This is CRITICAL
- Pick ONLY the PRIMARY technology (if role uses SQL + MongoDB + AWS, pick the MOST important one)
- For "DBA" roles: Include database type (Oracle, PostgreSQL, etc.) - e.g., "Oracle DBA"
- For "Architect" roles: Include domain (AWS, Azure, Solutions, Cloud, Data, etc.) - e.g., "Cloud Data Architect"
- For "Developer" roles: Include primary language/framework (Java, Python, .NET, etc.) - e.g., "Java Developer"
- For "Engineer" roles: Include domain (DevOps, Data, Cloud, etc.) - e.g., "Data Engineer"
- **DO NOT list multiple technologies** - Pick the primary one only

**STEP 4: GBaMS ID / REQUIREMENT_ID EXTRACTION**
Look for BOTH patterns:
1. "GBaMS ID:" or "GBaMS ReqID:" 
2. "REQUIREMENT_ID:" or "REQUIREMENT_ID-" (with dash or colon)
Use whichever is found. This field goes into "gbamsId" key.

**STEP 4.5: EXPERIENCE EXTRACTION - CRITICAL**
Look for "Experience Required:" field in the email.

**CONVERSION RULES - FOLLOW EXACTLY:**
- "10 & Above" -> Convert to "10+"
- "8 & Above" -> Convert to "8+"
- "6 & Above" -> Convert to "6+"
- ANY "X & Above" -> Convert to "X+"
- "6-8" -> Keep as "6-8"
- "8-10" -> Keep as "8-10"
- "5+ years" -> Extract as "5+"
- "10+" -> Keep as "10+"
- "N/A" -> Extract as "N/A"

**CRITICAL**: ALWAYS convert "& Above" format to "+" format. Never return "10 & Above" - it MUST be "10+".

If not found in "Experience Required:" field, search in "Required Skills" or "Job Description" (e.g., "10+ years of experience").

Format should ALWAYS be:
- Single number with +: "10+", "8+", "6+"
- Range: "6-8", "8-10", "5-7"
- N/A: "N/A"

**STEP 4.6: WORK MODE EXTRACTION**
Determine from job description:
- If mentions "ONSITE", "100% ONSITE", "on-site", "FROM DAY 1" -> workMode: "Onsite"
- If mentions "REMOTE", "work from home", "WFH" -> workMode: "Remote"  
- If mentions "HYBRID", "2-3 days onsite", "flexible" -> workMode: "Hybrid"
- If unclear, default to "Onsite"

**STEP 5: jobDescriptionDetailed - CRITICAL INSTRUCTIONS FOR ACCURACY**:

🎯 **JOB DESCRIPTION PROCESSING RULES - READ CAREFULLY**:

**RULE 1: DETERMINE JOB DESCRIPTION LENGTH**
Count the number of lines in the "Role Description" or "Job Description" section:
- If SHORT (less than 5 lines): Follow SHORT JD PROCESS
- If LONG (5 or more lines): Follow LONG JD PROCESS

**SHORT JD PROCESS (< 5 lines):**
YOU MUST preserve the EXACT content from the email with NO expansion, NO generation, NO adding information.
ONLY improve the formatting:

1. Take the EXACT text from the email's job description section
2. Clean up formatting issues:
   - Split text that uses "|" (pipe) into separate bullet points
   - Fix obvious typos or spacing issues
   - Convert to proper HTML format with <ul><li> tags
3. DO NOT add any new information
4. DO NOT expand or elaborate
5. DO NOT write summaries or introductions
6. Keep it EXACTLY as written in the email, just formatted better

**Example SHORT JD:**
Email text: "5 years Java experience| Spring Boot| Microservices| AWS deployment| Must know REST APIs"

Output HTML:
```html
<ul>
<li>5 years Java experience</li>
<li>Spring Boot</li>
<li>Microservices</li>
<li>AWS deployment</li>
<li>Must know REST APIs</li>
</ul>
```

**LONG JD PROCESS (≥ 5 lines):**
Read the ENTIRE job description carefully and create a well-structured summary (~250 words) that captures ALL key points:

1. **Extract ALL information** from these sections in the email:
   - Role Description / Role Descriptions
   - Essential Skills
   - Desirable Skills / Preferred Qualifications
   - Responsibilities
   - Job Description
   - Required Skills / Must Have Skills

2. **Organize into proper HTML structure:**
   ```html
   <p><strong>Role Overview:</strong></p>
   <p>[1-2 sentence summary of the role]</p>
   
   <p><strong>Key Responsibilities:</strong></p>
   <ul>
   <li>[Responsibility 1 from email]</li>
   <li>[Responsibility 2 from email]</li>
   <li>[Continue with all responsibilities mentioned]</li>
   </ul>
   
   <p><strong>Required Skills:</strong></p>
   <ul>
   <li>[Skill 1 from email]</li>
   <li>[Skill 2 from email]</li>
   <li>[Continue with all skills mentioned]</li>
   </ul>
   
   <p><strong>Qualifications:</strong></p>
   <ul>
   <li>[Years of experience mentioned]</li>
   <li>[Certifications mentioned]</li>
   <li>[Education mentioned]</li>
   </ul>
   
   <p><strong>Preferred Skills:</strong></p>
   <ul>
   <li>[Desirable skill 1 from email]</li>
   <li>[Desirable skill 2 from email]</li>
   </ul>
   ```

3. **CRITICAL ACCURACY RULES:**
   - Use ONLY information explicitly stated in the email
   - DO NOT make up or infer skills not mentioned
   - DO NOT add generic statements like "The ideal candidate will..."
   - Preserve specific tool names, technologies, and version numbers EXACTLY as written
   - Keep technical terms EXACTLY as they appear (don't change "Kubernetes" to "container orchestration")
   - Aim for ~250 words total
   - Split pipe-separated items (e.g., "AWS| Azure| GCP" becomes 3 separate <li> items)

4. **Formatting fixes:**
   - Fix obvious typos
   - Split run-on lists into proper bullets
   - Clean up spacing issues
   - Ensure proper grammar in bullet points

**CRITICAL RULES FOR BOTH:**
- Use proper HTML tags: <p>, <strong>, <ul>, <li>
- NO markdown syntax (no **, no ##, no bullets like •)
- All HTML tags must have closing tags
- Be ACCURATE - use only what's in the email
- DO NOT add generic corporate speak
- Preserve technical accuracy above all else

🔍 **ANALYZE THIS EMAIL**:

Email Subject: {subject}

Email Body:
{body_truncated}

**NOW EXTRACT AND RETURN JSON**:

Return ONLY a valid JSON object with these exact keys:
{{
  "client": "extracted from subject",
  "jobTitle": "THE MOST SPECIFIC, ACCURATE JOB TITLE following all rules above",
  "city": "city name ONLY if real city",
  "state": "two-letter state code ONLY if valid",
  "country": "United States, Canada, United Kingdom, India, etc.",
  "workMode": "Remote|Hybrid|Onsite",
  "billRateMax": "number or 0",
  "billRateMin": "number or 0",
  "experience": "MUST be in '+' format (e.g., '10+', '8+') or range format (e.g., '6-8'). NEVER use '& Above' format.",
  "requestId": "Job ID or Request ID",
  "gbamsId": "GBaMS ID or REQUIREMENT_ID",
  "clientPOC": "MSP Owner name",
  "startDate": "YYYY-MM-DD format",
  "endDate": "YYYY-MM-DD format",
  "jobDescriptionDetailed": "Follow SHORT or LONG JD process as described above. For SHORT JDs: preserve exact content with better formatting. For LONG JDs: comprehensive ~250 word summary in clean HTML format."
}}

Today's date: {datetime.now().strftime('%Y-%m-%d')}

🚨 CRITICAL REMINDERS:
1. jobTitle MUST be SPECIFIC with technology/platform (never just "Analyst" or "Engineer")
2. experience MUST use "+" format: Convert "10 & Above" to "10+", "8 & Above" to "8+", etc.
3. jobDescriptionDetailed: 
   - SHORT JD (< 5 lines): Keep EXACT content from email, just format better
   - LONG JD (≥ 5 lines): Create accurate ~250 word summary with ALL key points from email
4. ACCURACY IS CRITICAL: Use ONLY information from the email, do NOT add or infer

RESPOND WITH JSON ONLY. NO MARKDOWN CODE BLOCKS. NO PREAMBLES. START WITH {{ AND END WITH }}.
"""

            # Call Groq API with retry logic
            try:
                chat_completion = client.chat.completions.create(
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    model="llama-3.3-70b-versatile",
                    temperature=0.1,  # Lower temperature for more consistent, accurate extraction
                    max_tokens=3500,
                    timeout=30  # Add timeout
                )
            except Exception as api_error:
                logger.error(f"❌ Groq API call failed (attempt {attempt + 1}/{max_retries}): {api_error}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s
                    logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error("❌ All Groq API retry attempts failed, using fallback parser")
                    return {}

            raw_text = chat_completion.choices[0].message.content
            
            # Validation: Check if response is empty or None
            if not raw_text or raw_text.strip() == "":
                logger.error(f"❌ Groq returned empty response (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    logger.info("⏳ Retrying with fallback prompt...")
                    time.sleep(2)
                    continue
                else:
                    return {}
            
            logger.info(f"📄 Raw Groq response length: {len(raw_text)} chars")

            # Clean up the response - ENHANCED CLEANING
            raw_text = raw_text.strip()
            
            # Remove any text before first {
            if '{' in raw_text:
                start_idx = raw_text.index('{')
                if start_idx > 0:
                    removed_text = raw_text[:start_idx]
                    logger.info(f"🧹 Removed preamble: '{removed_text[:50]}...'")
                    raw_text = raw_text[start_idx:]
            
            # Remove any text after last }
            if '}' in raw_text:
                end_idx = raw_text.rindex('}') + 1
                if end_idx < len(raw_text):
                    removed_text = raw_text[end_idx:]
                    logger.info(f"🧹 Removed postamble: '{removed_text[:50]}...'")
                    raw_text = raw_text[:end_idx]
            
            # Remove markdown code blocks if present
            if "```" in raw_text:
                parts = raw_text.split("```")
                for part in parts:
                    if "{" in part and "}" in part:
                        clean_part = part.strip()
                        if clean_part.startswith("json"):
                            clean_part = clean_part[4:].strip()
                        try:
                            json.loads(clean_part)
                            raw_text = clean_part
                            logger.info("🧹 Extracted JSON from markdown code block")
                            break
                        except:
                            continue
            
            # Final validation before parsing
            if not raw_text.startswith('{') or not raw_text.endswith('}'):
                logger.error(f"❌ Response doesn't look like valid JSON (attempt {attempt + 1}/{max_retries})")
                logger.error(f"First 100 chars: {raw_text[:100]}")
                logger.error(f"Last 100 chars: {raw_text[-100:]}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return {}
            
            # Parse JSON with better error handling
            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError as json_err:
                logger.error(f"❌ JSON parsing failed (attempt {attempt + 1}/{max_retries}): {json_err}")
                logger.error(f"Error at line {json_err.lineno}, column {json_err.colno}")
                logger.error(f"Problematic text: {raw_text[max(0, json_err.pos-50):json_err.pos+50]}")
                
                if attempt < max_retries - 1:
                    logger.info("⏳ Retrying with cleaned prompt...")
                    time.sleep(2)
                    continue
                else:
                    return {}

            # ===================== POST-PROCESSING =====================
            
            # Validate that we got a dict
            if not isinstance(data, dict):
                logger.error(f"❌ Parsed data is not a dictionary: {type(data)}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return {}
            
            # 1. JOB TITLE - Enhanced with specificity check AND length truncation
            job_title = data.get("jobTitle", "").strip()
            
            if explicit_job_title and explicit_job_title.strip() and not is_generic_title(explicit_job_title):
                job_title = explicit_job_title
                logger.info(f"✅ Using pre-extracted job title: '{job_title}'")
            else:
                if job_title:
                    # Clean up generic patterns
                    job_title = re.sub(r'^Information Technology.*?-\s*', '', job_title, flags=re.IGNORECASE)
                    job_title = re.sub(r'^[A-Z]{3,}\s*-\s*', '', job_title)
                    job_title = re.sub(r'\s*\(USA\)|\s*\(US\)', '', job_title)
                    job_title = job_title.strip()
                    
                    # 🔧 NEW: Truncate overly long titles with "with/and" patterns
                    # "Cloud Data Architect with MS SQL, SSIS..." → "Cloud Data Architect"
                    original_title = job_title
                    
                    # Remove everything after " with ", " and ", commas if title is too long
                    if len(job_title) > 50 or ' with ' in job_title.lower() or job_title.count(',') > 0:
                        # Split at " with " or " and " or first comma
                        for split_pattern in [r'\s+with\s+', r'\s+and\s+', r',']:
                            parts = re.split(split_pattern, job_title, maxsplit=1, flags=re.IGNORECASE)
                            if len(parts) > 1:
                                job_title = parts[0].strip()
                                logger.info(f"✂️ Truncated long title: '{original_title}' → '{job_title}'")
                                break
                    
                    # Additional cleanup: Remove parentheses with details
                    if '(' in job_title:
                        job_title = re.sub(r'\s*\([^)]*\)', '', job_title).strip()
                        logger.info(f"✂️ Removed parenthetical details: '{job_title}'")
                    
                    # Final length check: If still > 8 words, keep only first 6 words
                    words = job_title.split()
                    if len(words) > 8:
                        job_title = ' '.join(words[:6])
                        logger.warning(f"✂️ Title still too long, truncated to first 6 words: '{job_title}'")
                    
                    # Check if title is still too generic
                    generic_check = ['developer', 'engineer', 'consultant', 'architect', 'analyst', 'administrator']
                    is_still_generic = any(
                        job_title.lower() == keyword or 
                        job_title.lower() == f"senior {keyword}" or 
                        job_title.lower() == f"lead {keyword}" or
                        job_title.lower() == f"principal {keyword}"
                        for keyword in generic_check
                    )
                    
                    if is_still_generic:
                        logger.warning(f"⚠️ Groq generated generic title: '{job_title}' - This needs technology/platform specification")
                        logger.warning(f"⚠️ Expected format: [Technology] + [Role] (e.g., 'Java Developer', 'AWS Architect')")
                    else:
                        logger.info(f"✅ Final job title: '{job_title}' ({len(words)} words)")
            
            data["jobTitle"] = job_title
            
            # 2. PROCESS DETAILED JOB DESCRIPTION
            detailed_desc = data.get("jobDescriptionDetailed", "").strip()
            
            if detailed_desc:
                # Clean up markdown artifacts
                detailed_desc = re.sub(r'\*\*', '', detailed_desc)
                detailed_desc = re.sub(r'#{1,6}\s*', '', detailed_desc)
                detailed_desc = re.sub(r'^\s*[-*•]\s*', '', detailed_desc, flags=re.MULTILINE)
                
                # Convert to HTML if needed
                if not detailed_desc.startswith("<"):
                    paragraphs = detailed_desc.split('\n\n')
                    html_paragraphs = []
                    for para in paragraphs:
                        if para.strip():
                            if '\n' in para and any(line.strip().startswith(('-', '•', '*')) for line in para.split('\n')):
                                items = [line.strip().lstrip('-•* ') for line in para.split('\n') if line.strip()]
                                html_paragraphs.append('<ul>' + ''.join(f'<li>{item}</li>' for item in items if item) + '</ul>')
                            else:
                                html_paragraphs.append(f'<p>{para.strip()}</p>')
                    detailed_desc = '\n'.join(html_paragraphs)
                
                # Clean duplicate tags
                detailed_desc = detailed_desc.replace('<p><p>', '<p>').replace('</p></p>', '</p>')
                detailed_desc = detailed_desc.replace('<ul><ul>', '<ul>').replace('</ul></ul>', '</ul>')
                
                # Validate minimum length
                if len(detailed_desc) < 300:
                    logger.warning(f"⚠️ Generated description too short ({len(detailed_desc)} chars)")
                
                data["jobDescriptionBullets"] = [detailed_desc]
                logger.info(f"✅ Generated detailed job description ({len(detailed_desc)} chars)")
            else:
                logger.warning("⚠️ No detailed job description generated")
                data["jobDescriptionBullets"] = []
            
            if "jobDescriptionDetailed" in data:
                del data["jobDescriptionDetailed"]
            
            # 3. LOCATION
            # 3. LOCATION - UNIFIED AI NORMALIZATION
            
            # 3. LOCATION - FIXED EXTRACTION
            
            combined_text = f"{subject}\n{body}"
            
            # ✅ STEP 0: Check for system code locations BUT also check if there's a more specific Location field
            system_code_match = re.search(r'(?:Tax Work )?Location\s*:\s*(US\s+Default|USA\s+Default|Default)\b', combined_text, re.IGNORECASE)
            
            # If we found a system code, check if there's ALSO a more specific "Location:" field (not "Tax Work Location:")
            if system_code_match:
                # Look for a non-system-code Location field
                specific_location_match = re.search(r'(?<!Tax Work )Location\s*:\s*(?!(?:US|USA)\s+Default\b)([^\n]+)', combined_text, re.IGNORECASE)
                
                if specific_location_match:
                    # There's a more specific location field, use that instead
                    logger.info(f"✅ Found both system code and specific location - using specific location")
                    location_field = specific_location_match.group(1).strip()
                    
                    # Clean it
                    location_field = re.sub(r'\s*\([^)]*(?:onsite|hybrid|remote|week)[^)]*\)', '', location_field, flags=re.IGNORECASE)
                    location_field = re.sub(r'^ONSITE[-\s]*', '', location_field, flags=re.IGNORECASE)
                    location_field = re.sub(r'[-\s]*100%\s*ONSITE.*$', '', location_field, flags=re.IGNORECASE)
                    location_field = re.sub(r'[-\s]*ONSITE.*$', '', location_field, flags=re.IGNORECASE)
                    # Convert tildes to pipes (they're location separators like / or |)
                    location_field = location_field.replace(' ~ ', ' | ').replace('~', ' | ').strip()
                    
                    logger.info(f"🔍 Cleaned location field: '{location_field}'")
                    
                    # ✅ FIRST: Check if multiple locations (contains // or | or multiple state codes)
                    has_multiple_separators = '//' in location_field or '|' in location_field or '/' in location_field
                    state_codes = re.findall(r'\b[A-Z]{2}\b', location_field)
                    has_multiple_states = len(set(state_codes)) > 1
                    
                    logger.info(f"🔍 Location analysis for: '{location_field}'")
                    logger.info(f"   - Contains //: {'//' in location_field}")
                    logger.info(f"   - Contains |: {'|' in location_field}")
                    logger.info(f"   - Contains /: {'/' in location_field}")
                    logger.info(f"   - State codes found: {state_codes}")
                    logger.info(f"   - Unique states: {set(state_codes)}")
                    logger.info(f"   - Multiple states: {has_multiple_states}")
                    logger.info(f"   - Multiple separators: {has_multiple_separators}")
                    logger.info(f"   - Will use simple pattern: {not has_multiple_separators and not has_multiple_states}")
                    
                    # Only try simple pattern if it's clearly a single location
                    if not has_multiple_separators and not has_multiple_states:
                        # Process it - Try multiple patterns
                        # Pattern 1: City, ST (with comma)
                        simple_pattern = re.match(r'^([A-Za-z\s]+),\s*([A-Z]{2})$', location_field.strip())
                        
                        # Pattern 2: City ST (no comma) - like "Roseville CA"
                        if not simple_pattern:
                            simple_pattern = re.match(r'^([A-Za-z\s]+)\s+([A-Z]{2})$', location_field.strip())
                        
                        if simple_pattern:
                            city_name = simple_pattern.group(1).strip()
                            state_code = simple_pattern.group(2).strip().upper()
                            
                            US_STATE_NAMES = {
                                "alabama", "alaska", "arizona", "arkansas", "california", "colorado", 
                                "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho", 
                                "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", 
                                "maine", "maryland", "massachusetts", "michigan", "minnesota", 
                                "mississippi", "missouri", "montana", "nebraska", "nevada", 
                                "new hampshire", "new jersey", "new mexico", "new york", 
                                "north carolina", "north dakota", "ohio", "oklahoma", "oregon", 
                                "pennsylvania", "rhode island", "south carolina", "south dakota", 
                                "tennessee", "texas", "utah", "vermont", "virginia", "washington", 
                                "west virginia", "wisconsin", "wyoming"
                            }
                            
                            if city_name.lower() not in US_STATE_NAMES:
                                formatted_city = ' '.join(w.capitalize() for w in city_name.split())
                                logger.info(f"✅ Simple pattern detected: {formatted_city}, {state_code}")
                                normalized = {"city": formatted_city, "state": state_code, "country": "US"}
                            else:
                                # Pattern didn't match - pass location_field to AI, not entire email
                                logger.info(f"🤖 Passing to AI: '{location_field}'")
                                normalized = normalize_locations_with_ai(text=location_field)
                        else:
                            # No simple pattern - pass location_field to AI, not entire email  
                            logger.info(f"🤖 Passing to AI: '{location_field}'")
                            normalized = normalize_locations_with_ai(text=location_field)
                    else:
                        # Multiple locations detected - pass to AI
                        logger.info(f"🤖 Multiple locations detected, passing to AI: '{location_field}'")
                        normalized = normalize_locations_with_ai(text=location_field)
                else:
                    # Only system code found, use default
                    logger.info(f"✅ System code location detected: '{system_code_match.group(1)}' -> Defaulting to Statewide/US")
                    normalized = {"city": "Statewide", "state": "", "country": "US"}
            else:
                # No system code, proceed normally
                # ✅ STEP 1: Extract ONLY the Location field
                location_field = extract_location_field(combined_text)
                
                if not location_field:
                    logger.warning("⚠️ No location field found, using Groq's extracted values")
                    # Fallback to Groq's values
                    city = data.get("city", "").strip()
                    state = data.get("state", "").strip()
                    country = data.get("country", "US").strip()
                    
                    # If Groq also didn't find anything, default to Statewide
                    if not city and not state:
                        logger.warning("⚠️ No location data from any source -> Defaulting to Statewide/US")
                        normalized = {"city": "Statewide", "state": "", "country": "US"}
                    else:
                        normalized = normalize_locations_with_ai(city=city, state=state, country=country)
                else:
                    # ✅ STEP 2: Check if multiple locations in this field
                    if detect_multiple_locations(combined_text):
                        # Multiple locations - normalize from location field (NOT entire email)
                        logger.info(f"🌍 Multiple locations detected, passing to AI: '{location_field}'")
                        normalized = normalize_locations_with_ai(text=location_field)
                    else:
                        # Single location - normalize from location field (NOT entire email)
                        logger.info(f"📍 Single location detected, passing to AI: '{location_field}'")
                        normalized = normalize_locations_with_ai(text=location_field)
            
            # Update data with normalized locations
            data["city"] = normalized["city"]
            data["state"] = normalized["state"]
            data["country"] = normalized["country"]
            
            # Log final result
            if "|" in data["city"]:
                num_locations = len(data["city"].split("|"))
                logger.info(f"✅ {num_locations} locations normalized:")
            else:
                logger.info(f"✅ Location normalized:")
            
            logger.info(f"   City: {data['city']}")
            logger.info(f"   State: {data['state']}")
            logger.info(f"   Country: {data['country']}")       
            # 4. WORK MODE
            work_mode = data.get("workMode", "Onsite").strip()
            if work_mode.lower() not in ['remote', 'hybrid', 'onsite']:
                work_mode = "Onsite"
            data["workMode"] = work_mode.capitalize()
            
            # 5. RATES
            try:
                bill_rate_min = float(data.get("billRateMin", 0))
                bill_rate_max = float(data.get("billRateMax", 0))
                
                if bill_rate_min == bill_rate_max and bill_rate_max > 0:
                    data["billRateMin"] = 0
                
                data["billRateMin"] = bill_rate_min
                data["billRateMax"] = bill_rate_max
            except (ValueError, TypeError):
                data["billRateMin"] = 0
                data["billRateMax"] = 0
            
            # 6. EXPERIENCE
            experience = data.get("experience", "").strip()
            if experience:
                experience = re.sub(r'\s+(?:years?|yrs?|months?)', '', experience, flags=re.IGNORECASE)
                experience = re.sub(r'\s+', '', experience)
                data["experience"] = experience
            else:
                data["experience"] = ""
            
            # 7. DATES
            for date_field in ['startDate', 'endDate']:
                date_str = data.get(date_field, "").strip()
                if date_str and date_str not in ['', 'None', 'N/A']:
                    try:
                        if not re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                            parsed_date = None
                            for fmt in ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d']:
                                try:
                                    parsed_date = datetime.strptime(date_str, fmt)
                                    date_str = parsed_date.strftime('%Y-%m-%d')
                                    break
                                except:
                                    continue
                            
                            if parsed_date is None:
                                data[date_field] = ""
                            else:
                                data[date_field] = date_str
                        else:
                            data[date_field] = date_str
                    except:
                        data[date_field] = ""
                else:
                    data[date_field] = ""
            
            # 8. CLIENT POC
            client_poc = data.get("clientPOC", "").strip()
            if client_poc:
                client_poc = re.sub(r'\S+@\S+', '', client_poc)
                client_poc = re.sub(r'[\d\-\+\(\)]{7,}', '', client_poc)
                client_poc = client_poc.strip()
                data["clientPOC"] = client_poc
            
            # 9. GBaMS ID - Enhanced extraction
            if not data.get("gbamsId") or data["gbamsId"] in ['', 'N/A', 'Unknown']:
                gbams_match = re.search(r'GBaMS\s*(?:ReqID|ID)[:\-\s]*(\d+)', body, re.IGNORECASE)
                if gbams_match:
                    data["gbamsId"] = gbams_match.group(1).strip()
                else:
                    requirement_match = re.search(r'REQUIREMENT_ID[:\-\s]*(\d+)', body, re.IGNORECASE)
                    if requirement_match:
                        data["gbamsId"] = requirement_match.group(1).strip()
            
            logger.info(f"✅ Groq parsed successfully: {data.get('jobTitle','Unknown')}")
            return data  # Success! Return the data

        except Exception as e:
            logger.error(f"❌ Groq parsing attempt {attempt + 1}/{max_retries} failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                logger.error("❌ All retry attempts exhausted")
                return {}
    
    # If we get here, all retries failed
    logger.error("❌ All Groq parsing attempts failed")
    return {}
# ===================== API TOKEN =====================
def get_auth_token():
    global AUTH_TOKEN, TOKEN_EXPIRY
    if AUTH_TOKEN and time.time() < TOKEN_EXPIRY:
        return AUTH_TOKEN

    cfg = load_config()
    url = f"{cfg['api']['base_url']}/api/auth/login"
    try:
        r = requests.post(url, json={
            "username": cfg['api']['service_username'],
            "password": cfg['api']['service_password']
        }, timeout=10)
        if r.status_code == 200:
            AUTH_TOKEN = r.json().get('token')
            TOKEN_EXPIRY = time.time() + 3500
            logger.info("New API token obtained")
            return AUTH_TOKEN
        else:
            logger.error(f"Login failed: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"Token error: {e}")
    return None

# ===================== EMAIL LISTENER =====================
# ===================== EMAIL LISTENER =====================
class EmailListener:
    def __init__(self, server, port, username, password=None,
                 ssl_required=True, mailbox_name=None, mailbox_id=None,
                 use_oauth=False):
        self.server = server
        self.port = port
        self.username = username
        self.password = password          # only used when use_oauth=False
        self.ssl_required = ssl_required
        self.use_oauth = use_oauth        # True → XOAUTH2 via Azure AD
        self.mail = None
        self.last_connect_time = 0
        self.connection_timeout = 1800    # Reconnect every 30 minutes
        self.mailbox_name = mailbox_name or username
        self.mailbox_id = mailbox_id or "email"
        self._running = False
        self.processed_uids = set()

    def connect(self):
        try:
            if self.mail:
                try:
                    self.mail.logout()
                except:
                    pass
                self.mail = None

            context = ssl.create_default_context()
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

            if self.ssl_required or self.use_oauth:
                self.mail = imaplib.IMAP4_SSL(self.server, self.port, ssl_context=context)
            else:
                self.mail = imaplib.IMAP4(self.server, self.port)

            if self.use_oauth:
                token = _get_oauth_access_token(self.username)
                # _build_xoauth2_string returns RAW bytes (not base64).
                # imaplib._Authenticator.encode() will base64-encode them.
                # The lambda must accept exactly 1 arg (server challenge).
                xoauth2_raw = _build_xoauth2_string(self.username, token)
                self.mail.authenticate("XOAUTH2", lambda challenge: xoauth2_raw)
                logger.info(f"✅ OAuth2 IMAP connected: {self.username}")
            else:
                self.mail.login(self.username, self.password)
                logger.info(f"✅ Basic auth IMAP connected: {self.username}")

            self.last_connect_time = time.time()
            return True

        except Exception as e:
            logger.error(f"❌ IMAP connect failed for {self.username}: {e}")
            logger.error(f"   Server: {self.server}, Port: {self.port}, OAuth: {self.use_oauth}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            self.mail = None
            return False
    def is_connection_alive(self):
        """Check if the IMAP connection is still alive"""
        try:
            if not self.mail:
                return False
            
            # Try a NOOP command to check connection
            status = self.mail.noop()
            return status[0] == 'OK'
        except:
            return False

    def ensure_connection(self):
        """Ensure connection is alive; proactively refresh OAuth token before expiry."""
        if self.use_oauth:
            cached = _oauth_token_cache.get(self.username)
            if cached and time.time() >= cached["expires_at"] - 300:
                logger.info(f"🔑 OAuth token expiring soon for {self.username} — reconnecting")
                return self.connect()

        if time.time() - self.last_connect_time > self.connection_timeout:
            logger.info("⚠️ Connection timeout reached, reconnecting...")
            return self.connect()

        if not self.is_connection_alive():
            logger.warning("⚠️ Connection lost, reconnecting...")
            return self.connect()

        return True

    def get_all_email_uids(self):
        """Get all email UIDs from inbox with reconnection handling"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Ensure connection is alive before trying to fetch
                if not self.ensure_connection():
                    logger.error("❌ Cannot establish connection")
                    retry_count += 1
                    time.sleep(2)
                    continue
                
                self.mail.select('INBOX')
                status, data = self.mail.uid('search', None, 'ALL')
                
                if status == 'OK' and data[0]:
                    uids = data[0].split()
                    return [uid.decode() for uid in uids]
                else:
                    logger.warning(f"⚠️ No UIDs returned, status: {status}")
                    return []
                    
            except imaplib.IMAP4.abort as e:
                logger.error(f"❌ IMAP connection aborted: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"🔄 Retry {retry_count}/{max_retries} - Reconnecting...")
                    time.sleep(2)
                    self.connect()
                else:
                    logger.error("❌ Max retries reached for get_all_email_uids")
                    
            except (socket.error, ssl.SSLError, EOFError) as e:
                logger.error(f"❌ Socket/SSL error getting email UIDs: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"🔄 Retry {retry_count}/{max_retries} - Reconnecting...")
                    time.sleep(2)
                    self.connect()
                else:
                    logger.error("❌ Max retries reached for get_all_email_uids")
                    
            except Exception as e:
                logger.error(f"❌ Error getting email UIDs: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"🔄 Retry {retry_count}/{max_retries}...")
                    time.sleep(2)
                else:
                    logger.error("❌ Max retries reached")
        
        return []

    def get_email_content(self, uid):
        """Fetch email content with reconnection handling"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Ensure connection is alive
                if not self.ensure_connection():
                    logger.error("❌ Cannot establish connection")
                    retry_count += 1
                    time.sleep(2)
                    continue
                
                status, msg_data = self.mail.uid('fetch', uid, '(RFC822)')
                if status != 'OK':
                    logger.warning(f"⚠️ Failed to fetch UID {uid}, status: {status}")
                    return None
                    
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                subject = self._decode(msg.get('Subject', ''))
                from_ = self._decode(msg.get('From', ''))
                date = msg.get('Date', '')

                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(errors='ignore')
                            break
                        if part.get_content_type() == "text/html" and not body:
                            body = part.get_payload(decode=True).decode(errors='ignore')
                else:
                    body = msg.get_payload(decode=True).decode(errors='ignore')

                return {'subject': subject, 'from': from_, 'date': date, 'body': body, 'uid': uid, 'vendor': self.mailbox_name}
                
            except imaplib.IMAP4.abort as e:
                logger.error(f"❌ IMAP abort while fetching UID {uid}: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"🔄 Retry {retry_count}/{max_retries} - Reconnecting...")
                    time.sleep(2)
                    self.connect()
                    
            except (socket.error, ssl.SSLError, EOFError) as e:
                logger.error(f"❌ Socket/SSL error fetching UID {uid}: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"🔄 Retry {retry_count}/{max_retries} - Reconnecting...")
                    time.sleep(2)
                    self.connect()
                    
            except Exception as e:
                logger.error(f"❌ Fetch error for UID {uid}: {e}")
                return None
        
        logger.error(f"❌ Failed to fetch UID {uid} after {max_retries} retries")
        return None

    def _decode(self, header):
        if not header: return ""
        decoded = decode_header(header)
        return ''.join(
            t.decode(e or 'utf-8', errors='ignore') if isinstance(t, bytes) else t
            for t, e in decoded
        )

    def stop(self):
        """Signal the listener to stop and close the IMAP connection."""
        self._running = False
        if self.mail:
            try:
                self.mail.logout()
            except Exception:
                pass
            self.mail = None
        logger.info(f"⏹ EmailListener stopped: {self.username}")

    def start_listening(self, callback, check_interval=30):
        """Start listening with improved error handling and auto-reconnect"""
        self._running = True
        if not self.connect():
            logger.error("Failed to connect to email server")
            return
        
        # Get initial state — mark ALL existing emails as already processed
        # so we only act on emails that arrive AFTER this script starts
        initial_uids = self.get_all_email_uids()
        if initial_uids:
            for uid in initial_uids:
                self.processed_uids.add(uid)
            logger.info(f"[{self.mailbox_name}] Initialized with {len(initial_uids)} existing emails (will not reprocess)")
        
        logger.info("Starting Groq-powered email listener...")
        logger.info("Monitoring for: New Request, Cancelled, On Hold, Closed, Modified, Updated, Re-opened emails")
        logger.info("Press Ctrl+C to stop")

        consecutive_errors = 0
        max_consecutive_errors = 5

        try:
            while self._running:
                try:
                    # Ensure connection before checking emails
                    if not self.ensure_connection():
                        logger.error("❌ Connection check failed, waiting before retry...")
                        time.sleep(10)
                        continue
                    
                    # Get ALL current email UIDs
                    current_uids = self.get_all_email_uids()
                    
                    if current_uids is None or len(current_uids) == 0:
                        # If we got no UIDs, connection might be bad
                        logger.warning("⚠️ No UIDs received, checking connection...")
                        consecutive_errors += 1
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error("❌ Too many consecutive errors, forcing reconnection...")
                            self.connect()
                            consecutive_errors = 0
                        
                        time.sleep(check_interval)
                        continue
                    
                    # Reset error counter on success
                    consecutive_errors = 0
                    
                    if current_uids:
                        # Process emails from oldest to newest
                        current_uids.reverse()
                        
                        new_emails_processed = 0
                        cancelled_emails = 0
                        on_hold_emails = 0
                        closed_emails = 0
                        modified_emails = 0
                        reopened_emails = 0
                        skipped_emails = 0
                        
                        for uid in current_uids:
                            if uid not in self.processed_uids:
                                email_data = self.get_email_content(uid)
                                if email_data:
                                    # ── Sender filter: check against allowed_senders in config ──
                                    from_address = email_data.get('from', '')
                                    cfg = load_config()
                                    mb_cfg = cfg.get(self.mailbox_id) or cfg.get('email') or {}
                                    allowed_raw = mb_cfg.get('allowed_senders', '')
                                    if not is_sender_allowed(from_address, allowed_raw):
                                        logger.info(f"⏭ [{self.mailbox_id}] Skipping email from '{from_address}' — not in allowed list.")
                                        self.processed_uids.add(uid)
                                        continue
                                    # ── Allowed — proceed ──
                                    subject_lower = email_data['subject'].lower()
                                    
                                    # Classify email type
                                    is_cancelled = 'cancelled' in subject_lower or 'canceled' in subject_lower
                                    is_on_hold = 'put on hold' in subject_lower or 'on hold' in subject_lower
                                    is_closed = 'closed' in subject_lower
                                    is_reopened = 're-opened' in subject_lower or 'reopened' in subject_lower or 're opened' in subject_lower
                                    is_modified = any(kw in subject_lower for kw in ['updated', 'modified', 'amendment', 'revision', 'change'])
                                    is_new_request = 'new request' in subject_lower
                                    
                                    # Process based on type
                                    if is_cancelled:
                                        logger.info(f"🚫 Cancelled Email (UID: {uid})")
                                        callback(email_data)
                                        cancelled_emails += 1
                                    elif is_on_hold:
                                        logger.info(f"⏸️ On Hold Email (UID: {uid})")
                                        callback(email_data)
                                        on_hold_emails += 1
                                    elif is_closed:
                                        logger.info(f"🔒 Closed Email (UID: {uid})")
                                        callback(email_data)
                                        closed_emails += 1
                                    elif is_reopened:
                                        logger.info(f"🔓 Re-opened Email (UID: {uid})")
                                        callback(email_data)
                                        reopened_emails += 1
                                    elif is_modified:
                                        logger.info(f"🔄 Modified/Updated Email (UID: {uid})")
                                        callback(email_data)
                                        modified_emails += 1
                                    elif is_new_request:
                                        logger.info(f"🎯 New Request Email (UID: {uid})")
                                        callback(email_data)
                                        new_emails_processed += 1
                                    else:
                                        logger.info(f"⭐ Skipped Email (UID: {uid}): {email_data['subject']}")
                                        skipped_emails += 1
                                    
                                    self.processed_uids.add(uid)
                        
                        # Summary log
                        total_processed = new_emails_processed + cancelled_emails + on_hold_emails + closed_emails + modified_emails + reopened_emails
                        if total_processed > 0 or skipped_emails > 0:
                            logger.info("=" * 70)
                            logger.info("📊 CYCLE SUMMARY:")
                            if new_emails_processed > 0:
                                logger.info(f"   🎯 New Roles Created: {new_emails_processed}")
                            if cancelled_emails > 0:
                                logger.info(f"   🚫 Cancelled: {cancelled_emails}")
                            if on_hold_emails > 0:
                                logger.info(f"   ⏸️ On Hold: {on_hold_emails}")
                            if closed_emails > 0:
                                logger.info(f"   🔒 Closed: {closed_emails}")
                            if modified_emails > 0:
                                logger.info(f"   🔄 Modified/Updated: {modified_emails}")
                            if reopened_emails > 0:
                                logger.info(f"   🔓 Re-opened: {reopened_emails}")
                            if skipped_emails > 0:
                                logger.info(f"   ⭐ Skipped: {skipped_emails}")
                            logger.info("=" * 70 + "\n")
                    
                    # Wait before checking again
                    time.sleep(check_interval)
                    
                except imaplib.IMAP4.abort as e:
                    logger.error(f"❌ IMAP connection aborted in main loop: {e}")
                    consecutive_errors += 1
                    logger.info("🔄 Attempting to reconnect...")
                    time.sleep(5)
                    self.connect()
                    
                except (socket.error, ssl.SSLError, EOFError) as e:
                    logger.error(f"❌ Socket/SSL error in main loop: {e}")
                    consecutive_errors += 1
                    logger.info("🔄 Attempting to reconnect...")
                    time.sleep(5)
                    self.connect()
                    
                except Exception as e:
                    logger.error(f"❌ Error in email listener: {e}")
                    consecutive_errors += 1
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    time.sleep(5)
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("❌ Too many consecutive errors, forcing reconnection...")
                        self.connect()
                        consecutive_errors = 0
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping listener...")
        finally:
            if self.mail:
                try:
                    self.mail.logout()
                    logger.info("✅ Logged out from email server")
                except:
                    pass
# ===================== MAIN HANDLER WITH AUTO-ASSIGNMENT =====================

def handle_new_email(email_data):
    """
    Main email handler that routes emails based on their type:
    - New Request → Create new role
    - Cancelled → Update status to Cancelled
    - On Hold → Update status to On Hold
    - Closed → Update status to Closed
    - Modified/Updated → Update status to Modified/Updated
    - Re-opened → Update status to Active
    """
    subject = email_data['subject']
    subject_lower = subject.lower()
    
    logger.info("\n" + "╔" + "═" * 78 + "╗")
    logger.info(f"  📧 NEW EMAIL RECEIVED")
    logger.info(f"  Subject: {subject}")
    logger.info("╚" + "═" * 78 + "╝")
    
    # ========== PRIORITY 1: CANCELLED EMAILS ==========
    if 'cancelled' in subject_lower or 'canceled' in subject_lower:
        logger.info("🚫 ROUTING: Cancelled notification email")
        handle_cancelled_email(email_data)
        return
    
    # ========== PRIORITY 2: ON HOLD EMAILS ==========
    if 'put on hold' in subject_lower or 'on hold' in subject_lower:
        logger.info("⏸️ ROUTING: On Hold notification email")
        handle_on_hold_email(email_data)
        return
    
    # ========== PRIORITY 3: CLOSED EMAILS ==========
    if 'closed' in subject_lower:
        logger.info("🔒 ROUTING: Closed notification email")
        handle_closed_email(email_data)
        return
    
    # ========== PRIORITY 4: RE-OPENED EMAILS ==========
    if 're-opened' in subject_lower or 'reopened' in subject_lower or 're opened' in subject_lower:
        logger.info("🔓 ROUTING: Re-opened notification email")
        handle_reopened_email(email_data)
        return
    
    # ========== PRIORITY 5: MODIFIED/UPDATED EMAILS ==========
    # Check for update keywords
    update_keywords = ['updated', 'modified', 'amendment', 'revision', 'change']
    is_update = any(keyword in subject_lower for keyword in update_keywords)
    
    if is_update:
        logger.info("🔄 ROUTING: Modified/Updated notification email")
        handle_modified_email(email_data)
        return
    
    # ========== PRIORITY 6: NEW REQUEST EMAILS ==========
    is_new_request = 'new request' in subject_lower
    
    if is_new_request:
        logger.info("🎯 ROUTING: New Request email → Creating new role")
        process_job_email(email_data)
        return
    
    # ========== SKIP: OTHER EMAILS ==========
    logger.info(f"⭐ SKIPPED: Not a recognized job-related email")
    logger.info(f"   Subject doesn't match: New Request, Cancelled, On Hold, Closed, Modified, or Re-opened")
    return

def format_location_display(city, state, country):
    """
    Formats location to: City1, City2 | State1, State2 | Country
    Example: Richardson, Woonsocket | TX, RI | US
    """
    if not city:
        return ""
    
    # Internal normalization often uses pipes for processing
    # We clean these to use commas within categories for the final display
    cities_formatted = city.replace(" | ", ", ")
    states_formatted = str(state).replace(" | ", ", ")
    
    parts = []
    
    # Add the comma-separated cities
    if cities_formatted:
        parts.append(cities_formatted)
    
    # Add the comma-separated states
    if states_formatted:
        parts.append(states_formatted)
    
    # Add the standardized country code
    if country:
        # If multiple country codes exist, take the first one
        country_code = country.split("|")[0].strip().upper() if "|" in country else country.strip().upper()
        parts.append(country_code)
    
    # Join the three major blocks with pipes as per your requirement
    return " | ".join(parts)

def append_vendor_to_role(role_id, new_vendor):
    """
    Appends new_vendor to the existing vendor field of a role (comma-separated),
    only if it is not already present. Used when a duplicate job arrives from a
    second mailbox — we skip creating the role but still record that this mailbox
    also received it.

    Example: existing vendor = "prophechy"  +  new_vendor = "beeline"
             result vendor   = "prophechy, beeline"
    """
    try:
        token = get_auth_token()
        if not token:
            logger.error("❌ append_vendor_to_role: No auth token")
            return False

        cfg = load_config()
        url = f"{cfg['api']['base_url']}/api/recruitment/roles/{role_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Step 1: Fetch current role
        get_resp = requests.get(url, headers=headers, timeout=10)
        if get_resp.status_code != 200:
            logger.error(f"❌ append_vendor_to_role: Cannot fetch role {role_id} — {get_resp.status_code}")
            return False

        role_data = get_resp.json()
        current_vendor = role_data.get('vendor') or ''
        current_vendor = current_vendor.strip()

        # Step 2: Build the updated vendor string (no duplicates)
        existing_vendors = [v.strip() for v in current_vendor.split(',') if v.strip()]
        if new_vendor.strip() in existing_vendors:
            logger.info(f"ℹ️ Vendor '{new_vendor}' already listed for role {role_id} — no update needed")
            return True

        existing_vendors.append(new_vendor.strip())
        updated_vendor = ', '.join(existing_vendors)
        logger.info(f"📦 Updating vendor for role {role_id}: '{current_vendor}' → '{updated_vendor}'")

        # Step 3: Preserve vendor explicitly and PUT back
        role_data['vendor'] = updated_vendor

        put_resp = requests.put(url, json=role_data, headers=headers, timeout=10)
        if put_resp.status_code in [200, 204]:
            logger.info(f"✅ Vendor updated successfully for role {role_id}: '{updated_vendor}'")
            # ✅ Update watchdog registry so it monitors the new combined vendor string
            register_role_vendor(role_id, updated_vendor)
            return True
        else:
            logger.error(f"❌ append_vendor_to_role: PUT failed — {put_resp.status_code} {put_resp.text}")
            return False

    except Exception as e:
        logger.error(f"❌ append_vendor_to_role error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def process_job_email(email_data):
    """
    Process job email with FIXED assignment logic
    """
    # vendor = which mailbox received this email (used as DB column)
    # Sanitize: strip whitespace, fall back to sender address, never allow blank/null
    _raw_vendor = email_data.get('vendor') or email_data.get('from') or ''
    vendor = _raw_vendor.strip() if _raw_vendor.strip() else 'Unknown'
    logger.info(f"📦 Vendor (source mailbox) resolved as: '{vendor}'")

    parsed = parse_with_groq(email_data['subject'], email_data['body'])
    
    # Use fallback parser if Groq fails
    if not parsed or not parsed.get('jobTitle'):
        logger.warning("⚠ Groq parsing failed, using fallback parser...")
        parsed = parse_email_fallback(email_data['subject'], email_data['body'])
    else:
        # Double-check GBaMS ID with regex
        body = email_data['body']
        if not parsed.get("gbamsId") or parsed["gbamsId"] in ['', 'N/A', 'Unknown']:
            requirement_match = re.search(r'REQUIREMENT_ID[:\-\s]*(\d+)', body, re.IGNORECASE)
            if requirement_match:
                parsed["gbamsId"] = requirement_match.group(1).strip()
                logger.info(f"✓ Secondary check found REQUIREMENT_ID: {parsed['gbamsId']}")
            else:
                gbams_match = re.search(r'GBaMS\s*(?:ReqID|ID)[:\-\s]*(\d+)', body, re.IGNORECASE)
                if gbams_match:
                    parsed["gbamsId"] = gbams_match.group(1).strip()
                    logger.info(f"✓ Secondary check found GBaMS ID: {parsed['gbamsId']}")
    
    # Generic title detection
    raw_job_title = parsed.get('jobTitle', '').strip() if parsed else ''
    generic_patterns = [
        r'Information Technology.*_USA.*_',
        r'USA_\w+',
        r'\w+_USA_',
        r'^(Developer|Engineer|Lead|Analyst|Manager|Architect|Consultant)$',  # Added $ to match ONLY these words alone
        r'^(Senior|Junior|Lead|Principal|Staff)\s+(Developer|Engineer|Analyst|Manager|Architect|Consultant)$',  # Seniority + generic word only
        r'Default',
        r'Refer\s*JD',
    ]
    
    has_generic_title = any(re.search(pattern, raw_job_title, re.IGNORECASE) for pattern in generic_patterns)
    job_title_found = raw_job_title and raw_job_title not in ['', 'N/A', 'Unknown'] and not has_generic_title
    
    if not job_title_found or has_generic_title:
        if has_generic_title:
            logger.warning(f"⚠️ Generic/coded job title detected: '{raw_job_title}'")
        else:
            logger.error("❌ Job title extraction failed")

    # Extract client from subject line
    subject_client_match = re.match(r'(?:Fwd: )?([A-Z0-9&]+) - ', email_data['subject'])
    client_name = subject_client_match.group(1) if subject_client_match else parsed.get("client", "Beeline")

    # Fallback dates
    start = parsed.get('startDate') or (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    end = parsed.get('endDate') or (datetime.now() + timedelta(days=210)).strftime('%Y-%m-%d')

    # Extract Job ID for duplicate check
    job_id = parsed.get("requestId", "")

    # ✅ THREAD LOCK: Both mailbox threads share this lock.
    # Only ONE thread at a time can run the duplicate-check + role-creation block.
    # This prevents a race condition where both threads pass the DB check
    # simultaneously (before either has written the role) and both try to create it.
    # Scenario coverage:
    #   Same day / script running  → in-memory set catches the second thread
    #   Different day / restarted  → DB check catches it (PROCESSED_REQUEST_IDS empty)
    #   Simultaneous arrival       → lock ensures only one thread enters at a time
    with _ROLE_CREATION_LOCK:

        # ✅ CHECK 1: FAST IN-MEMORY CROSS-MAILBOX DUPLICATE CHECK
        # If the same requestId was already processed this session by another mailbox,
        # skip role creation but append this mailbox's vendor name to the existing role.
        if job_id and job_id.strip() and job_id in PROCESSED_REQUEST_IDS:
            logger.warning("=" * 70)
            logger.warning(f"⚠️ CROSS-MAILBOX DUPLICATE: Request ID '{job_id}' already processed this session")
            logger.warning(f"⚠️ Source mailbox (current): {vendor}")
            logger.warning("⚠️ SKIPPING role creation — counters NOT advanced")
            logger.warning("📦 Appending vendor name to existing role...")
            logger.warning("=" * 70 + "\n")
            try:
                _token = get_auth_token()
                if _token:
                    _cfg = load_config()
                    _roles_resp = requests.get(
                        f"{_cfg['api']['base_url']}/api/recruitment/roles",
                        headers={"Authorization": f"Bearer {_token}"},
                        timeout=10
                    )
                    if _roles_resp.status_code == 200:
                        for _r in _roles_resp.json():
                            if _r.get('jobId') == job_id:
                                append_vendor_to_role(_r.get('id'), vendor)
                                break
            except Exception as _e:
                logger.error(f"❌ Could not append vendor on in-memory duplicate: {_e}")
            return

        # ✅ CHECK 2: DATABASE DUPLICATE CHECK (catches restarts / different-day duplicates)
        # Also catches the second thread in a simultaneous-arrival race, because by the
        # time the lock releases to the second thread, the first has already written to DB.
        if job_id and job_id.strip():
            token = get_auth_token()
            if token:
                cfg = load_config()
                check_url = f"{cfg['api']['base_url']}/api/recruitment/roles"
                _headers = {"Authorization": f"Bearer {token}"}

                try:
                    response = requests.get(check_url, headers=_headers, timeout=10)
                    if response.status_code == 200:
                        existing_roles = response.json()

                        duplicate_role = None
                        for role in existing_roles:
                            if role.get('jobId') == job_id:
                                duplicate_role = role
                                break

                        if duplicate_role:
                            assigned_recruiters = duplicate_role.get('assignedRecruiters', [])
                            recruiter_names = ', '.join([r.get('name', 'Unknown') for r in assigned_recruiters]) if assigned_recruiters else 'None'

                            logger.warning("=" * 70)
                            logger.warning(f"⚠️ DUPLICATE JOB ID DETECTED: {job_id}")
                            logger.warning(f"⚠️ Role already exists with ID: {duplicate_role.get('id')}")
                            logger.warning(f"⚠️ Role Name: {duplicate_role.get('role', 'Unknown')}")
                            logger.warning(f"⚠️ Role Owner: {duplicate_role.get('assignTo', 'Unknown')}")
                            logger.warning(f"⚠️ Assigned Recruiters: {recruiter_names}")
                            logger.warning(f"⚠️ SKIPPING ROLE CREATION (Round-robin counters NOT advanced)")
                            logger.warning(f"📦 Appending vendor '{vendor}' to existing role {duplicate_role.get('id')}...")
                            logger.warning("=" * 70 + "\n")
                            # ✅ Append this mailbox's vendor (comma-separated, no duplicates)
                            append_vendor_to_role(duplicate_role.get('id'), vendor)
                            PROCESSED_REQUEST_IDS.add(job_id)
                            return  # Exit without advancing counters

                except Exception as e:
                    logger.error(f"Error checking for duplicate Job ID: {e}")

        # ✅ NO DUPLICATE - NOW ADVANCE COUNTERS AND CREATE ROLE
        logger.info("=" * 70)
        logger.info("🎯 PROCEEDING WITH ROLE CREATION")
        logger.info("=" * 70)
    
        # 🔧 FIX 1: Get role owner ID and fetch name properly
        role_owner_id = get_next_role_owner_id()
        logger.info(f"📋 Selected Role Owner ID: {role_owner_id}")
    
        role_owner_name = get_role_owner_name(role_owner_id)
    
        if not role_owner_name:
            logger.warning(f"⚠️ Could not fetch name for role owner ID {role_owner_id}")
            # Try to fetch from API one more time
            token = get_auth_token()
            if token:
                cfg = load_config()
                url = f"{cfg['api']['base_url']}/api/recruitment/recruiters/{role_owner_id}"
                headers = {"Authorization": f"Bearer {token}"}
                try:
                    r = requests.get(url, headers=headers, timeout=10)
                    if r.status_code == 200:
                        recruiter_data = r.json()
                        role_owner_name = recruiter_data.get('name')
                        logger.info(f"✓ Fetched role owner name directly: {role_owner_name}")
                except Exception as e:
                    logger.error(f"Error fetching role owner directly: {e}")
        
            if not role_owner_name:
                logger.error(f"❌ CRITICAL: Cannot fetch role owner name for ID {role_owner_id}")
                logger.error("❌ This role will be created WITHOUT a role owner!")
                role_owner_name = None  # Will cause issues - should be fixed
        else:
            logger.info(f"✅ Role Owner: {role_owner_name} (ID: {role_owner_id})")

        # 🔧 FIX 2: Get recruiters and validate
        assigned_recruiter_ids = get_next_recruiter_ids(RECRUITERS_PER_ROLE)
        logger.info(f"📋 Selected Recruiter IDs: {assigned_recruiter_ids}")
    
        recruiter_names = get_recruiter_names(assigned_recruiter_ids)
    
        logger.info(f"🔄 Auto-assigning {RECRUITERS_PER_ROLE} Recruiter(s):")
        for rec_id in assigned_recruiter_ids:
            rec_name = recruiter_names.get(rec_id, f"Unknown (ID: {rec_id})")
            logger.info(f"   - {rec_name} (ID: {rec_id})")
            if rec_id not in recruiter_names:
                logger.warning(f"⚠️ Could not fetch name for recruiter ID {rec_id}")

        # Rate handling
        bill_rate_min = float(parsed.get("billRateMin", 0)) if parsed else 0
        bill_rate_max = float(parsed.get("billRateMax", 0)) if parsed else 0
    
        logger.info(f"💰 Rate Information:")
        logger.info(f"   - Min Rate: {format_rate_display(bill_rate_min)}")
        logger.info(f"   - Max Rate: {format_rate_display(bill_rate_max)}")

        # Convert single-rate values to 0-max
        if bill_rate_min == bill_rate_max and bill_rate_max > 0:
            bill_rate_min = 0
            logger.info(f"   ⚡ Single-rate detected, setting Min Rate to 0")

        logger.info(f"💰 Corrected Rate Information:")
        logger.info(f"   - Min Rate: {format_rate_display(bill_rate_min)}")
        logger.info(f"   - Max Rate: {format_rate_display(bill_rate_max)}")

        # Job title formatting
        if not job_title_found or has_generic_title:
            # Try to generate title from description before falling back to (Refer JD)
            logger.warning(f"⚠️ Generic/missing job title: '{raw_job_title}' - attempting to generate from description")
            generated_title = generate_job_title_from_description(body)
        
            # Check if generated title is better than generic
            if generated_title and generated_title != "Technical Professional" and not is_generic_title(generated_title):
                final_job_title = generated_title
                logger.info(f"✅ Generated specific title from description: '{generated_title}'")
            else:
                # Fallback to (Refer JD) only if generation failed
                if has_generic_title:
                    role_keyword_match = re.search(r'(Developer|Engineer|Lead|Analyst|Manager|Architect|Consultant)', raw_job_title, re.IGNORECASE)
                    if role_keyword_match:
                        role_keyword = role_keyword_match.group(1).title()
                        final_job_title = f"{role_keyword} (Refer JD)"
                        logger.info(f"🔄 Extracted role keyword: '{role_keyword}' from generic title")
                    else:
                        final_job_title = "ROLE TITLE - REFER JD"
                else:
                    final_job_title = "ROLE TITLE EXTRACTION FAILED - REFER JD"
        
            warning_note = "⚠️ AUTOMATED TITLE EXTRACTION INCOMPLETE - PLEASE REVIEW JOB DESCRIPTION FOR EXACT ROLE TITLE"
            logger.warning("🔄 Job title needs manual review")
        else:
            final_job_title = raw_job_title
            warning_note = ""

        job_description_html = ""
    
        if parsed and parsed.get("jobDescriptionBullets"):
            bullets = parsed.get("jobDescriptionBullets", [])
        
            if bullets and len(bullets) == 1 and ('<p>' in bullets[0] or '<ul>' in bullets[0]):
                job_description_html = bullets[0]
            
                if not job_title_found or has_generic_title:
                    warning_html = "<div style='background-color: #fff3cd; border: 2px solid #ffc107; padding: 10px; margin-bottom: 15px;'>"
                    warning_html += "<p><strong>🚨 WARNING: JOB TITLE NEEDS REVIEW</strong></p>"
                    if has_generic_title:
                        warning_html += f"<p>Generic/coded title detected: <em>{raw_job_title}</em></p>"
                    else:
                        warning_html += "<p>Automated job title extraction failed</p>"
                    warning_html += "<p><strong>Action Required:</strong> Please review the job description below and update the role title accordingly.</p>"
                    warning_html += "</div>"
                
                    job_description_html = warning_html + job_description_html
            
                logger.info(f"✅ Using detailed HTML job description ({len(job_description_html)} chars)")
            else:
                warning_bullets = []
                if not job_title_found or has_generic_title:
                    if has_generic_title:
                        warning_bullets.append("🚨 GENERIC/CODED JOB TITLE DETECTED - REVIEW REQUIRED")
                        warning_bullets.append(f"Original title from system: {raw_job_title}")
                    else:
                        warning_bullets.append("🚨 AUTOMATED JOB TITLE EXTRACTION FAILED")
                    warning_bullets.append("PLEASE REVIEW THE ORIGINAL JOB DESCRIPTION TO IDENTIFY THE CORRECT ROLE TITLE")
                    warning_bullets.append("---")
            
                all_bullets = warning_bullets + bullets[:20]
                job_description_html = "<ul>" + "".join(
                    f"<li>{truncate_field(b, 300)}</li>" for b in all_bullets
                ) + "</ul>"
                logger.info(f"✅ Using legacy bullet format ({len(all_bullets)} bullets)")
        else:
            job_description_html = "<p><strong>⚠️ No job description available</strong></p>"
            job_description_html += "<p>Please update this role with the complete job description.</p>"
            logger.warning("⚠️ No job description bullets available")
    
        logger.info(f"📄 Final job description length: {len(job_description_html)} characters")

        # 🔧 FIX 3: Build role payload with PROPER assignTo field
        # The city field already contains the full formatted location: "City1, City2 | State1, State2 | Country"
        # Extract components for API - Option 2: Split city and state properly
        city_field = parsed.get("city", "") if parsed else ""
        state_field = parsed.get("state", "") if parsed else ""
        country_field = parsed.get("country", "United States") if parsed else "United States"
    
        # If city field contains the full pipe-separated format, extract cities and states separately
        if city_field and "|" in city_field:
            # Format is: "Cities | States | Country"
            parts = [p.strip() for p in city_field.split("|")]
            if len(parts) >= 2:
                actual_cities = parts[0]  # "Piscataway, Buffalo, Albany"
                actual_states = parts[1]  # "NJ, NY, NJ"
                # For API: Send cities and states separately
                city_for_api = actual_cities  # Just the cities
                state_for_api = actual_states  # Just the states
            else:
                state_for_api = state_field
                city_for_api = city_field
        else:
            # Single location format
            state_for_api = state_field
            city_for_api = city_field
    
        role_data = {
            "jobId": parsed.get("requestId", "") if parsed else "",
            "gbamsId": parsed.get("gbamsId", "") if parsed else "",
            "role": truncate_field(final_job_title, 100),
            "roleType": "Contract",
            "country": country_field,
            "state": truncate_field(state_for_api, 50),
            "city": truncate_field(city_for_api, 200),
            "currency": "USD",
            "minRate": bill_rate_min,
            "maxRate": bill_rate_max,
            "client": truncate_field(client_name, 50),
            "clientPOC": truncate_field(parsed.get("clientPOC", ""), 50) if parsed else "",
            "roleLocation": truncate_field(parsed.get("workMode", "Onsite"), 50) if parsed else "Onsite",
            "experience": truncate_field(extract_experience(parsed.get("experience", "") if parsed else ""), 20),
            "urgency": "Normal",
            "status": "Active",
            "jobDescription": job_description_html,
            "startDate": start,
            "endDate": end,
            "profilesNeeded": 1,
            "expensePaid": False,
            "specialNotes": warning_note,
            "visaTypes": truncate_field(parsed.get("visaTypes", ""), 100) if parsed else "",
            "createdBy": "Groq_Automation",
            "applications": 0,
            "assignTo": role_owner_name,  # This is the role owner name
            "assignedRecruiterIds": assigned_recruiter_ids,  # These are the recruiter IDs
            "vendor": vendor  # Which mailbox received this email
        }

        # 🔧 FIX 4: Log the complete payload being sent
        logger.info("=" * 70)
        logger.info("📤 PAYLOAD BEING SENT TO API:")
        logger.info(f"   Role Title: {role_data['role']}")
        logger.info(f"   Job ID: {role_data['jobId']}")
        logger.info(f"   GBaMS ID: {role_data['gbamsId']}")
        logger.info(f"   Client: {role_data['client']}")
        logger.info(f"   City: {role_data['city']}")
        logger.info(f"   State: {role_data['state']}")
        logger.info(f"   Country: {role_data['country']}")
        logger.info(f"   assignTo (Role Owner): {role_data['assignTo']}")
        logger.info(f"   assignedRecruiterIds: {role_data['assignedRecruiterIds']}")
        logger.info("=" * 70)

        # Get API token
        token = get_auth_token()
        if not token:
            logger.error("❌ No API token – cannot create role")
            return

        cfg = load_config()
        url = f"{cfg['api']['base_url']}/api/recruitment/roles"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        try:
            # Step 1: Create the role
            logger.info(f"📤 Sending POST request to: {url}")
            r = requests.post(url, json=role_data, headers=headers, timeout=30)
        
            logger.info(f"📥 Response Status: {r.status_code}")
        
            if r.status_code == 201:
                response_data = r.json()
                role_id = response_data.get('roleId', 'Unknown')
            
                # ✅ Register this requestId so the other mailbox skips it if it arrives there too
                if job_id and job_id.strip():
                    PROCESSED_REQUEST_IDS.add(job_id)
                    logger.info(f"🔒 Request ID '{job_id}' registered — other mailbox will skip if duplicate arrives")

                # ✅ Register vendor in watchdog so it can detect and restore if wiped
                if role_id and role_id != 'Unknown':
                    register_role_vendor(role_id, vendor)

                logger.info("=" * 70)
                logger.info(f"✅ SUCCESS! Role created → ID: {role_id}")
                logger.info(f"✅ Vendor (source mailbox): {vendor}")
                logger.info(f"✅ Role Owner: {role_owner_name} (ID: {role_owner_id})")
                logger.info(f"✅ Rates: Min={format_rate_display(bill_rate_min)}, Max={format_rate_display(bill_rate_max)}")
            
                # Step 2: Assign recruiters
                if assigned_recruiter_ids:
                    assign_url = f"{cfg['api']['base_url']}/api/recruitment/roles/{role_id}/assign-multiple-recruiters"
                    assign_payload = {"recruiterIds": assigned_recruiter_ids}
                
                    logger.info(f"📤 Attempting to assign recruiters to role {role_id}")
                    logger.info(f"📤 Assign URL: {assign_url}")
                    logger.info(f"📤 Assign Payload: {assign_payload}")
                
                    assign_response = requests.post(assign_url, json=assign_payload, headers=headers, timeout=30)
                
                    logger.info(f"📥 Assign Response Status: {assign_response.status_code}")
                
                    if assign_response.status_code == 200:
                        logger.info(f"✅ Recruiters assigned successfully to role {role_id}")
                        for rec_id in assigned_recruiter_ids:
                            rec_name = recruiter_names.get(rec_id, f"ID {rec_id}")
                            logger.info(f"   ✓ {rec_name}")
                    else:
                        logger.error(f"❌ Failed to assign recruiters: {assign_response.status_code}")
                        logger.error(f"Response: {assign_response.text}")
            
                logger.info("=" * 70 + "\n")
            
            else:
                logger.error("=" * 70)
                logger.error(f"❌ API Error {r.status_code}: {r.text}")
                logger.error("=" * 70 + "\n")
            
        except Exception as e:
            logger.error("=" * 70)
            logger.error(f"❌ Request failed: {e}")
            logger.error("=" * 70 + "\n")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
def extract_job_id_from_subject(subject):
    """
    Extract Job ID from email subject line.
    Handles various formats like:
    - "TCS - Cancelled (12345)"
    - "Cancelled - Request ID: 12345"
    - "CANCELLED: Job ABC123"
    """
    if not subject:
        return None
    
    # Pattern 1: ID in parentheses - (12345)
    match = re.search(r'\(([A-Z0-9\-_]+)\)', subject)
    if match:
        job_id = match.group(1)
        logger.info(f"✓ Found Job ID in parentheses: '{job_id}'")
        return job_id
    
    # Pattern 2: Request ID: 12345
    match = re.search(r'Request ID[:\s]+([A-Z0-9\-_]+)', subject, re.IGNORECASE)
    if match:
        job_id = match.group(1)
        logger.info(f"✓ Found Request ID: '{job_id}'")
        return job_id
    
    # Pattern 3: Job ID: 12345
    match = re.search(r'Job ID[:\s]+([A-Z0-9\-_]+)', subject, re.IGNORECASE)
    if match:
        job_id = match.group(1)
        logger.info(f"✓ Found Job ID: '{job_id}'")
        return job_id
    
    # Pattern 4: REQ-12345 or similar alphanumeric IDs
    match = re.search(r'\b(REQ-[0-9]+|[A-Z]{2,}[0-9]{3,})\b', subject, re.IGNORECASE)
    if match:
        job_id = match.group(1)
        logger.info(f"✓ Found Job ID pattern: '{job_id}'")
        return job_id
    
    logger.warning(f"⚠️ Could not extract Job ID from subject: '{subject}'")
    return None

def extract_job_id_from_body(body):
    """
    Extract Job ID from email body.
    Looks for patterns like:
    - Request ID: 12345
    - Job ID: 12345
    - GBaMS ID: 12345
    - REQUIREMENT_ID: 12345
    """
    if not body:
        return None
    
    patterns = [
        r'Request ID[:\s]+([A-Z0-9\-_]+)',
        r'Job ID[:\s]+([A-Z0-9\-_]+)',
        r'GBaMS\s*(?:ReqID|ID)[:\-\s]*([A-Z0-9\-_]+)',
        r'REQUIREMENT_ID[:\-\s]*([A-Z0-9\-_]+)',
        r'Req\s*#[:\s]*([A-Z0-9\-_]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            job_id = match.group(1)
            logger.info(f"✓ Found Job ID in body: '{job_id}'")
            return job_id
    
    logger.warning("⚠️ Could not extract Job ID from body")
    return None

def find_role_by_job_id(job_id):
    """
    Find a role in the database by its Job ID.
    Returns the role object with role_id if found, None otherwise.
    """
    if not job_id:
        logger.error("❌ Cannot search for role: Job ID is empty")
        return None
    
    try:
        token = get_auth_token()
        if not token:
            logger.error("❌ Cannot search for role: No auth token")
            return None
        
        cfg = load_config()
        url = f"{cfg['api']['base_url']}/api/recruitment/roles"
        headers = {"Authorization": f"Bearer {token}"}
        
        logger.info(f"🔍 Searching for role with Job ID: '{job_id}'")
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            roles = response.json()
            
            # Search through all roles for matching Job ID
            for role in roles:
                role_job_id = role.get('jobId', '').strip()
                
                # Case-insensitive comparison
                if role_job_id.lower() == job_id.lower():
                    logger.info(f"✅ Found matching role:")
                    logger.info(f"   - Role ID: {role.get('id')}")
                    logger.info(f"   - Role Title: {role.get('role', 'Unknown')}")
                    logger.info(f"   - Job ID: {role_job_id}")
                    logger.info(f"   - Current Status: {role.get('status', 'Unknown')}")
                    logger.info(f"   - Client: {role.get('client', 'Unknown')}")
                    return role
            
            logger.warning(f"⚠️ No role found with Job ID: '{job_id}'")
            logger.info(f"📊 Searched through {len(roles)} roles in database")
            return None
        else:
            logger.error(f"❌ Failed to fetch roles: {response.status_code} {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error searching for role: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def update_role_status_via_notes(role_id, new_status):
    """
    Alternative approach: If no status update is possible, 
    add a note to the specialNotes field to track the status change.
    
    This is a workaround when the API doesn't support direct status updates.
    
    Args:
        role_id: The ID of the role to update
        new_status: New status (e.g., "Cancelled", "On Hold", "Closed")
    
    Returns:
        True if successful, False otherwise
    """
    try:
        token = get_auth_token()
        if not token:
            logger.error("❌ Cannot update role: No auth token")
            return False
        
        cfg = load_config()
        url = f"{cfg['api']['base_url']}/api/recruitment/roles/{role_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"📤 Adding status note to role {role_id}: '{new_status}'")
        
        # Step 1: Fetch the current role data
        try:
            get_response = requests.get(url, headers=headers, timeout=10)
            
            if get_response.status_code != 200:
                logger.error(f"❌ Failed to fetch role: {get_response.status_code}")
                return False
            
            role_data = get_response.json()
            
        except Exception as e:
            logger.error(f"❌ Error fetching role: {e}")
            return False
        
        # Step 2: Update both status AND specialNotes
        from datetime import datetime
        
        old_status = role_data.get('status', 'Unknown')
        current_notes = role_data.get('specialNotes', '')
        
        # Create status change note with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status_note = f"\n[{timestamp}] STATUS CHANGED: {old_status} → {new_status} (via email automation)"
        
        # ✅ FIX: Always explicitly set vendor to whatever is in DB (never let it go null).
        # We re-read it from the GET response; if the backend returned null/empty,
        # we do NOT pop it (popping causes backends to nullify the field on PUT).
        # Instead we force-set it to the last known good value or leave what's there.
        safe_vendor = role_data.get('vendor') or ''
        safe_vendor = safe_vendor.strip()
        if safe_vendor.lower() in ('', 'null', 'none', 'n/a'):
            # Backend returned null — do not overwrite with null. Set to empty string
            # so the backend at minimum gets an explicit value, not a missing key.
            logger.warning(f"⚠️ vendor is '{safe_vendor}' from GET — setting to empty string to avoid null PUT")
            role_data['vendor'] = ''
        else:
            role_data['vendor'] = safe_vendor
            logger.info(f"✅ Vendor preserved for PUT: '{safe_vendor}'")

        # Update both fields
        role_data['status'] = new_status
        role_data['specialNotes'] = (current_notes + status_note).strip()
        
        logger.info(f"🔄 Updating:")
        logger.info(f"   Status: '{old_status}' → '{new_status}'")
        logger.info(f"   Adding note: {status_note.strip()}")
        
        # Step 3: PUT the updated role back
        try:
            put_response = requests.put(url, json=role_data, headers=headers, timeout=10)
            
            if put_response.status_code in [200, 204]:
                logger.info(f"✅ Successfully updated role {role_id}")
                logger.info(f"   ✓ Status changed to: {new_status}")
                logger.info(f"   ✓ Note added to Special Notes field")
                return True
            else:
                logger.error(f"❌ Failed to update role: {put_response.status_code}")
                logger.error(f"Response: {put_response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating role: {e}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in update_role_status_via_notes: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def update_role_status_flexible(role_id, new_status):
    """
    Flexible status update that tries multiple approaches.
    Handles None values properly.
    """
    try:
        token = get_auth_token()
        if not token:
            logger.error("❌ Cannot update role: No auth token")
            return False
        
        cfg = load_config()
        url = f"{cfg['api']['base_url']}/api/recruitment/roles/{role_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"📤 Attempting to update role {role_id} to status: '{new_status}'")
        
        # Fetch current role
        try:
            get_response = requests.get(url, headers=headers, timeout=10)
            
            if get_response.status_code != 200:
                logger.error(f"❌ Cannot fetch role {role_id}: {get_response.status_code}")
                logger.error(f"⚠️ Skipping status update for this role")
                return False
            
            role_data = get_response.json()
            old_status = role_data.get('status', 'Unknown')
            
            logger.info(f"✅ Current role status: '{old_status}'")
            
            # Check if already in target status
            if old_status.lower() == new_status.lower():
                logger.warning(f"⚠️ Role {role_id} is already '{new_status}'")
                logger.info("✓ No update needed")
                return True
            
        except Exception as e:
            logger.error(f"❌ Error fetching role: {e}")
            return False
        
        # Update status
        role_data['status'] = new_status

        # ✅ FIX: Always explicitly set vendor — never pop/omit it from PUT payload.
        # Popping causes many REST backends to interpret the missing key as null and overwrite it.
        safe_vendor = role_data.get('vendor') or ''
        safe_vendor = safe_vendor.strip()
        if safe_vendor.lower() in ('', 'null', 'none', 'n/a'):
            logger.warning(f"⚠️ vendor is '{safe_vendor}' from GET — setting to empty string to avoid null PUT")
            role_data['vendor'] = ''
        else:
            logger.info(f"✅ Vendor preserved for PUT: '{safe_vendor}'")
            role_data['vendor'] = safe_vendor
        
        # Add timestamped note - FIX: Handle None values properly
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Get current notes and handle None
        current_notes = role_data.get('specialNotes')
        if current_notes is None:
            current_notes = ""
        
        # Create status note
        status_note = f"[{timestamp}] Status changed: {old_status} → {new_status} (Email notification received)"
        
        # Combine notes - ensure both are strings
        if current_notes.strip():
            # If there are existing notes, add new note on new line
            role_data['specialNotes'] = f"{current_notes}\n{status_note}"
        else:
            # If no existing notes, just set the new note
            role_data['specialNotes'] = status_note
        
        # Try to update
        try:
            put_response = requests.put(url, json=role_data, headers=headers, timeout=10)
            
            if put_response.status_code in [200, 204]:
                logger.info("=" * 70)
                logger.info(f"✅ SUCCESS! Role updated")
                logger.info(f"   - Role ID: {role_id}")
                logger.info(f"   - Status: {old_status} → {new_status}")
                logger.info(f"   - Note added to Special Notes")
                logger.info("=" * 70)
                return True
            elif put_response.status_code == 405:
                logger.error("❌ PUT method not allowed on this endpoint")
                logger.error("⚠️ Your API may not support role updates")
                logger.error("💡 Contact your backend developer to add an update endpoint")
                return False
            else:
                logger.error(f"❌ Update failed: {put_response.status_code}")
                logger.error(f"Response: {put_response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error sending update: {e}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def handle_cancelled_email(email_data):
    """
    Handle cancelled notification emails.
    NOW NOTIFIES RECRUITERS AND ROLE OWNER.
    """
    subject = email_data['subject']
    body = email_data['body']
    
    logger.info("\n" + "╔" + "═" * 70)
    logger.info("🚫 CANCELLED EMAIL DETECTED")
    logger.info(f"📧 Subject: {subject}")
    logger.info("╚" + "═" * 70)
    
    job_id = extract_job_id_from_subject(subject) or extract_job_id_from_body(body)
    
    if not job_id:
        logger.error("❌ CRITICAL: Could not extract Job ID")
        return
    
    # Find role
    logger.info(f"🔍 Searching for role with Job ID: '{job_id}'")
    role = find_role_by_job_id(job_id)
    
    if not role:
        logger.error(f"❌ Role not found: '{job_id}'")
        logger.warning("🚨 SENDING ALERT EMAIL")
        # Alert logic (Beeline/India/US)
        send_alert_email(job_id, subject, "Cancelled", country="", email_body=body)
        return
    
    role_id = role.get('id')
    current_status = role.get('status', 'Unknown')
    
    if current_status.lower() == 'cancelled':
        logger.warning(f"⚠️ Role {role_id} is already 'Cancelled'")
        return
    
    # Update Status
    role['old_status'] = current_status
    success = update_role_status_flexible(role_id, "Cancelled")
    
    if success:
        logger.info(f"✅ Role {role_id} updated to 'Cancelled'")
        # ✅ NEW: Trigger Notification
        notify_assigned_recruiters(role_id, role, "Cancelled", subject)
    
    logger.info("╚" + "═" * 70 + "\n")
def handle_on_hold_email(email_data):
    """
    Handle 'On Hold' notification emails.
    Notifies Recruiters and Role Owner.
    """
    subject = email_data['subject']
    body = email_data['body']
    
    logger.info("\n" + "╔" + "═" * 70)
    logger.info("⏸️ ON HOLD EMAIL DETECTED")
    logger.info(f"📧 Subject: {subject}")
    logger.info("╚" + "═" * 70)
    
    job_id = extract_job_id_from_subject(subject) or extract_job_id_from_body(body)
    
    if not job_id:
        logger.error("❌ Could not extract Job ID")
        return
    
    role = find_role_by_job_id(job_id)
    if not role:
        logger.error(f"❌ Role not found: '{job_id}'")
        logger.warning("🚨 SENDING ALERT EMAIL")
        send_alert_email(job_id, subject, "On Hold", country="", email_body=body)
        return
    
    role_id = role.get('id')
    current_status = role.get('status', 'Unknown')
    
    if current_status.lower() == 'on hold':
        logger.warning(f"⚠️ Role {role_id} is already 'On Hold'")
        return
    
    role['old_status'] = current_status
    success = update_role_status_flexible(role_id, "hold")
    
    if success:
        logger.info("✅ Role status updated to 'On Hold'")
        # ✅ Trigger Notification
        notify_assigned_recruiters(role_id, role, "On Hold", subject)
    
    logger.info("╚" + "═" * 70 + "\n")
def send_alert_email(job_id, email_subject, email_type, country="", email_body=""):
    """
    Send alert email with Beeline and Subject priority logic.
    """
    try:
        import smtplib
        from email.mime.text import MIMEText
        from datetime import datetime
        
        cfg = load_config()
        ONSHORE_EMAIL = cfg['alert_emails']['onshore']
        OFFSHORE_EMAIL = cfg['alert_emails']['offshore']
        
        if not ONSHORE_EMAIL or not OFFSHORE_EMAIL:
            logger.warning("⚠️ Alert emails not configured")
            return
        
        country_lower = country.lower() if country else ""
        subject_lower = email_subject.lower()
        body_lower = email_body.lower() if email_body else ""
        
        is_india_job = False
        is_us_job = False
        
        # ✅ PRIORITY 1: Check for BEELINE (Always US/Onshore)
        if "beeline" in subject_lower or "beeline" in body_lower:
            is_us_job = True
            logger.info("✅ Detected 'Beeline' -> Routing to ONSHORE (US)")
            
        # ✅ PRIORITY 2: Check Explicit Country Argument
        elif re.search(r'\b(india|indian)\b', country_lower):
            is_india_job = True
        elif re.search(r'\b(united states|usa|us|america)\b', country_lower):
            is_us_job = True
            
        # ✅ PRIORITY 3: Check Subject Line & Body
        if not is_india_job and not is_us_job:
            # Check for US indicators
            # FIX: Added underscore handling for "USA_Analyst"
            if re.search(r'\b(usa|us|united states|ny|nj|tx|ca|fl|ga|il|ma|pa)\b', subject_lower) or \
               re.search(r'usa_', subject_lower) or \
               re.search(r'usa_', body_lower):
                is_us_job = True
                
            # Check for India indicators
            elif re.search(r'\b(india|bangalore|mumbai|hyderabad|chennai|pune)\b', subject_lower) or \
                 re.search(r'\b(india|bangalore|mumbai|hyderabad|chennai|pune)\b', body_lower):
                is_india_job = True
        
        # ✅ PRIORITY 4: Final Routing Decision
        if is_us_job:
            ALERT_EMAIL = ONSHORE_EMAIL
            location_type = "ONSHORE (US)"
        elif is_india_job:
            ALERT_EMAIL = OFFSHORE_EMAIL
            location_type = "OFFSHORE (India)"
        else:
            # Default to Onshore
            ALERT_EMAIL = ONSHORE_EMAIL
            location_type = "ONSHORE (US - Default)"
        
        # Email setup
        SMTP_SERVER = cfg['email']['server'].replace('imap', 'smtp')
        SMTP_PORT = 587
        SMTP_USERNAME = cfg['email']['username']
        SMTP_PASSWORD = cfg['email']['password']
        
        email_content = f"""
========================================
🚨 ALERT: Job ID Not Found in Database
========================================

DETAILS:
--------
Job ID: {job_id}
Email Type: {email_type}
Routing: {location_type}
Subject: {email_subject}

This alert was sent because a notification was received for a Job ID that does not exist in the system.
========================================
"""
        msg = MIMEText(email_content, 'plain')
        msg['From'] = SMTP_USERNAME
        msg['To'] = ALERT_EMAIL
        msg['Subject'] = f"🚨 Alert [{location_type}]: Job ID Not Found - {job_id}"
        
        # Send
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"✅ Alert email sent to {ALERT_EMAIL} ({location_type})")
        
    except Exception as e:
        logger.error(f"❌ Error sending alert email: {e}")

# =====================================================================
# UPDATED HANDLER FUNCTIONS WITH EMAIL ALERTS
# Replace your existing handler functions with these updated versions
# =====================================================================

def get_recruiter_email(recruiter_id):
    """
    Fetch recruiter's email address from the database.
    
    Args:
        recruiter_id: The ID of the recruiter
    
    Returns:
        Email address string or None if not found
    """
    try:
        cfg = load_config()
        token = get_auth_token()
        if not token:
            logger.error("Cannot fetch recruiter email: No auth token")
            return None
        
        # Fetch recruiter details from API
        url = f"{cfg['api']['base_url']}/api/recruitment/recruiters/{recruiter_id}"
        headers = {"Authorization": f"Bearer {token}"}
        
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            recruiter_data = r.json()
            email = recruiter_data.get('email')
            if email:
                logger.info(f"✅ Found email for recruiter ID {recruiter_id}: {email}")
                return email
            else:
                logger.warning(f"⚠️ No email found for recruiter ID {recruiter_id}")
                return None
        else:
            logger.error(f"Failed to fetch recruiter {recruiter_id}: {r.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching recruiter email: {e}")
        return None
def get_recruiter_email_by_name(name):
    """
    Fetch recruiter email by name.
    Useful for getting Role Owner's email from 'assignTo' field.
    """
    if not name: 
        return None
    
    try:
        cfg = load_config()
        token = get_auth_token()
        if not token: 
            return None
        
        # Fetch all recruiters
        url = f"{cfg['api']['base_url']}/api/recruitment/recruiters"
        headers = {"Authorization": f"Bearer {token}"}
        
        r = requests.get(url, headers=headers, timeout=10)
        
        if r.status_code == 200:
            recruiters = r.json()
            target = name.lower().strip()
            
            for rec in recruiters:
                r_name = rec.get('name', '').lower().strip()
                # Check for exact or partial match
                if target == r_name or target in r_name or r_name in target:
                    email = rec.get('email')
                    if email:
                        logger.info(f"✓ Resolved Role Owner '{name}' to {email}")
                        return email
            
            logger.warning(f"⚠️ Role Owner '{name}' found but has no email or not found in list")
        return None
            
    except Exception as e:
        logger.error(f"Error resolving email for name '{name}': {e}")
        return None

def get_assigned_recruiters_for_role(role_id):
    """
    Get list of recruiters assigned to a specific role.
    PRIORITY FIX: Force check for 'assignedRecruiters' BEFORE 'assignTo' (Role Owner).
    """
    try:
        cfg = load_config()
        token = get_auth_token()
        if not token:
            logger.error("Cannot fetch assigned recruiters: No auth token")
            return []
        
        # Fetch role details
        url = f"{cfg['api']['base_url']}/api/recruitment/roles/{role_id}"
        headers = {"Authorization": f"Bearer {token}"}
        
        logger.info(f"🔍 Fetching role details from: {url}")
        
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            role_data = r.json()
            
            assigned_data = None
            
            # ✅ PRIORITY FIX: 
            # 1. Look for explicit RECRUITER fields first (Ajay)
            # 2. Look for ROLE OWNER fields last (Rishi)
            possible_fields = [
                'assignedRecruiters',    # Priority 1: Plural (Common)
                'assignedRecruiter',     # Priority 2: Singular (User specified)
                'AssignedRecruiters',    # Priority 3: PascalCase
                'AssignedRecruiter',     # Priority 4: PascalCase
                'assigned_recruiters',   # Priority 5: Snake_case
                'recruiters',            # Priority 6: Generic list
                'recruiter',             # Priority 7: Generic singular
                'recruiterName',         # Priority 8: Name string
                'assignTo'               # Priority 9: LAST RESORT (Role Owner)
            ]
            
            field_found = ""
            for field in possible_fields:
                val = role_data.get(field)
                
                # Check if field exists and has real data
                if val:
                    # If it's a list with items, use it immediately
                    if isinstance(val, list) and len(val) > 0:
                        assigned_data = val
                        field_found = field
                        logger.info(f"✅ Found recruiter list in field: '{field}' (Count: {len(val)})")
                        break
                    
                    # If it's a dictionary (object), use it immediately
                    elif isinstance(val, dict):
                        assigned_data = val
                        field_found = field
                        logger.info(f"✅ Found recruiter object in field: '{field}'")
                        break
                    
                    # If it's a string, use it ONLY if it's not empty
                    elif isinstance(val, str) and val.strip():
                        assigned_data = val
                        field_found = field
                        logger.info(f"✅ Found recruiter name in field: '{field}'")
                        break
            
            if not assigned_data:
                logger.warning(f"⚠️ No assigned recruiters found in role {role_id}")
                return []
            
            recruiters_with_emails = []
            items = []
            
            # CASE 1: Data is a List
            if isinstance(assigned_data, list):
                items = assigned_data
                
            # CASE 2: Data is a Single Dictionary
            elif isinstance(assigned_data, dict):
                items = [assigned_data]
                
            # CASE 3: Data is a String (Name)
            elif isinstance(assigned_data, str):
                logger.info(f"ℹ️ Recruiter data is a string ('{assigned_data}'), resolving to ID/Email...")
                
                # Fetch all recruiters to find the matching name
                all_recruiters_url = f"{cfg['api']['base_url']}/api/recruitment/recruiters"
                r_all = requests.get(all_recruiters_url, headers=headers, timeout=10)
                
                if r_all.status_code == 200:
                    all_recruiters = r_all.json()
                    target_name = assigned_data.lower().strip()
                    found_recruiter = None
                    
                    for rec in all_recruiters:
                        rec_name = rec.get('name', '').lower().strip()
                        if target_name == rec_name or target_name in rec_name or rec_name in target_name:
                            found_recruiter = rec
                            logger.info(f"✅ Matched name '{assigned_data}' to Recruiter: {rec.get('name')} (ID: {rec.get('id')})")
                            break
                    
                    if found_recruiter:
                        items = [found_recruiter]
                    else:
                        logger.warning(f"⚠️ Could not find recruiter profile for name: '{assigned_data}'")
                        return []
                else:
                    logger.error("❌ Failed to fetch recruiter list for name resolution")
                    return []
            else:
                return []

            # Process items to get emails
            for recruiter in items:
                recruiter_id = None
                recruiter_name = 'Unknown'
                recruiter_email = None

                if isinstance(recruiter, dict):
                    recruiter_id = recruiter.get('id') or recruiter.get('recruiterId')
                    recruiter_name = recruiter.get('name') or recruiter.get('recruiterName', 'Unknown')
                    recruiter_email = recruiter.get('email')
                
                # Try to get email if missing
                if not recruiter_email and recruiter_id:
                    recruiter_email = get_recruiter_email(recruiter_id)
                
                if recruiter_email:
                    recruiters_with_emails.append({
                        'id': recruiter_id,
                        'name': recruiter_name,
                        'email': recruiter_email
                    })
                    logger.info(f"✅ Added recruiter for notification: {recruiter_name} ({recruiter_email})")
                else:
                    logger.warning(f"⚠️ Skipping recruiter {recruiter_name} - no email found")
            
            return recruiters_with_emails
            
        else:
            logger.error(f"Failed to fetch role {role_id}: {r.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"Error fetching assigned recruiters: {e}")
        return []


def send_recruiter_notification(recruiter_email, recruiter_name, role_data, notification_type, original_subject, cc_email=None):
    """
    Send email notification to assigned recruiter about role status change.
    Now supports CCing the Role Owner.
    """
    try:
        import smtplib
        from email.mime.text import MIMEText
        from datetime import datetime
        
        cfg = load_config()
        
        # Email server settings
        SMTP_SERVER = cfg['email']['server'].replace('imap', 'smtp')
        SMTP_PORT = 587
        SMTP_USERNAME = cfg['email']['username']
        SMTP_PASSWORD = cfg['email']['password']
        
        logger.info(f"📧 Preparing notification email...")
        logger.info(f"   To: {recruiter_email} ({recruiter_name})")
        if cc_email:
            logger.info(f"   Cc: {cc_email} (Role Owner)")
        logger.info(f"   Type: {notification_type}")
        
        # Extract role information
        job_id = role_data.get('jobId', 'N/A')
        role_title = role_data.get('role', 'Unknown')
        client = role_data.get('client', 'Unknown')
        location = f"{role_data.get('city', '')}, {role_data.get('state', '')}, {role_data.get('country', '')}"
        old_status = role_data.get('old_status', 'Unknown')
        new_status = role_data.get('status', notification_type)
        role_owner = role_data.get('assignTo', 'Unknown')
        
        # Create timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create simple text email
        email_body = f"""
Hi {recruiter_name},

This is an automated notification regarding a role status update.

========================================
ROLE STATUS CHANGED TO: {notification_type.upper()}
========================================

ROLE DETAILS:
-------------
Job ID: {job_id}
Role Title: {role_title}
Client: {client}
Location: {location}
Role Owner: {role_owner}
Previous Status: {old_status}
New Status: {new_status}
Timestamp: {timestamp}

ORIGINAL NOTIFICATION:
----------------------
{original_subject}

ACTION REQUIRED:
----------------
"""
        
        if notification_type == "On Hold":
            email_body += """
- This role has been put ON HOLD by the client
- Do NOT submit new candidates for this role
- Inform any candidates in the pipeline about the hold status
- Monitor for re-opening notification
"""
        elif notification_type in ["Modified", "Updated"]:
            email_body += """
- This role has been MODIFIED/UPDATED
- Review the changes in the recruitment system
- Check if any requirements have changed
- Update your candidate pipeline accordingly
"""
        
        email_body += """

Please log into the recruitment system for full details.

========================================
This is an automated message. Please do not reply.
========================================
"""
        
        # Create email message
        msg = MIMEText(email_body, 'plain')
        msg['From'] = SMTP_USERNAME
        msg['To'] = recruiter_email
        if cc_email:
            msg['Cc'] = cc_email
            
        msg['Subject'] = f"Role Status Update: {notification_type} - {role_title} ({job_id})"
        
        # Send email
        try:
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()

            # ── Login: OAuth2 first, basic auth as fallback ──
            if _OAUTH_READY and SMTP_USERNAME:
                try:
                    access_token = _get_oauth_access_token(SMTP_USERNAME)
                    # Build XOAUTH2: raw string → base64 → decoded to str for the AUTH command
                    xoauth2 = base64.b64encode(
                        f"user={SMTP_USERNAME}\x01auth=Bearer {access_token}\x01\x01".encode("ascii")
                    ).decode("ascii")
                    server.docmd('AUTH', 'XOAUTH2 ' + xoauth2)
                    logger.info("✅ OAuth2 SMTP login successful")
                except Exception as oauth_err:
                    logger.warning(f"⚠️ SMTP OAuth2 failed ({oauth_err}) — trying basic auth...")
                    if SMTP_PASSWORD:
                        server.login(SMTP_USERNAME, SMTP_PASSWORD)
                    else:
                        raise
            elif SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            
            # send_message automatically handles To and Cc headers
            server.send_message(msg)
            
            server.quit()
            
            logger.info(f"✅ Notification sent to {recruiter_name}" + (f" (CC: {cc_email})" if cc_email else ""))
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email via SMTP: {e}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error in send_recruiter_notification: {e}")
        return False

def notify_assigned_recruiters(role_id, role_data, notification_type, original_subject):
    """
    Notify assigned recruiters AND Role Owner about status changes.
    - If Recruiter exists: Email Recruiter + CC Role Owner
    - If NO Recruiter: Email Role Owner directly
    """
    try:
        logger.info("=" * 70)
        logger.info(f"📧 NOTIFYING STAKEHOLDERS (Recruiters & Owner)")
        logger.info(f"   Role ID: {role_id}")
        logger.info(f"   Notification Type: {notification_type}")
        logger.info("=" * 70)
        
        # 1. Get Role Owner Email
        role_owner_name = role_data.get('assignTo')
        role_owner_email = None
        
        if role_owner_name:
            role_owner_email = get_recruiter_email_by_name(role_owner_name)
            if role_owner_email:
                logger.info(f"📋 Role Owner identified: {role_owner_name} <{role_owner_email}>")
            else:
                logger.warning(f"⚠️ Role Owner '{role_owner_name}' has no email address found")
        
        # 2. Get assigned recruiters
        recruiters = get_assigned_recruiters_for_role(role_id)
        
        # CASE A: No Recruiters Assigned -> Send to Role Owner Only
        if not recruiters:
            logger.warning("⚠️ No recruiters assigned to this role.")
            
            if role_owner_email:
                logger.info(f"📧 Sending notification DIRECTLY to Role Owner ({role_owner_name})...")
                success = send_recruiter_notification(
                    recruiter_email=role_owner_email,
                    recruiter_name=role_owner_name,
                    role_data=role_data,
                    notification_type=notification_type,
                    original_subject=original_subject,
                    cc_email=None # No CC needed, sending direct
                )
                if success:
                    logger.info("✅ Notification sent to Role Owner")
                return 1
            else:
                logger.error("❌ No recruiters AND no Role Owner email found. No notification sent.")
                return 0
        
        # CASE B: Recruiters Assigned -> Email Recruiter + CC Owner
        success_count = 0
        for recruiter in recruiters:
            # Don't CC the owner if the owner IS the recruiter (prevent duplicate)
            current_cc = role_owner_email
            if role_owner_email and recruiter['email'].lower() == role_owner_email.lower():
                current_cc = None
            
            logger.info(f"\n📧 Sending notification to {recruiter['name']}...")
            
            success = send_recruiter_notification(
                recruiter_email=recruiter['email'],
                recruiter_name=recruiter['name'],
                role_data=role_data,
                notification_type=notification_type,
                original_subject=original_subject,
                cc_email=current_cc
            )
            
            if success:
                success_count += 1
            else:
                logger.error(f"❌ Failed to send notification to {recruiter['name']}")
        
        logger.info("\n" + "=" * 70)
        logger.info(f"📊 NOTIFICATION SUMMARY:")
        logger.info(f"   Recruiters Notified: {success_count}")
        if role_owner_email:
            logger.info(f"   Role Owner Included: Yes ({role_owner_email})")
        logger.info("=" * 70 + "\n")
        
        return success_count
        
    except Exception as e:
        logger.error(f"❌ Error in notify_assigned_recruiters: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 0


# ===================== UPDATED HANDLER FUNCTIONS =====================
# Replace your existing handle_on_hold_email and handle_modified_email functions with these:

def handle_modified_email(email_data):
    """
    Handle 'Modified' or 'Updated' notification emails.
    Notifies Recruiters and Role Owner.
    """
    subject = email_data['subject']
    body = email_data['body']
    
    logger.info("\n" + "╔" + "═" * 70)
    logger.info("📄 MODIFIED/UPDATED EMAIL DETECTED")
    logger.info(f"📧 Subject: {subject}")
    logger.info("╚" + "═" * 70)
    
    job_id = extract_job_id_from_subject(subject) or extract_job_id_from_body(body)
    
    if not job_id:
        logger.error("❌ Could not extract Job ID")
        return
    
    role = find_role_by_job_id(job_id)
    if not role:
        logger.error(f"❌ Role not found: '{job_id}'")
        logger.warning("🚨 SENDING ALERT EMAIL")
        email_type = "Updated" if 'updated' in subject.lower() else "Modified"
        send_alert_email(job_id, subject, email_type, country="", email_body=body)
        return
    
    role_id = role.get('id')
    current_status = role.get('status', 'Unknown')
    new_status = "Updated" if 'updated' in subject.lower() else "Modified"
    
    if current_status == new_status:
        logger.warning(f"⚠️ Role {role_id} is already '{new_status}'")
        return
    
    role['old_status'] = current_status
    success = update_role_status_flexible(role_id, new_status)
    
    if success:
        logger.info(f"✅ Role status updated to {new_status}")
        # ✅ Trigger Notification
        notify_assigned_recruiters(role_id, role, new_status, subject)
    
    logger.info("╚" + "═" * 70 + "\n")

def handle_reopened_email(email_data):
    """
    Handle 'Re-opened' notification emails.
    Notifies Recruiters and Role Owner.
    """
    subject = email_data['subject']
    body = email_data['body']
    
    logger.info("\n" + "╔" + "═" * 70)
    logger.info("🔓 RE-OPENED EMAIL DETECTED")
    logger.info(f"📧 Subject: {subject}")
    logger.info("╚" + "═" * 70)
    
    job_id = extract_job_id_from_subject(subject) or extract_job_id_from_body(body)
    
    if not job_id:
        logger.error("❌ Could not extract Job ID")
        return
    
    role = find_role_by_job_id(job_id)
    if not role:
        logger.error(f"❌ Role not found: '{job_id}'")
        logger.warning("🚨 SENDING ALERT EMAIL")
        send_alert_email(job_id, subject, "Re-opened", country="", email_body=body)
        return
    
    role_id = role.get('id')
    current_status = role.get('status', 'Unknown')
    role['old_status'] = current_status
    
    if current_status.lower() == 'active':
        logger.warning(f"⚠️ Role {role_id} is already 'Active'")
        # Still notify even if status doesn't change? Usually better to just log.
        # But if you want a reminder notification, uncomment the next line:
        # notify_assigned_recruiters(role_id, role, "Re-opened", subject)
        return
    
    success = update_role_status_flexible(role_id, "Active")
    if success:
        logger.info(f"✅ Role {role_id} re-opened (Active)")
        # ✅ Trigger Notification
        notify_assigned_recruiters(role_id, role, "Re-opened", subject)
    
    logger.info("╚" + "═" * 70 + "\n")

# ===================== START =====================
# ===================== START WITH ENHANCED LOGGING =====================
if __name__ == "__main__":

    load_config()

    logger.info("=" * 80)
    logger.info("🚀 Groq AI Email Automation STARTED")
    logger.info("=" * 80)

    # ── OAuth2 + API credential status ───────────────────────────────────────
    logger.info("🔍 AUTH CONFIGURATION:")
    logger.info(f"   AZURE_CLIENT_ID     : {'✅ set' if _AZURE_CLIENT_ID     else '❌ NOT SET'}")
    logger.info(f"   AZURE_CLIENT_SECRET : {'✅ set' if _AZURE_CLIENT_SECRET else '❌ NOT SET'}")
    logger.info(f"   AZURE_TENANT_ID     : {'✅ set' if _AZURE_TENANT_ID     else '❌ NOT SET'}")
    logger.info(f"   AZURE_EMAIL         : {_AZURE_EMAIL  or '❌ NOT SET'}")
    logger.info(f"   AZURE_EMAIL2        : {_AZURE_EMAIL2 or '(not set — single mailbox mode)'}")
    logger.info(f"   OAuth2 ready        : {'✅ YES' if _OAUTH_READY else '❌ NO'}")
    logger.info(f"   API_SERVICE_USERNAME: {'✅ set' if CONFIG['api'].get('service_username') else '❌ NOT SET — add to .env'}")
    logger.info(f"   API_SERVICE_PASSWORD: {'✅ set' if CONFIG['api'].get('service_password') else '❌ NOT SET — add to .env'}")
    logger.info("=" * 80)

    if not CONFIG['api'].get('service_username') or not CONFIG['api'].get('service_password'):
        logger.error("❌ API credentials missing! Add these to your .env file:")
        logger.error("   API_SERVICE_USERNAME=your_api_username")
        logger.error("   API_SERVICE_PASSWORD=your_api_password")
        logger.error("   API_BASE_URL=https://prophechyerp.duckdns.org")
        exit(1)

    # Initialize round-robin
    initialize_round_robin()
    status = get_round_robin_status()

    logger.info("📊 ROUND-ROBIN ASSIGNMENT STATUS:")
    logger.info(f"   Role Owners Pool: {ROLE_OWNER_IDS}")
    logger.info(f"   Current Index: {status['current_owner_index']}")
    logger.info(f"   Next Role Owner: ID {status['next_owner_id']}")

    next_owner_name = get_role_owner_name(status['next_owner_id'])
    if next_owner_name:
        logger.info(f"   Next Owner Name: {next_owner_name}")

    logger.info(f"   Recruiters Pool: {RECRUITER_IDS}")
    logger.info(f"   Current Index: {status['current_recruiter_index']}")
    logger.info(f"   Next Recruiter: ID {status['next_recruiter_id']}")
    logger.info(f"   Recruiters per Role: {status['recruiters_per_role']}")

    next_recruiter_names = get_recruiter_names([status['next_recruiter_id']])
    if next_recruiter_names:
        logger.info(f"   Next Recruiter Name: {next_recruiter_names.get(status['next_recruiter_id'], 'Unknown')}")

    logger.info("=" * 80)
    logger.info("📧 Monitoring inbox for emails...")
    logger.info("=" * 80 + "\n")

    # ── Auth mode per mailbox ─────────────────────────────────────────────────
    _primary_oauth   = _OAUTH_READY and bool(_AZURE_EMAIL)
    _secondary_oauth = _OAUTH_READY and bool(_AZURE_EMAIL2)

    # ===================== PRIMARY MAILBOX =====================
    if _primary_oauth:
        _p_user = _AZURE_EMAIL
        _p_name = CONFIG['email'].get('display_name', _AZURE_EMAIL)
        listener1 = EmailListener(
            server="outlook.office365.com", port=993,
            username=_p_user, password=None,
            ssl_required=True, mailbox_name=_p_name,
            mailbox_id='email', use_oauth=True,
        )
        logger.info(f"🔑 OAuth2  PRIMARY  → {_p_user}")
    else:
        _p_user = CONFIG['email']['username']
        listener1 = EmailListener(
            server=CONFIG['email']['server'],
            port=CONFIG['email']['port'],
            username=_p_user,
            password=CONFIG['email']['password'],
            ssl_required=CONFIG['email']['ssl_required'],
            mailbox_name=CONFIG['email']['display_name'],
            mailbox_id='email', use_oauth=False,
        )
        logger.info(f"🔐 Basic   PRIMARY  → {_p_user}")

    def run_listener1():
        logger.info(f"📬 Starting PRIMARY mailbox listener: {_p_user}")
        listener1.start_listening(handle_new_email, check_interval=60)

    thread1 = threading.Thread(target=run_listener1, name="Mailbox-Primary", daemon=True)
    thread1.start()
    logger.info(f"✅ Primary mailbox thread started: {_p_user}")

    # ===================== VENDOR WATCHDOG THREAD =====================
    watchdog_thread = threading.Thread(
        target=vendor_watchdog,
        kwargs={"check_interval_seconds": 120},
        name="Vendor-Watchdog",
        daemon=True
    )
    watchdog_thread.start()
    logger.info("🐕 Vendor watchdog thread started (checks every 120s)")

    # ===================== SECONDARY MAILBOX =====================
    _has_email2_config = CONFIG.get('email2') and CONFIG['email2'].get('username')

    if _secondary_oauth:
        _s_user = _AZURE_EMAIL2
        _s_name = (CONFIG['email2'].get('display_name', _AZURE_EMAIL2)
                   if _has_email2_config else _AZURE_EMAIL2)
        listener2 = EmailListener(
            server="outlook.office365.com", port=993,
            username=_s_user, password=None,
            ssl_required=True, mailbox_name=_s_name,
            mailbox_id='email2', use_oauth=True,
        )
        logger.info(f"🔑 OAuth2  SECONDARY → {_s_user}")

        def run_listener2():
            logger.info(f"📬 Starting SECONDARY mailbox listener: {_s_user}")
            listener2.start_listening(handle_new_email, check_interval=60)

        thread2 = threading.Thread(target=run_listener2, name="Mailbox-Secondary", daemon=True)
        thread2.start()
        logger.info(f"✅ Secondary mailbox thread started: {_s_user}")

    elif _has_email2_config:
        _s_user = CONFIG['email2']['username']
        listener2 = EmailListener(
            server=CONFIG['email2']['server'],
            port=CONFIG['email2']['port'],
            username=_s_user,
            password=CONFIG['email2']['password'],
            ssl_required=CONFIG['email2']['ssl_required'],
            mailbox_name=CONFIG['email2']['display_name'],
            mailbox_id='email2', use_oauth=False,
        )
        logger.info(f"🔐 Basic   SECONDARY → {_s_user}")

        def run_listener2():
            logger.info(f"📬 Starting SECONDARY mailbox listener: {_s_user}")
            listener2.start_listening(handle_new_email, check_interval=60)

        thread2 = threading.Thread(target=run_listener2, name="Mailbox-Secondary", daemon=True)
        thread2.start()
        logger.info(f"✅ Secondary mailbox thread started: {_s_user}")

    else:
        logger.info("ℹ️  No secondary mailbox configured — single mailbox mode")

    # ===================== KEEP MAIN THREAD ALIVE =====================
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping all listeners...")