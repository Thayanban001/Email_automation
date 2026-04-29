# 📧 AI Email Automation — Groq-Powered Inbox Intelligence

A production-grade email automation system that **listens to mailboxes in real time**, uses **Groq AI** to classify and process incoming emails, and automatically updates job roles, assigns recruiters, and sends notifications — all without human intervention.

---

## 🧠 How It Works

```
IMAP Inbox → Email Detected → Groq AI Classification → Action (Update Role / Assign / Notify) → Log
```

1. System **monitors** one or two IMAP mailboxes simultaneously (threading)
2. New emails are detected and parsed (subject, body, attachments)
3. **Groq AI** classifies the email type (new role, closed, re-opened, vendor, etc.)
4. Business logic executes: role status updated, recruiters assigned via **round-robin**, alerts sent
5. All actions logged with full audit trail

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| AI Classification | Groq LLaMA |
| Email Protocol | IMAP (SSL + Microsoft OAuth2 XOAUTH2) |
| Auth | Microsoft MSAL (Azure AD OAuth2) |
| Threading | Python `threading` + dual mailbox listeners |
| Language | Python 3.10+ |

---

## 🚀 Features

- ✅ **Real-time inbox monitoring** — checks every 60 seconds
- ✅ **Dual mailbox support** — primary + secondary mailbox simultaneously
- ✅ **Microsoft OAuth2 (XOAUTH2)** — secure, password-free IMAP auth
- ✅ **AI email classification** — new role, closed, re-opened, vendor emails
- ✅ **Round-robin assignment** — auto-assigns role owners (IDs 4,5,6) and recruiters in sequence
- ✅ **Persistent counter state** — round-robin survives restarts via JSON file
- ✅ **Vendor watchdog thread** — background monitor for vendor-related emails
- ✅ **Alert emails** — automatic notifications when job IDs can't be matched
- ✅ **Token caching** — OAuth2 tokens cached to avoid repeated auth calls
- ✅ **Graceful shutdown** — Ctrl+C stops all threads cleanly

---

## 📁 Project Structure

```
email-automation/
├── email_listener.py        # Main automation script
├── round_robin_state.json   # Auto-managed counter state
├── requirements.txt         # Dependencies
├── .env.example             # Environment variable template
└── README.md
```

---

## 🔧 Setup & Installation

### 1. Clone the repo
```bash
git clone https://github.com/your-username/ai-email-automation.git
cd ai-email-automation
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
```

Edit `.env`:
```env
# Azure OAuth2 (for Microsoft/Outlook mailboxes)
AZURE_CLIENT_ID=your_client_id
AZURE_CLIENT_SECRET=your_client_secret
AZURE_TENANT_ID=your_tenant_id
AZURE_EMAIL=primary@yourdomain.com
AZURE_EMAIL2=secondary@yourdomain.com   # optional

# API credentials
API_SERVICE_USERNAME=your_username
API_SERVICE_PASSWORD=your_password
API_BASE_URL=https://your-api.com

# Groq
GROQ_API_KEY=your_groq_api_key
```

### 4. Run
```bash
python email_listener.py
```

---

## 🔄 Round-Robin Assignment Logic

```
Role Owners Pool: [ID 4, ID 5, ID 6]  → rotates: 4 → 5 → 6 → 4 → ...
Recruiters Pool:  [ID 8, 9, 11, 40, 43, 48, 58, 59]  → rotates in sequence
```

State is saved to `round_robin_state.json` after every assignment — survives restarts.

---

## 🌍 Use Cases

- Recruitment workflow automation
- HR operations email processing
- Job board integrations via email triggers
- Any domain requiring inbox-driven business logic

---

## 📌 Requirements

```
groq
msal
python-dotenv
requests
```

---

## 👤 Author

**Thayanban Thamizhendhal**  
Python Developer | AI/ML Engineer  
[LinkedIn](https://linkedin.com/in/thayanbanthamizhendhal) · AWS Certified AI Practitioner
