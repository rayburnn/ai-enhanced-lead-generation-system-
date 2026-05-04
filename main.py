import requests
import re
import dns.resolver
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from concurrent.futures import ThreadPoolExecutor
import threading
import argparse
from datetime import datetime, timedelta, timezone
import csv
import os
import time
import smtplib
import ssl
import socket
import random
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pymongo import MongoClient
from groq import Groq

# -----------------------------
# CONFIG
# -----------------------------
MAX_DEPTH = 2
THREADS = 10
MAX_START_URLS = 25
DNS_VALIDATE = False

SMTP_VALIDATE_MAX = 50
SMTP_DELAY_MIN = 3
SMTP_DELAY_MAX = 8

SENDER_ID = "default_sender"
DAILY_SEND_QUOTA = 100
PER_DOMAIN_COOLDOWN_SECONDS = 90
STEP7_LIMIT = 50

QUOTA_RESERVE_STATUS = "reserved"
QUOTA_SENT_STATUS = "sent"
QUOTA_EXPORTED_STATUS = "exported"

STOP_WORDS = {"in", "at", "near", "the", "of", "for", "and"}

PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com",
    "outlook.com", "icloud.com", "protonmail.com"
}
DISPOSABLE_DOMAINS = {
    "tempmail.com", "10minutemail.com", "mailinator.com"
}
ROLE_PREFIXES = {"info", "support", "admin", "contact", "sales"}

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
VALID_EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

# -----------------------------
# SMTP CONFIG
# -----------------------------
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_USER = "joyboyraj800@gmail.com"
SMTP_PASS = "rcqv qmho bvss loaf"  # ⚠️ Generate a new one — this was exposed publicly

DEFAULT_FROM_NAME = "Raj"
DEFAULT_SENDER_EMAIL = SMTP_USER
DEFAULT_SUBJECT = "Inquiry from Lead Generation"

SEND_DELAY_SECONDS = 8
MAX_SEND_ATTEMPTS = 3

visited_urls = set()
visited_lock = threading.Lock()

# -----------------------------
# GROQ AI CONFIG
# -----------------------------
GROQ_API_KEY = "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"


# -----------------------------
# AI LEAD SCORING
# -----------------------------
def ai_score_leads(emails: list) -> list:
    """
    Uses Groq to rank emails by lead quality before sending.
    Best leads are sent first.
    Falls back to original order if AI fails.
    """
    if not GROQ_API_KEY or not emails:
        return emails

    prompt = f"""You are a B2B lead quality expert. Rank these emails from best to worst lead quality.

Ranking rules:
- Business domain (school.edu, company.in) = best
- Role emails (info@, contact@, admin@) = medium
- Personal domain (gmail, yahoo, hotmail) = worst

Emails to rank: {json.dumps(emails)}

Return ONLY a JSON array of the emails in order from best to worst.
No explanation, no markdown, just the JSON array like this:
["email1@domain.com", "email2@domain.com"]"""

    try:
        client_groq = Groq(api_key=GROQ_API_KEY)
        response = client_groq.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        ranked = json.loads(raw)

        if isinstance(ranked, list) and len(ranked) > 0:
            email_set = set(emails)
            # Keep only valid emails in AI's preferred order
            ranked_valid = [e for e in ranked if e in email_set]
            # Add any emails AI missed at the end
            missed = [e for e in emails if e not in set(ranked_valid)]
            final = ranked_valid + missed
            print(f"🤖 AI ranked {len(final)} emails by quality (best first)")
            return final
        else:
            print("⚠️ AI returned empty list — using original order")
            return emails

    except Exception as e:
        print(f"⚠️ AI ranking failed: {e} — using original order")
        return emails


# -----------------------------
# MONGODB SETUP
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["leadgen"]
collection = db["emails"]
quota_collection = db["quota_usage"]
send_log_collection = db["send_logs"]


# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def _utc_now():
    return datetime.now(timezone.utc)


def _day_window_utc(now=None):
    now = now or _utc_now()
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def clean_email(email):
    email = unquote(email)
    email = email.replace("mailto:", "").replace(" ", "").replace("%20", "").replace("\n", "").replace("\r", "")
    return email.strip().lower()


def is_valid_format(email):
    return re.match(VALID_EMAIL_REGEX, email) is not None


def dns_validate_email(email):
    domain = email.split("@", 1)[1]
    try:
        dns.resolver.resolve(domain, "MX", lifetime=5)
        return True
    except Exception:
        return False


# -----------------------------
# SMTP VALIDATION
# -----------------------------
def smtp_validate_email(email: str) -> bool:
    if not is_valid_format(email):
        return False
    domain = email.split("@", 1)[1]
    try:
        mx_records = dns.resolver.resolve(domain, 'MX', lifetime=8)
        mx_host = str(mx_records[0].exchange).rstrip('.')
        time.sleep(random.uniform(SMTP_DELAY_MIN, SMTP_DELAY_MAX))
        with smtplib.SMTP(mx_host, port=25, timeout=12) as server:
            server.ehlo_or_helo_if_needed()
            if server.has_extn('STARTTLS'):
                server.starttls()
                server.ehlo()
            server.mail("verify@noreply.example.com")
            code, _ = server.rcpt(email)
            server.quit()
            if code == 250:
                print(f"✅ SMTP Valid: {email}")
                return True
            else:
                print(f"❌ SMTP Rejected: {email} (Code: {code})")
                return False
    except dns.resolver.NXDOMAIN:
        print(f"❌ Domain not found: {domain}")
        return False
    except socket.timeout:
        print(f"⚠️ Timeout for {email}")
        return False
    except Exception as e:
        print(f"⚠️ SMTP check failed for {email}: {type(e).__name__}")
        return False


def is_disposable(email):
    domain = email.split("@", 1)[1]
    return domain in DISPOSABLE_DOMAINS


def is_role_email(email):
    prefix = email.split("@", 1)[0]
    return prefix in ROLE_PREFIXES


def classify_email(email):
    domain = email.split("@", 1)[1]
    return "personal" if domain in PERSONAL_DOMAINS else "professional"


def score_email(email):
    score = 0
    domain = email.split("@", 1)[1]
    prefix = email.split("@", 1)[0]
    if domain not in PERSONAL_DOMAINS:
        score += 2
    if not is_role_email(email):
        score += 2
    if len(prefix) > 3:
        score += 1
    return score


# -----------------------------
# QUOTA SYSTEM
# -----------------------------
def get_daily_sent_count(sender_id: str) -> int:
    start, end = _day_window_utc()
    return quota_collection.count_documents({
        "sender_id": sender_id,
        "status": QUOTA_SENT_STATUS,
        "sent_at": {"$gte": start, "$lt": end},
    })


def has_domain_cooldown(email: str, sender_id: str) -> bool:
    domain = email.split("@", 1)[1]
    cutoff = _utc_now() - timedelta(seconds=PER_DOMAIN_COOLDOWN_SECONDS)
    recent = quota_collection.find_one({
        "sender_id": sender_id,
        "domain": domain,
        "status": QUOTA_SENT_STATUS,
        "sent_at": {"$gte": cutoff},
    }, sort=[("sent_at", -1)])
    return recent is not None


def check_quota(email: str, sender_id: str) -> tuple[bool, str]:
    if get_daily_sent_count(sender_id) >= DAILY_SEND_QUOTA:
        return False, "daily_quota_exhausted"
    if has_domain_cooldown(email, sender_id):
        return False, "domain_cooldown_active"
    return True, "eligible"


def reserve_quota(email: str, sender_id: str, batch_id: str) -> bool:
    domain = email.split("@", 1)[1]
    existing = quota_collection.find_one({
        "sender_id": sender_id,
        "email": email,
        "batch_id": batch_id,
        "status": QUOTA_RESERVE_STATUS,
    })
    if existing:
        return True
    quota_collection.insert_one({
        "sender_id": sender_id,
        "batch_id": batch_id,
        "email": email,
        "domain": domain,
        "reserved_at": _utc_now(),
        "status": QUOTA_RESERVE_STATUS,
    })
    return True


def get_quota_eligible_emails(limit: int, sender_id: str, reserve: bool, batch_id: str):
    cursor = collection.find({}, {"email": 1, "score": 1}).sort("score", -1).limit(limit * 5)
    selected = []
    rejected = {"daily_quota_exhausted": 0, "domain_cooldown_active": 0, "already_reserved": 0}
    for doc in cursor:
        email = doc.get("email")
        if not email:
            continue
        ok, reason = check_quota(email, sender_id)
        if not ok:
            rejected[reason] = rejected.get(reason, 0) + 1
            continue
        if reserve:
            already = quota_collection.find_one({
                "sender_id": sender_id, "batch_id": batch_id, "email": email, "status": QUOTA_RESERVE_STATUS
            })
            if already:
                rejected["already_reserved"] = rejected.get("already_reserved", 0) + 1
            else:
                reserve_quota(email, sender_id, batch_id)
        selected.append(email)
        if len(selected) >= limit:
            break
    return selected, rejected


def export_step8_csv(emails: list, batch_id: str, sender_id: str, csv_path: str):
    if not emails:
        print("No emails to export.")
        return
    docs = list(collection.find({"email": {"$in": emails}}))
    docs_by_email = {d.get("email"): d for d in docs if d.get("email")}
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["email", "score", "type", "source", "batch_id", "sender_id"])
        for email in emails:
            d = docs_by_email.get(email, {})
            w.writerow([email, d.get("score"), d.get("type"), d.get("source"), batch_id, sender_id])
    quota_collection.update_many(
        {"sender_id": sender_id, "batch_id": batch_id, "email": {"$in": emails}, "status": QUOTA_RESERVE_STATUS},
        {"$set": {"status": QUOTA_EXPORTED_STATUS, "exported_at": _utc_now()}}
    )
    print(f"Step 8 export saved: {csv_path}")


def mark_step8_as_sent(batch_id: str, sender_id: str):
    res = quota_collection.update_many(
        {"sender_id": sender_id, "batch_id": batch_id, "status": {"$in": [QUOTA_RESERVE_STATUS, QUOTA_EXPORTED_STATUS]}},
        {"$set": {"status": QUOTA_SENT_STATUS, "sent_at": _utc_now()}}
    )
    print(f"Marked as sent: {getattr(res, 'modified_count', 0)} documents")


# -----------------------------
# SMTP SENDING
# -----------------------------
def send_single_email(to_email: str, subject: str, body_html: str, batch_id: str = None) -> bool:
    sender = f"{DEFAULT_FROM_NAME} <{DEFAULT_SENDER_EMAIL}>"
    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    plain_text = BeautifulSoup(body_html, "html.parser").get_text(strip=True) if body_html else body_html
    msg.attach(MIMEText(plain_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))
    context = ssl.create_default_context()
    for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
        try:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(DEFAULT_SENDER_EMAIL, to_email, msg.as_string())
            print(f"✅ Successfully sent to: {to_email}")
            if batch_id:
                send_log_collection.insert_one({
                    "sender_id": SENDER_ID, "batch_id": batch_id,
                    "email": to_email, "status": "sent",
                    "sent_at": _utc_now(), "subject": subject
                })
                quota_collection.update_one(
                    {"sender_id": SENDER_ID, "email": to_email, "batch_id": batch_id},
                    {"$set": {"status": QUOTA_SENT_STATUS, "sent_at": _utc_now()}}
                )
            return True
        except smtplib.SMTPAuthenticationError:
            print("❌ SMTP Authentication failed! Check your App Password.")
            return False
        except Exception as e:
            print(f"⚠️ Attempt {attempt}/{MAX_SEND_ATTEMPTS} failed for {to_email}: {e}")
            if attempt == MAX_SEND_ATTEMPTS:
                print(f"❌ Failed to send to {to_email}")
                return False
            time.sleep(SEND_DELAY_SECONDS * attempt)
    return False


# -----------------------------
# CRAWLER
# -----------------------------
def get_keyword_terms(keyword):
    return [w for w in keyword.lower().split() if w not in STOP_WORDS]


def get_must_terms(keyword_terms):
    if not keyword_terms:
        return []
    if len(keyword_terms) <= 2:
        return keyword_terms[:]
    return keyword_terms[-2:]


def is_page_relevant(text, keyword_terms, must_terms):
    text_l = text.lower()
    for t in must_terms:
        if t not in text_l:
            return False
    hits = sum(1 for w in keyword_terms if w in text_l)
    min_hits = max(2, len(keyword_terms) - 1) if len(keyword_terms) >= 2 else 1
    return hits >= min_hits


def is_url_relevant(url, keyword_terms, must_terms):
    u = url.lower()
    for t in must_terms:
        if t not in u:
            return False
    return any(w in u for w in keyword_terms)


def collect_urls(keyword):
    headers = {"User-Agent": "Mozilla/5.0"}
    sources = [("post", "https://html.duckduckgo.com/html/"), ("get", "https://duckduckgo.com/html/")]
    all_candidates = []
    for mode, url in sources:
        try:
            if mode == "post":
                resp = requests.post(url, headers=headers, data={"q": keyword}, timeout=10)
            else:
                resp = requests.get(url, headers=headers, params={"q": keyword}, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            results = soup.select("a.result__a") or soup.select("a.result-link, a[data-testid='result-title-link'], a[href]")
            for r in results:
                href = r.get("href")
                if href and href.startswith("http"):
                    all_candidates.append(href)
            if all_candidates:
                break
        except Exception:
            continue
    return list(dict.fromkeys(all_candidates))[:MAX_START_URLS]


def extract_links(url):
    links = set()
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            full_url = urljoin(url, a["href"])
            if full_url.startswith("http"):
                links.add(full_url)
    except Exception:
        pass
    return links


def process_url(url, keyword_terms, must_terms):
    results = {"links": set()}
    with visited_lock:
        if url in visited_urls:
            return results
        visited_urls.add(url)
    print("Visiting:", url)
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        text = r.text
        if not is_page_relevant(text, keyword_terms, must_terms):
            return results
        emails = re.findall(EMAIL_REGEX, text)
        for email in emails:
            email = clean_email(email)
            if not is_valid_format(email) or is_disposable(email):
                continue
            if DNS_VALIDATE and not dns_validate_email(email):
                continue
            category = classify_email(email)
            score = score_email(email)
            collection.update_one(
                {"email": email},
                {"$set": {"email": email, "type": category, "score": score, "source": url}},
                upsert=True
            )
        links = extract_links(url)
        for link in links:
            if is_url_relevant(link, keyword_terms, must_terms):
                results["links"].add(link)
    except Exception:
        pass
    return results


def crawl_and_extract(start_urls, keyword_terms, must_terms):
    to_visit = set(start_urls)
    for depth in range(MAX_DEPTH):
        print(f"\n--- Crawling level {depth + 1} ---")
        if not to_visit:
            break
        next_level = set()
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(process_url, url, keyword_terms, must_terms) for url in to_visit]
            for future in futures:
                result = future.result()
                next_level.update(result["links"])
        to_visit = next_level


# -----------------------------
# MAIN
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Leadgen crawler + SMTP Sender + Validator")
    parser.add_argument("--keyword", type=str, help="Keyword query (e.g. 'top schools in delhi')")
    parser.add_argument("--keep-mongo", action="store_true", help="Do NOT clear MongoDB before crawling")
    parser.add_argument("--sender-id", type=str, default=SENDER_ID)
    parser.add_argument("--step7-limit", type=int, default=STEP7_LIMIT)
    parser.add_argument("--reserve-quota", action="store_true")
    parser.add_argument("--step8-export", action="store_true")
    parser.add_argument("--step8-csv", type=str, default=None)
    parser.add_argument("--step8-mark-sent", action="store_true")
    parser.add_argument("--step8-only", action="store_true")
    parser.add_argument("--step8-batch-id", type=str, default=None)
    parser.add_argument("--step8-limit", type=int, default=None)
    parser.add_argument("--step8-send", action="store_true", help="Send emails via Gmail SMTP")
    parser.add_argument("--validate-smtp", action="store_true", help="Validate emails using SMTP RCPT TO")
    parser.add_argument("--subject", type=str, default=None, help="Custom email subject")
    parser.add_argument("--body-html", type=str, default=None, help="Path to HTML email template file")

    args = parser.parse_args()

    # SMTP VALIDATION MODE
    if args.validate_smtp:
        print("🚀 Starting SMTP validation on stored emails...")
        cursor = collection.find({"smtp_valid": {"$ne": True}}, {"email": 1}).limit(SMTP_VALIDATE_MAX)
        validated_count = 0
        for doc in cursor:
            email = doc.get("email")
            if smtp_validate_email(email):
                collection.update_one(
                    {"email": email},
                    {"$set": {"smtp_valid": True, "validated_at": _utc_now()}}
                )
                validated_count += 1
            time.sleep(random.uniform(2, 5))
        print(f"SMTP validation completed! {validated_count} emails marked as valid.")
        return

    # NORMAL MODE
    keyword = (args.keyword or input("Enter keyword: ")).strip()
    if not keyword:
        print("Keyword is required.")
        return

    keyword_terms = get_keyword_terms(keyword)
    must_terms = get_must_terms(keyword_terms)
    print("Keyword terms:", keyword_terms)
    print("Must terms:", must_terms)

    if not args.keep_mongo:
        collection.delete_many({})
        print("Old data cleared from MongoDB")

    print("\nSearching for start URLs...")
    start_urls = collect_urls(keyword)
    print(f"Found {len(start_urls)} start URLs")

    crawl_and_extract(start_urls, keyword_terms, must_terms)
    print("\nCrawling completed. Emails stored in MongoDB.")

    batch_id = f"batch_{int(_utc_now().timestamp())}"
    eligible_emails, rejected = get_quota_eligible_emails(
        limit=args.step7_limit,
        sender_id=args.sender_id,
        reserve=bool(args.reserve_quota),
        batch_id=batch_id
    )

    daily_sent = get_daily_sent_count(args.sender_id)
    print("\n=== Step 7 Summary ===")
    print(f"Daily sent: {daily_sent}/{DAILY_SEND_QUOTA}")
    print(f"Quota eligible emails: {len(eligible_emails)}")
    print("Rejected:", rejected)
    print(f"Batch ID: {batch_id}")

    send_log_collection.insert_one({
        "sender_id": args.sender_id,
        "batch_id": batch_id,
        "keyword": keyword,
        "created_at": _utc_now(),
        "stage": "step7_quota_preview",
        "emails_count": len(eligible_emails),
        "rejected": rejected
    })

    if args.step8_export:
        limit = args.step8_limit or len(eligible_emails)
        csv_path = args.step8_csv or f"step8_export_{batch_id}.csv"
        export_step8_csv(eligible_emails[:limit], batch_id, args.sender_id, csv_path)

    if args.step8_mark_sent:
        mark_step8_as_sent(batch_id, args.sender_id)

    if args.step8_send:
        if not eligible_emails:
            print("No eligible emails to send.")
            return

        # AI ranks emails best to worst before sending
        print("\n🤖 AI scoring and ranking leads...")
        eligible_emails = ai_score_leads(eligible_emails)

        print(f"\n🚀 Starting SMTP sending for {len(eligible_emails)} emails...")

        subject = args.subject or DEFAULT_SUBJECT

        if args.body_html and os.path.exists(args.body_html):
            with open(args.body_html, "r", encoding="utf-8") as f:
                body_html = f.read()
        else:
            body_html = f"""
            <h2>Hello,</h2>
            <p>I came across your contact while researching <strong>{keyword}</strong>.</p>
            <p>Would love to connect and explore possible collaboration.</p>
            <p>Best regards,<br>{DEFAULT_FROM_NAME}</p>
            """

        sent_count = 0
        for email in eligible_emails:
            success = send_single_email(
                to_email=email,
                subject=subject,
                body_html=body_html,
                batch_id=batch_id
            )
            if success:
                sent_count += 1
            time.sleep(SEND_DELAY_SECONDS)

        print(f"\n🎉 SMTP Sending Completed!")
        print(f"Successfully sent: {sent_count}/{len(eligible_emails)} emails")


if __name__ == "__main__":
    main()
