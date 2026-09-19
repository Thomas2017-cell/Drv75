#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot d'alerte Deriv (indices synthétiques) — AUCUN ordre n'est passé.

1. Récupère les bougies daily et H4 via l'API WebSocket publique de Deriv.
2. Détecte automatiquement les zones daily :
   - grandes zones : regroupements de pivots (supports / résistances)
   - zones intermédiaires : order blocks (dernière bougie opposée avant une impulsion)
3. À chaque clôture H4, si la bougie est un doji (corps <= 40 % du range, y compris
   doji renversé), un marteau ou un marteau renversé ET touche une zone daily,
   envoie un email d'alerte.

Installation :
    pip install websocket-client

Utilisation :
    python deriv_alert_bot.py --zones        # affiche les zones détectées (sans email)
    python deriv_alert_bot.py --test-email   # teste l'envoi d'email
    python deriv_alert_bot.py --dry-run      # une vérification, affiche l'alerte au lieu de l'envoyer
    python deriv_alert_bot.py --once         # une vérification (pour cron / planificateur cloud)
    python deriv_alert_bot.py                # tourne en continu, vérifie à chaque clôture H4

Variables d'environnement pour l'email (Gmail : utiliser un "mot de passe d'application") :
    SMTP_USER       adresse Gmail qui envoie
    SMTP_PASSWORD   mot de passe d'application
    ALERT_TO        destinataire (défaut : dixon.thomas93@gmail.com)
    SMTP_HOST / SMTP_PORT   (défaut : smtp.gmail.com / 465)
"""
import argparse
import json
import logging
import os
import smtplib
import ssl
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage

# ============================== CONFIG ==============================
APP_ID = os.getenv("DERIV_APP_ID", "1089")
WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

SYMBOLS = {
    "R_75": "Volatility 75",
    "1HZ100V": "Volatility 100 (1s)",
    "R_25": "Volatility 25",
}

D1, H4 = 86400, 14400
DAILY_COUNT = 500          # nombre de bougies daily analysées
H4_COUNT = 30              # nombre de bougies H4 récupérées

# --- Détection des zones daily ---
ATR_PERIOD = 14
PIVOT_N = 5                # un pivot = extrême sur N bougies de chaque côté
CLUSTER_ATR = 0.6          # tolérance de regroupement des pivots (en ATR daily)
MAJOR_MIN_TOUCHES = 2      # pivots minimum pour une "grande zone"
ZONE_PAD_ATR = 0.1         # marge ajoutée autour des grandes zones (en ATR)
OB_IMPULSE_ATR = 1.0       # impulsion minimale après un order block (en ATR)
OB_LOOKAHEAD = 3           # nombre de bougies pour mesurer l'impulsion
NEAR_ATR = 1.5             # une zone est "proche" si à moins de X ATR du prix (affichage --zones)

# --- Signaux H4 ---
DOJI_BODY_MAX = 0.40       # corps <= 40 % du range total
HAMMER_BODY_MAX = 0.35     # corps max d'un marteau
HAMMER_SHADOW_MIN = 0.50   # mèche dominante >= 50 % du range et >= 2x le corps
HAMMER_OPP_SHADOW_MAX = 0.25

STATE_FILE = os.getenv("STATE_FILE", "alerts_sent.json")

log = logging.getLogger("deriv-bot")


# ============================== MODÈLES ==============================
@dataclass
class Candle:
    epoch: int
    o: float
    h: float
    l: float
    c: float


@dataclass
class Zone:
    low: float
    high: float
    kind: str              # "major" ou "ob"
    side: str = ""         # OB : "support" / "résistance" ; vide pour les grandes zones
    touches: int = 0
    formed: int = 0        # epoch de formation (OB)

    def distance(self, price):
        if self.low <= price <= self.high:
            return 0.0
        return min(abs(price - self.low), abs(price - self.high))

    def label(self):
        if self.kind == "major":
            return f"Grande zone ({self.touches} pivots)"
        sens = "haussier" if self.side == "support" else "baissier"
        return f"Order block {sens} du {fmt_day(self.formed)}"


def fmt_ts(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_day(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


# ============================== DONNÉES DERIV ==============================
def fetch_candles(symbol, granularity, count, retries=3):
    """Retourne uniquement les bougies CLÔTURÉES."""
    import websocket  # pip install websocket-client

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            ws = websocket.create_connection(WS_URL, timeout=20)
            try:
                ws.send(json.dumps({
                    "ticks_history": symbol,
                    "adjust_start_time": 1,
                    "count": count,
                    "end": "latest",
                    "granularity": granularity,
                    "style": "candles",
                }))
                data = json.loads(ws.recv())
            finally:
                ws.close()
            if "error" in data:
                raise RuntimeError(data["error"].get("message", str(data["error"])))
            now = time.time()
            candles = [
                Candle(int(c["epoch"]), float(c["open"]), float(c["high"]),
                       float(c["low"]), float(c["close"]))
                for c in data.get("candles", [])
            ]
            return [c for c in candles if c.epoch + granularity <= now]
        except Exception as e:  # noqa: BLE001
            last_err = e
            log.warning("%s/%s tentative %d/%d échouée : %s", symbol, granularity, attempt, retries, e)
            time.sleep(2 * attempt)
    raise RuntimeError(f"Impossible de récupérer {symbol}/{granularity} : {last_err}")


# ============================== ZONES DAILY ==============================
def atr(candles, period=ATR_PERIOD):
    trs = []
    for i in range(1, len(candles)):
        p = candles[i - 1].c
        c = candles[i]
        trs.append(max(c.h - c.l, abs(c.h - p), abs(c.l - p)))
    last = trs[-period:]
    return sum(last) / len(last)


def find_pivots(candles, n=PIVOT_N):
    levels = []
    for i in range(n, len(candles) - n):
        window = candles[i - n:i + n + 1]
        if candles[i].h == max(x.h for x in window):
            levels.append(candles[i].h)
        if candles[i].l == min(x.l for x in window):
            levels.append(candles[i].l)
    return levels


def major_zones(candles, a):
    """Grandes zones : pivots (hauts et bas) regroupés par proximité."""
    levels = sorted(find_pivots(candles))
    tol = CLUSTER_ATR * a
    clusters = []
    for lv in levels:
        if clusters and lv - clusters[-1][0] <= tol:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    pad = ZONE_PAD_ATR * a
    return [
        Zone(min(cl) - pad, max(cl) + pad, "major", touches=len(cl))
        for cl in clusters if len(cl) >= MAJOR_MIN_TOUCHES
    ]


def order_blocks(candles, a):
    """Zones intermédiaires : dernière bougie opposée avant une impulsion.
    Un OB est supprimé si une clôture daily le traverse ensuite."""
    obs = []
    for i in range(len(candles) - 1):
        c = candles[i]
        nxt = candles[i + 1:i + 1 + OB_LOOKAHEAD]
        after = candles[i + 1:]
        first = nxt[0]
        # OB haussier : bougie baissière, suivie d'une bougie non baissière + impulsion haussière
        if c.c < c.o and first.c >= first.o \
                and max(x.c for x in nxt) - c.h >= OB_IMPULSE_ATR * a:
            if not any(x.c < c.l for x in after):
                obs.append(Zone(c.l, c.h, "ob", "support", formed=c.epoch))
        # OB baissier : bougie haussière, suivie d'une bougie non haussière + impulsion baissière
        elif c.c > c.o and first.c <= first.o \
                and c.l - min(x.c for x in nxt) >= OB_IMPULSE_ATR * a:
            if not any(x.c > c.h for x in after):
                obs.append(Zone(c.l, c.h, "ob", "résistance", formed=c.epoch))
    return obs


def detect_zones(daily):
    a = atr(daily)
    return major_zones(daily, a) + order_blocks(daily, a), a


# ============================== PATTERNS H4 ==============================
def classify(c):
    """Retourne le nom du pattern ou None."""
    rng = c.h - c.l
    if rng <= 0:
        return None
    body = abs(c.c - c.o)
    upper = c.h - max(c.o, c.c)
    lower = min(c.o, c.c) - c.l
    body_r, upper_r, lower_r = body / rng, upper / rng, lower / rng

    if body_r <= HAMMER_BODY_MAX and lower_r >= HAMMER_SHADOW_MIN \
            and lower >= 2 * body and upper_r <= HAMMER_OPP_SHADOW_MAX:
        return "Marteau"
    if body_r <= HAMMER_BODY_MAX and upper_r >= HAMMER_SHADOW_MIN \
            and upper >= 2 * body and lower_r <= HAMMER_OPP_SHADOW_MAX:
        return "Marteau renversé"
    if body_r <= DOJI_BODY_MAX:
        if upper_r - lower_r > 0.2:
            return f"Doji renversé (corps {body_r:.0%})"
        return f"Doji (corps {body_r:.0%})"
    return None


def find_signal(daily, h4):
    """Analyse la dernière bougie H4 clôturée. Retourne un dict ou None."""
    zones, a = detect_zones(daily)
    last = h4[-1]
    pattern = classify(last)
    if not pattern:
        return None
    hits = []
    for z in zones:
        if last.l <= z.high and last.h >= z.low:
            if z.kind == "ob":
                side = z.side
            else:
                side = "support" if last.c >= (z.low + z.high) / 2 else "résistance"
            hits.append((z, side))
    if not hits:
        return None
    return {"candle": last, "pattern": pattern, "hits": hits, "atr": a}


# ============================== EMAIL ==============================
def build_email(name, symbol, sig):
    c = sig["candle"]
    close_ts = fmt_ts(c.epoch + H4)
    subject = f"[Deriv] {name} — {sig['pattern']} H4 sur zone daily"
    lines = [
        f"Indice : {name} ({symbol})",
        f"Bougie H4 clôturée à {close_ts}",
        f"Pattern : {sig['pattern']}",
        f"O {c.o:.4f} | H {c.h:.4f} | L {c.l:.4f} | C {c.c:.4f}",
        "",
        "Zone(s) daily touchée(s) :",
    ]
    for z, side in sig["hits"]:
        bias = "achat" if side == "support" else "vente"
        lines.append(
            f" - {z.label()} : {z.low:.4f} → {z.high:.4f} "
            f"(contexte {side}, biais {bias})"
        )
    lines += [
        "",
        "Alerte indicative uniquement : aucun ordre n'a été passé.",
        "Vérifie le graphique avant de prendre position.",
    ]
    return subject, "\n".join(lines)


def send_email(subject, body):
    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASSWORD")
    to = os.getenv("ALERT_TO", "dixon.thomas93@gmail.com")
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "465"))
    if not user or not pwd:
        raise RuntimeError("SMTP_USER et SMTP_PASSWORD doivent être définis.")
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, user, to
    msg.set_content(body)
    with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context(), timeout=30) as s:
        s.login(user, pwd)
        s.send_message(msg)
    log.info("Email envoyé à %s : %s", to, subject)


# ============================== ÉTAT (anti-doublons) ==============================
def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(state)[-500:], f)


# ============================== EXÉCUTION ==============================
def run_once(dry_run=False):
    state = load_state()
    for sym, name in SYMBOLS.items():
        try:
            daily = fetch_candles(sym, D1, DAILY_COUNT)
            h4 = fetch_candles(sym, H4, H4_COUNT)
            if len(daily) < 60 or not h4:
                log.warning("%s : pas assez de données", name)
                continue
            sig = find_signal(daily, h4)
            if not sig:
                log.info("%s : pas de signal (dernière H4 clôturée %s)", name, fmt_ts(h4[-1].epoch))
                continue
            key = f"{sym}|{sig['candle'].epoch}"
            if key in state:
                log.info("%s : signal déjà envoyé", name)
                continue
            subject, body = build_email(name, sym, sig)
            if dry_run:
                print(f"\n=== {subject} ===\n{body}\n")
            else:
                send_email(subject, body)
                state.add(key)
                save_state(state)
        except Exception as e:  # noqa: BLE001
            log.error("%s : %s", name, e)


def show_zones():
    for sym, name in SYMBOLS.items():
        daily = fetch_candles(sym, D1, DAILY_COUNT)
        h4 = fetch_candles(sym, H4, H4_COUNT)
        price = h4[-1].c
        zones, a = detect_zones(daily)
        near = sorted((z for z in zones if z.distance(price) <= NEAR_ATR * a),
                      key=lambda z: z.distance(price))
        print(f"\n=== {name} ({sym}) — prix {price:.4f} — ATR daily {a:.4f} ===")
        if not near:
            print("  Aucune zone proche du prix.")
        for z in near:
            print(f"  {z.label():40s} {z.low:.4f} → {z.high:.4f}  (distance {z.distance(price):.4f})")


def seconds_until_next_h4(delay=20):
    now = time.time()
    return (int(now) // H4 + 1) * H4 + delay - now


def main():
    p = argparse.ArgumentParser(description="Bot d'alerte Deriv (zones daily + signaux H4)")
    p.add_argument("--once", action="store_true", help="une seule vérification puis quitte")
    p.add_argument("--dry-run", action="store_true", help="affiche l'alerte au lieu de l'envoyer")
    p.add_argument("--zones", action="store_true", help="affiche les zones proches du prix")
    p.add_argument("--test-email", action="store_true", help="envoie un email de test")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.test_email:
        send_email("[Deriv] Test du bot d'alerte", "Si tu lis ceci, l'envoi d'email fonctionne.")
        return
    if args.zones:
        show_zones()
        return
    if args.once or args.dry_run:
        run_once(dry_run=args.dry_run)
        return

    log.info("Bot démarré. Vérification à chaque clôture H4 (UTC 00/04/08/12/16/20).")
    while True:
        run_once()
        wait = seconds_until_next_h4()
        log.info("Prochaine vérification dans %d min", wait // 60)
        time.sleep(wait)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
