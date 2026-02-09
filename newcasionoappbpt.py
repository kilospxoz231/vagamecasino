# ============================================================
# CASINO BOT — FULL BACKEND
# app.py — Часть 1/12: Импорты, конфиг, БД, хелперы
# ============================================================

import logging
import mysql.connector
import hashlib
import time
import json
import os
import re
import uuid
import random
import math
import threading
import hmac
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
import socketio
import eventlet
import eventlet.wsgi
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.constants import ParseMode

# ================================================================
# CONFIG
# ================================================================
TOKEN = "8384803283:AAEilCSeQizPNBPN7VONKVZczaSmttrK-L8"
ADMIN_IDS = [7486170742]
BOT_USERNAME = "vigame_run_bot"
SITE_URL = "https://vigame1x2.ru/newcasino/"
CRYPTOBOT_TOKEN = "528416:AAAfAsJnFTI7vxSIdXEp2aWvDY9GWX3geER"
COINS_PER_USD = 100  # 100 монет = $1

# Security whitelists for admin SQL queries
ALLOWED_GAME_SETTINGS = {'mines', 'cube', 'x50', 'cases_game', 'coinflip', 'slots'}
ALLOWED_FIN_SETTINGS = {'deposits_enabled', 'withdrawals_enabled'}

DB_CONFIG = {
    'host': 'mysql18.hostland.ru',
    'user': 'host1884970',
    'password': 'XsQFpVJba9',
    'database': 'host1884970_casino',
    'charset': 'utf8mb4',
    'autocommit': False,
    'pool_name': 'casino_pool',
    'pool_size': 10,
}

# ================================================================
# LOGGING
# ================================================================
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger('casino')

# ================================================================
# FLASK + SOCKET.IO
# ================================================================
flask_app = Flask(__name__)
CORS(flask_app, resources={r"/*": {"origins": "*"}})

sio = socketio.Server(
    async_mode='eventlet',
    cors_allowed_origins='*',
    ping_timeout=60,
    ping_interval=25,
    max_http_buffer_size=1e6,
)

app = socketio.WSGIApp(sio, flask_app)

# ================================================================
# LOCKS (Race Condition Protection)
# ================================================================
_locks = {}
_locks_lock = threading.Lock()


def get_lock(name):
    with _locks_lock:
        if name not in _locks:
            _locks[name] = threading.Lock()
        return _locks[name]


def with_lock(lock_names, func):
    locks = [get_lock(n) for n in lock_names]
    for lock in locks:
        lock.acquire()
    try:
        return func()
    finally:
        for lock in reversed(locks):
            lock.release()


# ================================================================
# DATABASE
# ================================================================
def get_db():
    return mysql.connector.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset=DB_CONFIG.get('charset', 'utf8mb4'),
    )


def query_one(sql, params=None):
    conn = get_db()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or [])
        result = cursor.fetchone()
        cursor.close()
        return result
    finally:
        conn.close()


def query_all(sql, params=None):
    conn = get_db()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or [])
        results = cursor.fetchall()
        cursor.close()
        return results
    finally:
        conn.close()


def query_exec(sql, params=None):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        last_id = cursor.lastrowid
        conn.commit()
        cursor.close()
        return last_id
    finally:
        conn.close()


def query_exec_many(queries):
    conn = get_db()
    cursor = conn.cursor()
    try:
        for sql, params in queries:
            cursor.execute(sql, params or [])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


# ================================================================
# INIT DATABASE — ALL TABLES
# ================================================================
def init_db():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT UNIQUE NOT NULL,
            name VARCHAR(100) DEFAULT '',
            avatar VARCHAR(300) DEFAULT '',
            balance INT DEFAULT 0,
            deposit INT DEFAULT 0,
            total_wagered INT DEFAULT 0,
            wager INT DEFAULT 0,
            role INT DEFAULT 0,
            ref_by BIGINT DEFAULT 0,
            refs INT DEFAULT 0,
            ref_earned INT DEFAULT 0,
            ref_available INT DEFAULT 0,
            token VARCHAR(100) DEFAULT '',
            launched_mini_app TINYINT DEFAULT 0,
            is_banned TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user_id (user_id),
            INDEX idx_token (token),
            INDEX idx_ref_by (ref_by)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mines_bets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            bet INT NOT NULL DEFAULT 0,
            mines INT NOT NULL DEFAULT 3,
            tiles TEXT DEFAULT NULL,
            lose_tiles TEXT DEFAULT NULL,
            steps INT DEFAULT 0,
            current_coeff DECIMAL(10,2) DEFAULT 1.00,
            currentAmount DECIMAL(15,2) DEFAULT 0,
            result DECIMAL(15,2) DEFAULT 0,
            hash VARCHAR(200) DEFAULT '',
            salt VARCHAR(50) DEFAULT '',
            status TINYINT DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user_status (user_id, status),
            INDEX idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cube_games (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            bet INT NOT NULL DEFAULT 0,
            mode VARCHAR(20) DEFAULT 'over',
            number INT DEFAULT NULL,
            cube_result INT DEFAULT 0,
            win_amount INT DEFAULT 0,
            coeff DECIMAL(10,2) DEFAULT 0,
            hash VARCHAR(200) DEFAULT '',
            salt VARCHAR(50) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS x50_bets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_id INT NOT NULL DEFAULT 0,
            user_id INT NOT NULL DEFAULT 0,
            bet INT NOT NULL DEFAULT 0,
            coeff VARCHAR(10) DEFAULT '0',
            coeffWon VARCHAR(10) DEFAULT '0',
            result INT DEFAULT 0,
            hash VARCHAR(200) DEFAULT '',
            status TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_game (game_id),
            INDEX idx_user (user_id),
            INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS history_x50 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            coeff VARCHAR(10) DEFAULT '0',
            hash VARCHAR(200) DEFAULT '',
            salt VARCHAR(50) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cases_bets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            case_type VARCHAR(50) DEFAULT 'standard',
            bet INT NOT NULL DEFAULT 0,
            win_amount INT DEFAULT 0,
            win_item VARCHAR(100) DEFAULT '',
            coeff DECIMAL(10,2) DEFAULT 0,
            hash VARCHAR(200) DEFAULT '',
            salt VARCHAR(50) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS coinflip_bets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            bet INT NOT NULL DEFAULT 0,
            side VARCHAR(10) DEFAULT 'heads',
            result VARCHAR(10) DEFAULT '',
            win_amount INT DEFAULT 0,
            hash VARCHAR(200) DEFAULT '',
            salt VARCHAR(50) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS slots_bets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            bet INT NOT NULL DEFAULT 0,
            result_symbols VARCHAR(50) DEFAULT '',
            win_amount INT DEFAULT 0,
            coeff DECIMAL(10,2) DEFAULT 0,
            hash VARCHAR(200) DEFAULT '',
            salt VARCHAR(50) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            tg_id BIGINT NOT NULL DEFAULT 0,
            amount_usd DECIMAL(15,2) NOT NULL DEFAULT 0,
            amount_coins INT NOT NULL DEFAULT 0,
            invoice_id VARCHAR(100) DEFAULT '',
            status TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id),
            INDEX idx_tg (tg_id),
            INDEX idx_invoice (invoice_id),
            INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # Замените существующий CREATE TABLE IF NOT EXISTS withdrawals (...) на этот блок
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            tg_id BIGINT NOT NULL DEFAULT 0,
            user_name VARCHAR(100) DEFAULT '',
            amount INT NOT NULL DEFAULT 0,
            amount_in_usdt DECIMAL(15,2) NOT NULL DEFAULT 0,
            system_type TINYINT DEFAULT 1,
            status TINYINT DEFAULT 0,
            check_link VARCHAR(300) DEFAULT '',
            admin_comment TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP NULL DEFAULT NULL,
            INDEX idx_user (user_id),
            INDEX idx_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promocodes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(50) UNIQUE NOT NULL,
            reward INT NOT NULL DEFAULT 0,
            wager_multiplier DECIMAL(5,2) DEFAULT 1.00,
            max_uses INT DEFAULT 1,
            current_uses INT DEFAULT 0,
            is_active TINYINT DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_code (code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promo_uses (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            promo_id INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_user_promo (user_id, promo_id),
            INDEX idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ref_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            referrer_id INT NOT NULL,
            referred_id INT NOT NULL,
            deposit_amount INT NOT NULL DEFAULT 0,
            bonus_amount INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_referrer (referrer_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            type VARCHAR(30) NOT NULL,
            amount INT NOT NULL DEFAULT 0,
            balance_after INT DEFAULT 0,
            description VARCHAR(300) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user (user_id),
            INDEX idx_type (type),
            INDEX idx_date (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            id INT PRIMARY KEY DEFAULT 1,
            `mines` TINYINT DEFAULT 1,
            `cube` TINYINT DEFAULT 1,
            `x50` TINYINT DEFAULT 1,
            `cases_game` TINYINT DEFAULT 1,
            `coinflip` TINYINT DEFAULT 1,
            `slots` TINYINT DEFAULT 1,
            `deposits_enabled` TINYINT DEFAULT 1,
            `withdrawals_enabled` TINYINT DEFAULT 1,
            `min_deposit_usd` DECIMAL(10,2) DEFAULT 1.00,
            `max_deposit_usd` DECIMAL(10,2) DEFAULT 10000.00,
            `min_withdraw_coins` INT DEFAULT 500,
            `max_withdraw_coins` INT DEFAULT 1000000,
            `coins_per_usd` INT DEFAULT 100,
            `ref_percent` DECIMAL(5,2) DEFAULT 10.00,
            `cryptobot_token` VARCHAR(200) DEFAULT '',
            `site_url` VARCHAR(200) DEFAULT '',
            `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS banks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game VARCHAR(30) UNIQUE NOT NULL,
            bank INT DEFAULT 0,
            min_bank INT DEFAULT -50000,
            max_bank INT DEFAULT 500000,
            INDEX idx_game (game)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    cursor.execute("SELECT COUNT(*) as c FROM settings")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO settings (id, `mines`, `cube`, `x50`, `cases_game`, `coinflip`, `slots`,
                `deposits_enabled`, `withdrawals_enabled`, `min_deposit_usd`, `max_deposit_usd`,
                `min_withdraw_coins`, `max_withdraw_coins`, `coins_per_usd`, `ref_percent`)
            VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1.00, 10000.00, 500, 1000000, 100, 10.00)
        """)

    cursor.execute("SELECT COUNT(*) as c FROM banks")
    if cursor.fetchone()[0] == 0:
        for g, mn, mx in [('mines', -50000, 500000), ('cube', -50000, 500000),
                           ('x50', -100000, 1000000), ('cases', -30000, 300000),
                           ('coinflip', -30000, 300000), ('slots', -30000, 300000)]:
            cursor.execute("INSERT INTO banks (game, bank, min_bank, max_bank) VALUES (%s, 0, %s, %s)", [g, mn, mx])

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Database initialized — all tables ready")


# ================================================================
# HELPER FUNCTIONS
# ================================================================
def get_user_by_id(internal_id):
    return query_one("SELECT * FROM users WHERE id = %s LIMIT 1", [internal_id])


def get_user_by_tg(tg_id):
    return query_one("SELECT * FROM users WHERE user_id = %s LIMIT 1", [tg_id])


def get_user_by_token(token):
    if not token:
        return None
    return query_one("SELECT * FROM users WHERE token = %s LIMIT 1", [token])


def get_settings():
    return query_one("SELECT * FROM settings WHERE id = 1")


def get_bank(game):
    return query_one("SELECT * FROM banks WHERE game = %s", [game])


def update_bank(game, profit_for_house):
    bank = get_bank(game)
    if not bank:
        return 0
    new_val = bank['bank'] + profit_for_house
    if new_val > bank['max_bank']:
        new_val = bank['max_bank']
    if new_val < bank['min_bank']:
        new_val = bank['min_bank']
    query_exec("UPDATE banks SET bank = %s WHERE game = %s", [new_val, game])
    return new_val


def add_transaction(user_id, tx_type, amount, balance_after, description=''):
    query_exec("""
        INSERT INTO transactions (user_id, type, amount, balance_after, description)
        VALUES (%s, %s, %s, %s, %s)
    """, [user_id, tx_type, amount, balance_after, description])


def get_or_create_user(tg_id, name='', avatar='', ref_by=0):
    user = get_user_by_tg(tg_id)
    if user:
        return user
    token = str(uuid.uuid4())
    query_exec("""
        INSERT INTO users (user_id, name, avatar, balance, deposit, ref_by, token)
        VALUES (%s, %s, %s, 0, 0, %s, %s)
    """, [tg_id, name, avatar, ref_by, token])
    if ref_by and ref_by != tg_id:
        ref_user = get_user_by_tg(ref_by)
        if ref_user:
            query_exec("UPDATE users SET refs = refs + 1 WHERE user_id = %s", [ref_by])
    return get_user_by_tg(tg_id)


def generate_hash(data_string):
    salt = generate_salt()
    h = hashlib.md5((data_string + "|" + salt).encode()).hexdigest()
    return h, salt


def generate_salt(length=12):
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(random.choice(chars) for _ in range(length))


def md5_hash(s):
    return hashlib.md5(s.encode()).hexdigest()


def get_random_string(length=30):
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(random.choice(chars) for _ in range(length))


def rounded_int(val):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def html_entities(s):
    if not s:
        return ''
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


# ================================================================
# MINES COEFFICIENT TABLE
# ================================================================
def get_mines_coefficients(mines_count):
    table = {}
    total_tiles = 25
    safe_tiles = total_tiles - mines_count
    house_edge = 0.03
    cumulative = 1.0
    for step in range(1, safe_tiles + 1):
        prob = (safe_tiles - (step - 1)) / (total_tiles - (step - 1))
        cumulative *= prob
        fair_coeff = 1.0 / cumulative
        coeff = round(fair_coeff * (1 - house_edge), 2)
        if coeff < 1.01:
            coeff = 1.01
        table[step] = coeff
    return table


# ================================================================
# X50 GAME STATE (in memory)
# ================================================================
x50_state = {
    'stage': 'WAIT',
    'bets': [],
    'users_bets_count': {},
    'users_bets_amount': {},
    'total': 0,
    'current_game_id': 1,
    'timer': None,
    'time_seconds': 15,
}

X50_ROLLS_HISTORY = []

# ================================================================
# CONNECTED USERS
# ================================================================
connected_users = {}
sid_to_user = {}


# === КОНЕЦ ЧАСТИ 1/12 ===
# Следующая часть: Socket.IO события + авторизация
# ============================================================
# app.py — Часть 2/12: Socket.IO события, авторизация, баланс
# ============================================================

# ================================================================
# AVATAR SYSTEM
# ================================================================
AVATAR_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'avatars')


def ensure_avatar_dir():
    if not os.path.exists(AVATAR_DIR):
        os.makedirs(AVATAR_DIR, exist_ok=True)


def download_avatar(bot_token, file_id, user_id):
    try:
        ensure_avatar_dir()
        r = requests.get(f'https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}', timeout=10)
        data = r.json()
        if not data.get('ok'):
            return ''
        file_path = data['result']['file_path']
        ext = os.path.splitext(file_path)[1] or '.jpg'
        filename = f'{user_id}{ext}'
        full_path = os.path.join(AVATAR_DIR, filename)
        img_r = requests.get(f'https://api.telegram.org/file/bot{bot_token}/{file_path}', timeout=15)
        with open(full_path, 'wb') as f:
            f.write(img_r.content)
        cleanup_old_avatars(user_id, filename)
        return f'avatars/{filename}'
    except Exception as e:
        logger.warning(f'Avatar download error: {e}')
        return ''


def cleanup_old_avatars(user_id, keep_filename):
    try:
        for f in os.listdir(AVATAR_DIR):
            if f.startswith(f'{user_id}.') and f != keep_filename:
                os.remove(os.path.join(AVATAR_DIR, f))
    except Exception:
        pass


def generate_default_avatar_path(user_id, name):
    filename = f'{user_id}.svg'
    colors = ['#3B82F6', '#EF4444', '#22C55E', '#8B5CF6', '#F59E0B', '#06B6D4']
    color = colors[user_id % len(colors)]
    letter = (name[0] if name else '?').upper()
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128">
<rect width="128" height="128" fill="{color}" rx="64"/>
<text x="64" y="72" font-size="64" font-family="Arial,sans-serif" fill="white"
text-anchor="middle" font-weight="bold">{letter}</text></svg>'''
    ensure_avatar_dir()
    full_path = os.path.join(AVATAR_DIR, filename)
    with open(full_path, 'w') as f:
        f.write(svg)
    return f'avatars/{filename}'


# ================================================================
# SOCKET.IO — CONNECTION / DISCONNECTION
# ================================================================
@sio.event
def connect(sid, environ):
    logger.info(f'Socket connected: {sid}')


@sio.event
def disconnect(sid):
    user_id = sid_to_user.pop(sid, None)
    if user_id and user_id in connected_users:
        if connected_users[user_id] == sid:
            del connected_users[user_id]
    logger.info(f'Socket disconnected: {sid} (user: {user_id})')


# ================================================================
# SOCKET.IO — AUTH
# ================================================================
@sio.on('auth')
def handle_auth(sid, data):
    try:
        token = data.get('token', '')
        tg_id = data.get('user_id', 0)

        if not token:
            sio.emit('message', {
                'type': 'alert',
                'type_alert': 'error',
                'alert_message': 'Токен не указан'
            }, room=sid)
            return

        user = get_user_by_token(token)
        if not user:
            sio.emit('message', {
                'type': 'alert',
                'type_alert': 'error',
                'alert_message': 'Неверный токен'
            }, room=sid)
            return

        if user.get('is_banned'):
            sio.emit('message', {
                'type': 'alert',
                'type_alert': 'error',
                'alert_message': 'Аккаунт заблокирован'
            }, room=sid)
            return

        internal_id = user['id']
        connected_users[internal_id] = sid
        sid_to_user[sid] = internal_id

        if user['launched_mini_app'] == 0:
            query_exec("UPDATE users SET launched_mini_app = 1 WHERE id = %s", [internal_id])
            if user['ref_by'] and user['ref_by'] != user['user_id']:
                ref_user = get_user_by_tg(user['ref_by'])
                if ref_user:
                    bonus = 50
                    query_exec("UPDATE users SET balance = balance + %s WHERE id = %s", [bonus, internal_id])
                    user['balance'] = user['balance'] + bonus
                    add_transaction(internal_id, 'ref_bonus', bonus, user['balance'], 'Бонус за реферала')

        active_mines = query_one(
            "SELECT * FROM mines_bets WHERE user_id = %s AND status = 1 LIMIT 1",
            [internal_id]
        )

        x50_history = query_all(
            "SELECT id, coeff FROM history_x50 ORDER BY id DESC LIMIT 15"
        )


        level_info = get_user_level_info(internal_id)
        cashback = get_cashback_amount(internal_id)
        event = get_active_event()
        welcome_data = {
            'type': 'welcome',
            'userId': internal_id,
            'tg_id': user['user_id'],
            'balance': int(user['balance']),
            'name': user['name'],
            'avatar': user['avatar'],
            'role': user['role'],
            'refs': user['refs'],
            'ref_earned': int(user['ref_earned']),
            'ref_available': int(user['ref_available']),
            'wager': int(user['wager']),
            'time': int(time.time()),
            # === NEW FIELDS ===
            'level': level_info['level'],
            'exp': level_info['exp'],
            'exp_needed': level_info['exp_needed'],
            'cashback': cashback,
            'event': event,
            # === END NEW ===
            'x50_history': [{'id': r['id'], 'coeff': r['coeff']} for r in x50_history] if x50_history else [],
            'x50_stage': x50_state['stage'],
            'x50_bets': x50_state['bets'],
            'x50_time': x50_state['time_seconds'],
        }

        if active_mines:
            opened_tiles = active_mines['tiles'].split('|') if active_mines['tiles'] else []
            welcome_data['activeMines'] = {
                'game_id': active_mines['id'],
                'bet': int(active_mines['bet']),
                'mines': active_mines['mines'],
                'steps': active_mines['steps'],
                'current_coeff': float(active_mines['current_coeff']),
                'currentAmount': int(active_mines['currentAmount']),
                'opened_tiles': opened_tiles,
            }

        sio.emit('message', welcome_data, room=sid)
        logger.info(f'User {internal_id} ({user["name"]}) authenticated')

    except Exception as e:
        logger.error(f'Auth error: {e}')
        sio.emit('message', {
            'type': 'alert',
            'type_alert': 'error',
            'alert_message': 'Ошибка авторизации'
        }, room=sid)


# ================================================================
# SOCKET.IO — MAIN MESSAGE ROUTER
# ================================================================
@sio.on('message')
def handle_message(sid, data):
    user_id = sid_to_user.get(sid)
    if not user_id:
        sio.emit('message', {
            'type': 'alert',
            'type_alert': 'error',
            'alert_message': 'Сначала авторизуйте��ь'
        }, room=sid)
        return

    msg_type = data.get('type', '')

    try:
        if msg_type == 'createMines':
            with_lock(['user_' + str(user_id), 'mines'], lambda: handle_create_mines(data, user_id, sid))

        elif msg_type == 'playMines':
            with_lock(['user_' + str(user_id), 'mines'], lambda: handle_play_mines(data, user_id, sid))

        elif msg_type == 'takeMines':
            with_lock(['user_' + str(user_id), 'mines'], lambda: handle_take_mines(user_id, sid))

        elif msg_type == 'minesCheck':
            handle_mines_check(data, user_id, sid)

        elif msg_type == 'betCube':
            with_lock(['user_' + str(user_id), 'cube'], lambda: handle_bet_cube(data, user_id, sid))

        elif msg_type == 'diceCheck':
            handle_dice_check(data, user_id, sid)

        elif msg_type == 'joinx50':
            with_lock(['user_' + str(user_id), 'x50'], lambda: handle_join_x50(data, user_id, sid))

        elif msg_type == 'x50Check':
            handle_x50_check(data, user_id, sid)

        elif msg_type == 'openCase':
            with_lock(['user_' + str(user_id), 'cases'], lambda: handle_open_case(data, user_id, sid))

        elif msg_type == 'betCoinflip':
            with_lock(['user_' + str(user_id), 'coinflip'], lambda: handle_bet_coinflip(data, user_id, sid))

        elif msg_type == 'betSlots':
            with_lock(['user_' + str(user_id), 'slots'], lambda: handle_bet_slots(data, user_id, sid))

        elif msg_type == 'deposit':
            with_lock(['user_' + str(user_id)], lambda: handle_deposit(data, user_id, sid))

        elif msg_type == 'withdraw':
            with_lock(['user_' + str(user_id)], lambda: handle_withdraw(data, user_id, sid))

        elif msg_type == 'cancelWithdrawal':
            with_lock(['user_' + str(user_id)], lambda: handle_cancel_withdrawal(data, user_id, sid))

        elif msg_type == 'withdrawRefBalance':
            with_lock(['user_' + str(user_id)], lambda: handle_withdraw_ref(user_id, sid))

        elif msg_type == 'activatePromo':
            with_lock(['user_' + str(user_id)], lambda: handle_activate_promo(data, user_id, sid))

        elif msg_type == 'historyTransactions':
            handle_history_transactions(user_id, sid)

        elif msg_type == 'getProfile':
            handle_get_profile(user_id, sid)

        elif msg_type == 'fairCheck':
            handle_fair_check(data, user_id, sid)   

        # === NEW: Extended message types ===
        else:
            handle_extended_messages(data, user_id, sid, msg_type)

    except Exception as e:
        logger.error(f'Message handler error [{msg_type}]: {e}')
        sio.emit('message', {
            'type': 'alert',
            'type_alert': 'error',
            'alert_message': 'Ошибка сервера'
        }, room=sid)


# ================================================================
# SEND TO USER HELPER
# ================================================================
def emit_to_user(user_id, event, data):
    sid = connected_users.get(user_id)
    if sid:
        sio.emit(event, data, room=sid)


def emit_balance(user_id):
    user = get_user_by_id(user_id)
    if user:
        emit_to_user(user_id, 'message', {
            'type': 'updateBalance',
            'balance': int(user['balance'])
        })


def emit_alert(sid, alert_type, message, sound=None):
    data = {
        'type': 'alert',
        'type_alert': alert_type,
        'alert_message': message,
    }
    if sound:
        data['sound'] = sound
    sio.emit('message', data, room=sid)


def emit_alert_to_user(user_id, alert_type, message, sound=None):
    sid = connected_users.get(user_id)
    if sid:
        emit_alert(sid, alert_type, message, sound)


def broadcast_all(event, data):
    sio.emit(event, data)


# ================================================================
# WAGER SYSTEM
# ================================================================
def process_wager(user_id, bet_amount):
    user = get_user_by_id(user_id)
    if not user:
        return
    current_wager = user['wager']
    if current_wager > 0:
        new_wager = max(0, current_wager - bet_amount)
        query_exec("UPDATE users SET wager = %s WHERE id = %s", [new_wager, user_id])
    query_exec("UPDATE users SET total_wagered = total_wagered + %s WHERE id = %s", [bet_amount, user_id])


# ================================================================
# REFERRAL SYSTEM — DEPOSIT BONUS
# ================================================================
def process_ref_deposit_bonus(user_id, deposit_coins):
    user = get_user_by_id(user_id)
    if not user or not user['ref_by']:
        return

    referrer = get_user_by_tg(user['ref_by'])
    if not referrer:
        return

    settings = get_settings()
    ref_percent = float(settings['ref_percent']) if settings else 10.0
    bonus = int(deposit_coins * ref_percent / 100)

    if bonus <= 0:
        return

    query_exec("""
        UPDATE users SET ref_earned = ref_earned + %s, ref_available = ref_available + %s
        WHERE id = %s
    """, [bonus, bonus, referrer['id']])

    query_exec("""
        INSERT INTO ref_log (referrer_id, referred_id, deposit_amount, bonus_amount)
        VALUES (%s, %s, %s, %s)
    """, [referrer['id'], user_id, deposit_coins, bonus])

    total_banks = 0
    all_banks = query_all("SELECT * FROM banks")
    for b in all_banks:
        total_banks += b['bank']

    if total_banks > 0:
        for b in all_banks:
            proportion = b['bank'] / total_banks if total_banks != 0 else 0
            deduction = int(bonus * proportion)
            if deduction != 0:
                update_bank(b['game'], -deduction)

    emit_alert_to_user(referrer['id'], 'success',
                       f'Реферальный бонус +{bonus} монет от депозита друга!')


# ================================================================
# HANDLE WITHDRAW REF BALANCE
# ================================================================
def handle_withdraw_ref(user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    ref_available = int(user['ref_available'])
    if ref_available <= 0:
        return emit_alert(sid, 'error', 'Реферальный баланс пуст!')

    query_exec("""
        UPDATE users SET balance = balance + %s, ref_available = 0 WHERE id = %s
    """, [ref_available, user_id])

    new_balance = int(user['balance']) + ref_available
    add_transaction(user_id, 'ref_withdraw', ref_available, new_balance, 'Вывод реферального баланса')

    sio.emit('message', {
        'type': 'refWithdrawSuccess',
        'new_balance': new_balance,
        'ref_available': 0,
        'withdrawn_amount': ref_available,
        'message': f'Зачислено {ref_available} монет на основной баланс!'
    }, room=sid)


# ================================================================
# HANDLE ACTIVATE PROMO
# ================================================================
def handle_activate_promo(data, user_id, sid):
    code = str(data.get('promo', '')).strip().upper()
    if not code:
        return emit_alert(sid, 'error', 'Введите промокод')

    promo = query_one("SELECT * FROM promocodes WHERE code = %s AND is_active = 1", [code])
    if not promo:
        return emit_alert(sid, 'error', 'Промокод не найден или неактивен')

    if promo['current_uses'] >= promo['max_uses']:
        return emit_alert(sid, 'error', 'Промокод исчерпан')

    already_used = query_one(
        "SELECT id FROM promo_uses WHERE user_id = %s AND promo_id = %s",
        [user_id, promo['id']]
    )
    if already_used:
        return emit_alert(sid, 'error', 'Вы уже использовали этот промокод')

    reward = int(promo['reward'])
    wager_mult = float(promo['wager_multiplier'])
    wager_add = int(reward * wager_mult)

    query_exec_many([
        ("UPDATE users SET balance = balance + %s, wager = wager + %s WHERE id = %s",
         [reward, wager_add, user_id]),
        ("UPDATE promocodes SET current_uses = current_uses + 1 WHERE id = %s",
         [promo['id']]),
        ("INSERT INTO promo_uses (user_id, promo_id) VALUES (%s, %s)",
         [user_id, promo['id']]),
    ])

    user = get_user_by_id(user_id)
    new_balance = int(user['balance']) if user else 0
    add_transaction(user_id, 'promo', reward, new_balance, f'Промокод: {code}')

    sio.emit('message', {
        'type': 'promoSuccess',
        'balance': new_balance,
        'reward': reward,
        'wager': int(user['wager']) if user else 0,
    }, room=sid)

    emit_alert(sid, 'success', f'Промокод активирован! +{reward} монет')


# ================================================================
# HANDLE HISTORY TRANSACTIONS
# ================================================================
def handle_history_transactions(user_id, sid):
    txs = query_all("""
        SELECT type, amount, balance_after, description, created_at
        FROM transactions
        WHERE user_id = %s
        ORDER BY id DESC
        LIMIT 50
    """, [user_id])

    formatted = []
    for tx in txs:
        formatted.append({
            'type': tx['type'],
            'amount': tx['amount'],
            'balance_after': tx['balance_after'],
            'description': tx['description'],
            'date': tx['created_at'].strftime('%d.%m.%Y %H:%M') if tx['created_at'] else '',
        })

    sio.emit('message', {
        'type': 'historyTransactions',
        'history': formatted,
    }, room=sid)


# ================================================================
# HANDLE GET PROFILE
# ================================================================
def handle_get_profile(user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return

    total_games = 0
    total_won = 0

    mines_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(CASE WHEN result > 0 THEN result ELSE 0 END), 0) as won FROM mines_bets WHERE user_id = %s",
        [user_id]
    )
    if mines_stats:
        total_games += mines_stats['cnt']
        total_won += int(mines_stats['won'])

    cube_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(win_amount), 0) as won FROM cube_games WHERE user_id = %s",
        [user_id]
    )
    if cube_stats:
        total_games += cube_stats['cnt']
        total_won += int(cube_stats['won'])

    x50_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(result), 0) as won FROM x50_bets WHERE user_id = %s AND bet > 0",
        [user_id]
    )
    if x50_stats:
        total_games += x50_stats['cnt']
        total_won += int(x50_stats['won'])

    cases_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(win_amount), 0) as won FROM cases_bets WHERE user_id = %s",
        [user_id]
    )
    if cases_stats:
        total_games += cases_stats['cnt']
        total_won += int(cases_stats['won'])

    coinflip_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(win_amount), 0) as won FROM coinflip_bets WHERE user_id = %s",
        [user_id]
    )
    if coinflip_stats:
        total_games += coinflip_stats['cnt']
        total_won += int(coinflip_stats['won'])

    slots_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(win_amount), 0) as won FROM slots_bets WHERE user_id = %s",
        [user_id]
    )
    if slots_stats:
        total_games += slots_stats['cnt']
        total_won += int(slots_stats['won'])

    total_deposited = query_one(
        "SELECT COALESCE(SUM(amount_coins), 0) as total FROM deposits WHERE user_id = %s AND status = 1",
        [user_id]
    )
    total_withdrawn = query_one(
        "SELECT COALESCE(SUM(amount), 0) as total FROM withdrawals WHERE user_id = %s AND status = 1",
        [user_id]
    )

    sio.emit('message', {
        'type': 'profileData',
        'user_id': user['user_id'],
        'name': user['name'],
        'avatar': user['avatar'],
        'balance': int(user['balance']),
        'total_deposited': int(total_deposited['total']) if total_deposited else 0,
        'total_withdrawn': int(total_withdrawn['total']) if total_withdrawn else 0,
        'total_wagered': int(user['total_wagered']),
        'total_games': total_games,
        'total_won': total_won,
        'wager': int(user['wager']),
        'refs': user['refs'],
        'ref_earned': int(user['ref_earned']),
        'ref_available': int(user['ref_available']),
        'ref_link': f'https://t.me/{BOT_USERNAME}?start={user["user_id"]}',
        'joined': user['created_at'].strftime('%d.%m.%Y') if user.get('created_at') else '',
    }, room=sid)


# ================================================================
# FAIR GAME CHECK (universal)
# ================================================================
def handle_fair_check(data, user_id, sid):
    game = data.get('game', '')
    game_id = data.get('id', '')

    if not game_id:
        return emit_alert(sid, 'error', 'Укажите ID игры')

    try:
        game_id = int(game_id)
    except (ValueError, TypeError):
        return emit_alert(sid, 'error', 'Неверный ID')

    if game == 'mines':
        row = query_one(
            "SELECT hash, salt, lose_tiles FROM mines_bets WHERE id = %s AND user_id = %s AND status = 0",
            [game_id, user_id]
        )
        if not row:
            return emit_alert(sid, 'error', 'Игра не найдена или ещё не завершена')
        sio.emit('message', {
            'type': 'fairResult',
            'game': 'mines',
            'hash': row['hash'],
            'salt': row['salt'],
            'result': row['lose_tiles'],
            'verify': f"md5(\"{row['lose_tiles']}|{row['salt']}\") = {row['hash']}",
        }, room=sid)

    elif game == 'dice':
        row = query_one(
            "SELECT hash, salt, cube_result FROM cube_games WHERE id = %s AND user_id = %s",
            [game_id, user_id]
        )
        if not row:
            return emit_alert(sid, 'error', 'Игра не найдена')
        sio.emit('message', {
            'type': 'fairResult',
            'game': 'dice',
            'hash': row['hash'],
            'salt': row['salt'],
            'result': str(row['cube_result']),
            'verify': f"md5(\"{row['cube_result']}|{row['salt']}\") = {row['hash']}",
        }, room=sid)

    elif game == 'x50':
        row = query_one(
            "SELECT hash, salt, coeff FROM history_x50 WHERE id = %s",
            [game_id]
        )
        if not row:
            return emit_alert(sid, 'error', 'Раунд не найден')
        sio.emit('message', {
            'type': 'fairResult',
            'game': 'x50',
            'hash': row['hash'],
            'salt': row['salt'],
            'result': str(row['coeff']),
            'verify': f"md5(\"{row['coeff']}|{row['salt']}\") = {row['hash']}",
        }, room=sid)

    elif game == 'coinflip':
        row = query_one(
            "SELECT hash, salt, result FROM coinflip_bets WHERE id = %s AND user_id = %s",
            [game_id, user_id]
        )
        if not row:
            return emit_alert(sid, 'error', 'Игра не найдена')
        sio.emit('message', {
            'type': 'fairResult',
            'game': 'coinflip',
            'hash': row['hash'],
            'salt': row['salt'],
            'result': row['result'],
            'verify': f"md5(\"{row['result']}|{row['salt']}\") = {row['hash']}",
        }, room=sid)

    elif game == 'cases':
        row = query_one(
            "SELECT hash, salt, win_item FROM cases_bets WHERE id = %s AND user_id = %s",
            [game_id, user_id]
        )
        if not row:
            return emit_alert(sid, 'error', 'Игра не найдена')
        sio.emit('message', {
            'type': 'fairResult',
            'game': 'cases',
            'hash': row['hash'],
            'salt': row['salt'],
            'result': row['win_item'],
            'verify': f"md5(\"{row['win_item']}|{row['salt']}\") = {row['hash']}",
        }, room=sid)

    elif game == 'slots':
        row = query_one(
            "SELECT hash, salt, result_symbols FROM slots_bets WHERE id = %s AND user_id = %s",
            [game_id, user_id]
        )
        if not row:
            return emit_alert(sid, 'error', 'Игра не найдена')
        sio.emit('message', {
            'type': 'fairResult',
            'game': 'slots',
            'hash': row['hash'],
            'salt': row['salt'],
            'result': row['result_symbols'],
            'verify': f"md5(\"{row['result_symbols']}|{row['salt']}\") = {row['hash']}",
        }, room=sid)

    else:
        emit_alert(sid, 'error', 'Неизвестная игра')


# === КОНЕЦ ЧАСТИ 2/12 ===
# Следующая часть: Mines — полная серверная логика
# ============================================================
# app.py — Часть 3/12: Mines — полная серверная логика
# ============================================================

# ================================================================
# MINES — CREATE GAME
# ================================================================
def handle_create_mines(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['mines'] and user['role'] != 1:
        return emit_alert(sid, 'error', 'Mines закрыты на тех. работы')

    active = query_one(
        "SELECT id FROM mines_bets WHERE user_id = %s AND status = 1 LIMIT 1",
        [user_id]
    )
    if active:
        return emit_alert(sid, 'error', 'У вас уже есть активная игра! Завершите её.')

    bet_amount = rounded_int(data.get('amount', 0))
    mines_count = rounded_int(data.get('mines', 3))

    if bet_amount < 1:
        return emit_alert(sid, 'error', 'Минимальная ставка — 1')

    if bet_amount > 100000:
        return emit_alert(sid, 'error', 'Максимальная ставка — 100,000')

    if mines_count < 1 or mines_count > 24:
        return emit_alert(sid, 'error', 'Количество мин: от 1 до 24')

    if user['balance'] < bet_amount:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [bet_amount, user_id]
    )

    process_wager(user_id, bet_amount)

    salt = generate_salt()
    hash_placeholder = md5_hash(get_random_string(30) + "|" + salt)

    game_id = query_exec("""
        INSERT INTO mines_bets (user_id, bet, mines, tiles, lose_tiles, steps, current_coeff,
            currentAmount, result, hash, salt, status)
        VALUES (%s, %s, %s, '', '', 0, 1.00, 0, 0, %s, %s, 1)
    """, [user_id, bet_amount, mines_count, hash_placeholder, salt])

    new_balance = int(user['balance']) - bet_amount
    add_transaction(user_id, 'mines_bet', -bet_amount, new_balance, f'Mines ставка #{game_id}')

    update_bank('mines', bet_amount)

    coeff_table = get_mines_coefficients(mines_count)
    coefficients = []
    safe_tiles = 25 - mines_count
    for step in range(1, safe_tiles + 1):
        coefficients.append({
            'step': step,
            'coeff': coeff_table[step]
        })

    sio.emit('message', {
        'type': 'createMines',
        'game_id': game_id,
        'bet': bet_amount,
        'mines': mines_count,
        'newbalance': new_balance,
        'coefficients': coefficients,
    }, room=sid)


# ================================================================
# MINES — PLAY (OPEN TILE)
# ================================================================
def handle_play_mines(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    tile = rounded_int(data.get('tile', -1))
    if tile < 1 or tile > 25:
        return emit_alert(sid, 'error', 'Неверная клетка')

    game = query_one(
        "SELECT * FROM mines_bets WHERE user_id = %s AND status = 1 LIMIT 1",
        [user_id]
    )
    if not game:
        return emit_alert(sid, 'error', 'Активная игра не найдена')

    opened_tiles = game['tiles'].split('|') if game['tiles'] else []

    if str(tile) in opened_tiles:
        return emit_alert(sid, 'error', 'Клетка уже открыта')

    mines_count = game['mines']
    current_steps = game['steps']
    bet_amount = int(game['bet'])
    safe_tiles = 25 - mines_count

    bank = get_bank('mines')
    bank_val = bank['bank'] if bank else 0

    all_tiles = list(range(1, 26))
    opened_ints = [int(t) for t in opened_tiles if t]
    available_tiles = [t for t in all_tiles if t not in opened_ints and t != tile]

    coeff_table = get_mines_coefficients(mines_count)
    next_step = current_steps + 1
    next_coeff = coeff_table.get(next_step, 1.01)
    potential_amount = int(bet_amount * next_coeff)

    is_last_safe = (next_step >= safe_tiles)

    potential_profit = potential_amount - bet_amount
    can_afford = True

    if user['role'] != 1:
        if bank_val - potential_profit < bank['min_bank']:
            can_afford = False

    if can_afford:
        is_bomb = False
    else:
        bomb_chance = min(95, max(30, 50 + int((potential_profit - bank_val) / 100)))
        is_bomb = random.randint(1, 100) <= bomb_chance

    if not is_bomb:
        opened_tiles.append(str(tile))
        new_tiles_str = '|'.join(opened_tiles)
        current_amount = potential_amount

        query_exec("""
            UPDATE mines_bets SET tiles = %s, steps = %s, current_coeff = %s, currentAmount = %s
            WHERE id = %s
        """, [new_tiles_str, next_step, next_coeff, current_amount, game['id']])

        next_next_coeff = coeff_table.get(next_step + 1, 0)

        response = {
            'type': 'playMines',
            'win': 1,
            'tile': tile,
            'steps': next_step,
            'current_coeff': next_coeff,
            'next_coeff': next_next_coeff,
            'currentAmount': current_amount,
            'mines': mines_count,
            'last_kletka': 1 if is_last_safe else 0,
        }

        sio.emit('message', response, room=sid)

        if is_last_safe:
            eventlet.spawn_after(1.0, lambda: with_lock(
                ['user_' + str(user_id), 'mines'],
                lambda: auto_cashout_mines(user_id, sid)
            ))

    else:
        bomb_tiles = generate_bomb_tiles(opened_tiles, tile, mines_count)
        bomb_tiles_str = '|'.join(str(b) for b in bomb_tiles)

        salt = generate_salt()
        hash_val = md5_hash(bomb_tiles_str + "|" + salt)

        query_exec("""
            UPDATE mines_bets SET tiles = %s, lose_tiles = %s, hash = %s, salt = %s,
                result = 0, status = 0
            WHERE id = %s
        """, [
            '|'.join(opened_tiles),
            bomb_tiles_str, hash_val, salt, game['id']
        ])

        

        response = {
            'type': 'playMines',
            'win': 0,
            'tile': tile,
            'bomb_tiles': bomb_tiles,
            'steps': next_step,
            'mines': mines_count,
            'hash': hash_val,
            'salt': salt,
            'resultMines': bomb_tiles_str,
            'game_id': game['id'],
        }

        sio.emit('message', response, room=sid)


def generate_bomb_tiles(opened_tiles, bomb_tile, mines_count):
    all_tiles = list(range(1, 26))
    opened_ints = [int(t) for t in opened_tiles if t]

    bombs = [bomb_tile]

    available_for_bombs = [t for t in all_tiles if t not in opened_ints and t != bomb_tile]
    random.shuffle(available_for_bombs)

    remaining_bombs = mines_count - 1
    bombs.extend(available_for_bombs[:remaining_bombs])
    bombs.sort()

    return bombs


def get_unopened_tiles(opened_tiles_str, mines_count):
    all_tiles = list(range(1, 26))
    opened = [int(t) for t in opened_tiles_str.split('|') if t]

    unopened = [t for t in all_tiles if t not in opened]
    random.shuffle(unopened)

    selected = unopened[:mines_count]
    selected.sort()

    return '|'.join(str(t) for t in selected)


# ================================================================
# MINES — CASHOUT (TAKE WINNINGS)
# ================================================================
def handle_take_mines(user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    game = query_one(
        "SELECT * FROM mines_bets WHERE user_id = %s AND status = 1 LIMIT 1",
        [user_id]
    )
    if not game:
        return emit_alert(sid, 'error', 'Активная игра не найдена')

    if game['steps'] == 0:
        return emit_alert(sid, 'error', 'Сделайте хотя бы 1 ход!')

    win_amount = int(game['currentAmount'])
    bet_amount = int(game['bet'])
    profit = win_amount - bet_amount

    opened_tiles = game['tiles'] if game['tiles'] else ''
    lose_tiles = get_unopened_tiles(opened_tiles, game['mines'])

    salt = generate_salt()
    hash_val = md5_hash(lose_tiles + "|" + salt)

    query_exec("""
        UPDATE users SET balance = balance + %s WHERE id = %s
    """, [win_amount, user_id])

    query_exec("""
        UPDATE mines_bets SET result = %s, lose_tiles = %s, hash = %s, salt = %s, status = 0
        WHERE id = %s
    """, [win_amount, lose_tiles, hash_val, salt, game['id']])

    if user['role'] != 1:
        update_bank('mines', -profit)

    new_balance = int(user['balance']) + win_amount
    add_transaction(user_id, 'mines_win', win_amount, new_balance, f'Mines выигрыш #{game["id"]}')

    sio.emit('message', {
        'type': 'cashoutMines',
        'coeff': float(game['current_coeff']),
        'hash': hash_val,
        'salt': salt,
        'resultMines': lose_tiles,
        'win_sum': win_amount,
        'newbalance': new_balance,
        'game_id': game['id'],
    }, room=sid)


def auto_cashout_mines(user_id, sid):
    try:
        handle_take_mines(user_id, sid)
    except Exception as e:
        logger.error(f'Auto cashout mines error: {e}')


# ================================================================
# MINES — CHECK (VERIFY FAIRNESS)
# ================================================================
def handle_mines_check(data, user_id, sid):
    game_id = data.get('id', '')
    if not game_id:
        return emit_alert(sid, 'error', 'Для начала сыграйте игру')

    try:
        game_id = int(game_id)
    except (ValueError, TypeError):
        return emit_alert(sid, 'error', 'Неверные данные')

    row = query_one(
        "SELECT hash, salt, lose_tiles FROM mines_bets WHERE id = %s AND user_id = %s AND status = 0 LIMIT 1",
        [game_id, user_id]
    )
    if not row:
        return emit_alert(sid, 'error', 'Игра не найдена или ещё активна')

    sio.emit('message', {
        'type': 'minesCheckResult',
        'hash': row['hash'],
        'salt': row['salt'],
        'lose_tiles': row['lose_tiles'],
        'game_id': game_id,
        'verify_string': f"{row['lose_tiles']}|{row['salt']}",
    }, room=sid)


# === КОНЕЦ ЧАСТИ 3/12 ===
# Следующая часть: Dice (Cube) — полная серверная логика
# ============================================================
# app.py — Часть 4/12: Dice, Coinflip, Slots — серверная логика
# ============================================================

# ================================================================
# DICE (CUBE) — BET
# ================================================================
DICE_MODES = {
    'over': {
        1: {'chance': 83.33, 'coeff': 1.15},
        2: {'chance': 66.67, 'coeff': 1.44},
        3: {'chance': 50.00, 'coeff': 1.92},
        4: {'chance': 33.33, 'coeff': 2.88},
        5: {'chance': 16.67, 'coeff': 5.76},
    },
    'under': {
        2: {'chance': 16.67, 'coeff': 5.76},
        3: {'chance': 33.33, 'coeff': 2.88},
        4: {'chance': 50.00, 'coeff': 1.92},
        5: {'chance': 66.67, 'coeff': 1.44},
        6: {'chance': 83.33, 'coeff': 1.15},
    },
    'exact': {
        1: {'chance': 16.67, 'coeff': 5.76},
        2: {'chance': 16.67, 'coeff': 5.76},
        3: {'chance': 16.67, 'coeff': 5.76},
        4: {'chance': 16.67, 'coeff': 5.76},
        5: {'chance': 16.67, 'coeff': 5.76},
        6: {'chance': 16.67, 'coeff': 5.76},
    },
}


def handle_bet_cube(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['cube'] and user['role'] != 1:
        return emit_alert(sid, 'error', 'Dice закрыт на тех. работы')

    bet_amount = rounded_int(data.get('bet', 0))
    mode = str(data.get('mode', 'over')).lower()
    number = rounded_int(data.get('number', 0))

    if bet_amount < 1:
        return emit_alert(sid, 'error', 'Минимальная ставка — 1')

    if bet_amount > 100000:
        return emit_alert(sid, 'error', 'Максимальная ставка — 100,000')

    if mode not in DICE_MODES:
        return emit_alert(sid, 'error', 'Неверный режим игры')

    if number not in DICE_MODES[mode]:
        return emit_alert(sid, 'error', 'Неверное число')

    if user['balance'] < bet_amount:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    coeff = DICE_MODES[mode][number]['coeff']
    potential_win = int(bet_amount * coeff)
    potential_profit = potential_win - bet_amount

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [bet_amount, user_id]
    )

    process_wager(user_id, bet_amount)
    update_bank('cube', bet_amount)

    bank = get_bank('cube')
    bank_val = bank['bank'] if bank else 0

    cube_result = random.randint(1, 6)

    if mode == 'over':
        natural_win = (cube_result > number)
    elif mode == 'under':
        natural_win = (cube_result < number)
    else:
        natural_win = (cube_result == number)

    if natural_win and user['role'] != 1:
        if bank_val - potential_profit < bank['min_bank']:
            force_lose_chance = min(90, max(20, 40 + int((potential_profit - bank_val) / 50)))
            if random.randint(1, 100) <= force_lose_chance:
                if mode == 'over':
                    possible_losing = [i for i in range(1, 7) if i <= number]
                elif mode == 'under':
                    possible_losing = [i for i in range(1, 7) if i >= number]
                else:
                    possible_losing = [i for i in range(1, 7) if i != number]
                if possible_losing:
                    cube_result = random.choice(possible_losing)
                    natural_win = False

    if mode == 'over':
        is_win = (cube_result > number)
    elif mode == 'under':
        is_win = (cube_result < number)
    else:
        is_win = (cube_result == number)

    if is_win:
        win_amount = potential_win
    else:
        win_amount = 0

    salt = generate_salt()
    hash_val = md5_hash(str(cube_result) + "|" + salt)

    if is_win:
        query_exec(
            "UPDATE users SET balance = balance + %s WHERE id = %s",
            [win_amount, user_id]
        )
        if user['role'] != 1:
            update_bank('cube', -potential_profit)

    game_id = query_exec("""
        INSERT INTO cube_games (user_id, bet, mode, number, cube_result, win_amount, coeff, hash, salt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, [user_id, bet_amount, mode, number, cube_result, win_amount, coeff, hash_val, salt])

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    if is_win:
        add_transaction(user_id, 'dice_win', win_amount, new_balance, f'Dice выигрыш #{game_id}')
    else:
        add_transaction(user_id, 'dice_bet', -bet_amount, new_balance, f'Dice ставка #{game_id}')

    sio.emit('message', {
        'type': 'betCubeResult',
        'cube_result': cube_result,
        'win': 1 if is_win else 0,
        'win_amount': win_amount,
        'bet': bet_amount,
        'coeff': coeff,
        'mode': mode,
        'number': number,
        'newbalance': new_balance,
        'game_id': game_id,
        'hash': hash_val,
        'salt': salt,
    }, room=sid)


# ================================================================
# DICE — CHECK FAIRNESS
# ================================================================
def handle_dice_check(data, user_id, sid):
    game_id = data.get('id', '')
    if not game_id:
        return emit_alert(sid, 'error', 'Для начала сыграйте игру')

    try:
        game_id = int(game_id)
    except (ValueError, TypeError):
        return emit_alert(sid, 'error', 'Неверные данные')

    row = query_one(
        "SELECT hash, salt, cube_result FROM cube_games WHERE id = %s AND user_id = %s LIMIT 1",
        [game_id, user_id]
    )
    if not row:
        return emit_alert(sid, 'error', 'Игра не найдена')

    sio.emit('message', {
        'type': 'diceCheckResult',
        'hash': row['hash'],
        'salt': row['salt'],
        'cube_result': row['cube_result'],
        'game_id': game_id,
        'verify_string': f"{row['cube_result']}|{row['salt']}",
    }, room=sid)


# ================================================================
# COINFLIP — BET
# ================================================================
def handle_bet_coinflip(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['coinflip'] and user['role'] != 1:
        return emit_alert(sid, 'error', 'Coinflip закрыт на тех. работы')

    bet_amount = rounded_int(data.get('bet', 0))
    side = str(data.get('side', 'heads')).lower()

    if bet_amount < 1:
        return emit_alert(sid, 'error', 'Минимальная ставка — 1')

    if bet_amount > 100000:
        return emit_alert(sid, 'error', 'Максимальная ставка — 100,000')

    if side not in ('heads', 'tails'):
        return emit_alert(sid, 'error', 'Выберите сторону: Орёл или Решка')

    if user['balance'] < bet_amount:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    coeff = 1.94
    potential_win = int(bet_amount * coeff)
    potential_profit = potential_win - bet_amount

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [bet_amount, user_id]
    )

    process_wager(user_id, bet_amount)
    update_bank('coinflip', bet_amount)

    bank = get_bank('coinflip')
    bank_val = bank['bank'] if bank else 0

    result = random.choice(['heads', 'tails'])
    natural_win = (result == side)

    if natural_win and user['role'] != 1:
        if bank_val - potential_profit < bank['min_bank']:
            force_lose_chance = min(85, max(15, 35 + int((potential_profit - bank_val) / 50)))
            if random.randint(1, 100) <= force_lose_chance:
                result = 'tails' if side == 'heads' else 'heads'
                natural_win = False

    is_win = (result == side)
    win_amount = potential_win if is_win else 0

    salt = generate_salt()
    hash_val = md5_hash(result + "|" + salt)

    if is_win:
        query_exec(
            "UPDATE users SET balance = balance + %s WHERE id = %s",
            [win_amount, user_id]
        )
        if user['role'] != 1:
            update_bank('coinflip', -potential_profit)

    game_id = query_exec("""
        INSERT INTO coinflip_bets (user_id, bet, side, result, win_amount, hash, salt)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, [user_id, bet_amount, side, result, win_amount, hash_val, salt])

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    if is_win:
        add_transaction(user_id, 'coinflip_win', win_amount, new_balance, f'Coinflip выигрыш #{game_id}')
    else:
        add_transaction(user_id, 'coinflip_bet', -bet_amount, new_balance, f'Coinflip ставка #{game_id}')

    sio.emit('message', {
        'type': 'coinflipResult',
        'result': result,
        'side': side,
        'win': 1 if is_win else 0,
        'win_amount': win_amount,
        'bet': bet_amount,
        'coeff': coeff,
        'newbalance': new_balance,
        'game_id': game_id,
        'hash': hash_val,
        'salt': salt,
    }, room=sid)


# ================================================================
# SLOTS — BET
# ================================================================
SLOTS_SYMBOLS = ['🍒', '🍋', '🍊', '🍇', '💎', '7️⃣', '⭐']

SLOTS_PAYOUTS = {
    '🍒🍒🍒': 3.0,
    '🍋🍋🍋': 4.0,
    '🍊🍊🍊': 5.0,
    '🍇🍇🍇': 8.0,
    '💎💎💎': 15.0,
    '7️⃣7️⃣7️⃣': 30.0,
    '⭐⭐⭐': 50.0,
}

SLOTS_TWO_MATCH = 1.5


def get_slots_result_weighted(bank_val, min_bank, bet_amount, is_admin):
    s1 = random.choice(SLOTS_SYMBOLS)
    s2 = random.choice(SLOTS_SYMBOLS)
    s3 = random.choice(SLOTS_SYMBOLS)
    result_str = s1 + s2 + s3

    coeff = SLOTS_PAYOUTS.get(result_str, 0)
    if coeff == 0:
        if s1 == s2 or s2 == s3 or s1 == s3:
            coeff = SLOTS_TWO_MATCH
        else:
            coeff = 0

    potential_win = int(bet_amount * coeff) if coeff > 0 else 0
    potential_profit = potential_win - bet_amount

    if not is_admin and coeff > 0:
        if bank_val - potential_profit < min_bank:
            force_lose_chance = min(90, max(25, 40 + int(coeff * 5)))
            if random.randint(1, 100) <= force_lose_chance:
                attempts = 0
                while attempts < 20:
                    s1 = random.choice(SLOTS_SYMBOLS)
                    s2 = random.choice(SLOTS_SYMBOLS)
                    s3 = random.choice(SLOTS_SYMBOLS)
                    if s1 != s2 and s2 != s3 and s1 != s3:
                        result_str = s1 + s2 + s3
                        coeff = 0
                        break
                    attempts += 1

    return s1, s2, s3, result_str, coeff


def handle_bet_slots(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['slots'] and user['role'] != 1:
        return emit_alert(sid, 'error', 'Слоты закрыты на тех. работы')

    bet_amount = rounded_int(data.get('bet', 0))

    if bet_amount < 1:
        return emit_alert(sid, 'error', 'Минимальная ставка — 1')

    if bet_amount > 50000:
        return emit_alert(sid, 'error', 'Максимальная ставка — 50,000')

    if user['balance'] < bet_amount:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [bet_amount, user_id]
    )

    process_wager(user_id, bet_amount)
    update_bank('slots', bet_amount)

    bank = get_bank('slots')
    bank_val = bank['bank'] if bank else 0
    min_bank = bank['min_bank'] if bank else -30000

    is_admin = (user['role'] == 1)
    s1, s2, s3, result_str, coeff = get_slots_result_weighted(bank_val, min_bank, bet_amount, is_admin)

    if coeff > 0:
        win_amount = int(bet_amount * coeff)
    else:
        win_amount = 0

    salt = generate_salt()
    hash_val = md5_hash(result_str + "|" + salt)

    if win_amount > 0:
        query_exec(
            "UPDATE users SET balance = balance + %s WHERE id = %s",
            [win_amount, user_id]
        )
        profit_for_house = -(win_amount - bet_amount)
        if not is_admin:
            update_bank('slots', profit_for_house)

    game_id = query_exec("""
        INSERT INTO slots_bets (user_id, bet, result_symbols, win_amount, coeff, hash, salt)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, [user_id, bet_amount, result_str, win_amount, coeff, hash_val, salt])

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    if win_amount > 0:
        add_transaction(user_id, 'slots_win', win_amount, new_balance, f'Slots выигрыш #{game_id}')
    else:
        add_transaction(user_id, 'slots_bet', -bet_amount, new_balance, f'Slots ставка #{game_id}')

    sio.emit('message', {
        'type': 'slotsResult',
        'symbols': [s1, s2, s3],
        'result_str': result_str,
        'win': 1 if win_amount > 0 else 0,
        'win_amount': win_amount,
        'bet': bet_amount,
        'coeff': coeff,
        'newbalance': new_balance,
        'game_id': game_id,
        'hash': hash_val,
        'salt': salt,
    }, room=sid)


# ================================================================
# CASES — OPEN
# ================================================================
CASES_CONFIG = {
    'standard': {
        'price': 100,
        'items': [
            {'name': '10 монет', 'amount': 10, 'chance': 25, 'color': '#6B7D95'},
            {'name': '50 монет', 'amount': 50, 'chance': 25, 'color': '#3B82F6'},
            {'name': '100 монет', 'amount': 100, 'chance': 20, 'color': '#3B82F6'},
            {'name': '250 монет', 'amount': 250, 'chance': 15, 'color': '#8B5CF6'},
            {'name': '500 монет', 'amount': 500, 'chance': 8, 'color': '#F59E0B'},
            {'name': '1000 монет', 'amount': 1000, 'chance': 5, 'color': '#EF4444'},
            {'name': '2500 монет', 'amount': 2500, 'chance': 1.5, 'color': '#EF4444'},
            {'name': '5000 монет', 'amount': 5000, 'chance': 0.5, 'color': '#22C55E'},
        ]
    },
    'premium': {
        'price': 500,
        'items': [
            {'name': '50 монет', 'amount': 50, 'chance': 20, 'color': '#6B7D95'},
            {'name': '200 монет', 'amount': 200, 'chance': 20, 'color': '#3B82F6'},
            {'name': '500 монет', 'amount': 500, 'chance': 20, 'color': '#3B82F6'},
            {'name': '1000 монет', 'amount': 1000, 'chance': 15, 'color': '#8B5CF6'},
            {'name': '2500 монет', 'amount': 2500, 'chance': 12, 'color': '#F59E0B'},
            {'name': '5000 монет', 'amount': 5000, 'chance': 8, 'color': '#EF4444'},
            {'name': '10000 монет', 'amount': 10000, 'chance': 4, 'color': '#EF4444'},
            {'name': '25000 монет', 'amount': 25000, 'chance': 1, 'color': '#22C55E'},
        ]
    },
    'vip': {
        'price': 2000,
        'items': [
            {'name': '200 монет', 'amount': 200, 'chance': 15, 'color': '#6B7D95'},
            {'name': '1000 монет', 'amount': 1000, 'chance': 20, 'color': '#3B82F6'},
            {'name': '2000 монет', 'amount': 2000, 'chance': 20, 'color': '#3B82F6'},
            {'name': '5000 монет', 'amount': 5000, 'chance': 18, 'color': '#8B5CF6'},
            {'name': '10000 монет', 'amount': 10000, 'chance': 12, 'color': '#F59E0B'},
            {'name': '25000 монет', 'amount': 25000, 'chance': 8, 'color': '#EF4444'},
            {'name': '50000 монет', 'amount': 50000, 'chance': 5, 'color': '#EF4444'},
            {'name': '100000 монет', 'amount': 100000, 'chance': 2, 'color': '#22C55E'},
        ]
    },
}


def pick_case_item(case_type, bank_val, min_bank, bet_amount, is_admin):
    config = CASES_CONFIG.get(case_type)
    if not config:
        return None

    items = config['items']
    total_chance = sum(item['chance'] for item in items)
    roll = random.uniform(0, total_chance)

    cumulative = 0
    selected = items[-1]
    for item in items:
        cumulative += item['chance']
        if roll <= cumulative:
            selected = item
            break

    if not is_admin and selected['amount'] > bet_amount:
        profit_for_players = selected['amount'] - bet_amount
        if bank_val - profit_for_players < min_bank:
            force_lower_chance = min(85, max(20, 35 + int(selected['amount'] / bet_amount * 10)))
            if random.randint(1, 100) <= force_lower_chance:
                affordable = [it for it in items if it['amount'] <= bet_amount]
                if affordable:
                    selected = random.choice(affordable)

    return selected


def handle_open_case(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['cases_game'] and user['role'] != 1:
        return emit_alert(sid, 'error', 'Кейсы закрыты на тех. работы')

    case_type = str(data.get('case_type', 'standard')).lower()
    if case_type not in CASES_CONFIG:
        return emit_alert(sid, 'error', 'Неизвестный тип кейса')

    config = CASES_CONFIG[case_type]
    price = config['price']

    if user['balance'] < price:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [price, user_id]
    )

    process_wager(user_id, price)
    update_bank('cases', price)

    bank = get_bank('cases')
    bank_val = bank['bank'] if bank else 0
    min_bank = bank['min_bank'] if bank else -30000

    is_admin = (user['role'] == 1)
    item = pick_case_item(case_type, bank_val, min_bank, price, is_admin)

    win_amount = item['amount']
    win_name = item['name']
    coeff = round(win_amount / price, 2) if price > 0 else 0

    salt = generate_salt()
    hash_val = md5_hash(win_name + "|" + salt)

    query_exec(
        "UPDATE users SET balance = balance + %s WHERE id = %s",
        [win_amount, user_id]
    )

    profit_for_house = price - win_amount
    if not is_admin:
        update_bank('cases', -win_amount)

    game_id = query_exec("""
        INSERT INTO cases_bets (user_id, case_type, bet, win_amount, win_item, coeff, hash, salt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, [user_id, case_type, price, win_amount, win_name, coeff, hash_val, salt])

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    add_transaction(user_id, 'case_open', win_amount - price, new_balance,
                    f'Кейс {case_type}: {win_name}')

    all_items_for_animation = []
    items_pool = config['items']
    for _ in range(30):
        roll = random.uniform(0, sum(it['chance'] for it in items_pool))
        cum = 0
        for it in items_pool:
            cum += it['chance']
            if roll <= cum:
                all_items_for_animation.append({
                    'name': it['name'],
                    'amount': it['amount'],
                    'color': it['color'],
                })
                break

    win_position = random.randint(22, 27)
    all_items_for_animation[win_position] = {
        'name': win_name,
        'amount': win_amount,
        'color': item['color'],
    }

    sio.emit('message', {
        'type': 'caseResult',
        'case_type': case_type,
        'win_item': win_name,
        'win_amount': win_amount,
        'bet': price,
        'coeff': coeff,
        'newbalance': new_balance,
        'game_id': game_id,
        'hash': hash_val,
        'salt': salt,
        'animation_items': all_items_for_animation,
        'win_position': win_position,
        'win_color': item['color'],
    }, room=sid)


# === КОНЕЦ ЧАСТИ 4/12 ===
# Следующая часть: X50 Рулетка — полная мультиплеерная логика
# ============================================================
# app.py — Часть 5/12: X50 Рулетка — мультиплеерная логика
# ============================================================

# ================================================================
# X50 ROULETTE — JOIN (PLACE BET)
# ================================================================
def handle_join_x50(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['x50'] and user['role'] != 1:
        return emit_alert(sid, 'error', 'X50 закрыт на тех. работы')

    if x50_state['stage'] == 'PICKING':
        return emit_alert(sid, 'error', 'Игра уже началась! Дождитесь следующего раунда.')

    bet_amount = rounded_int(data.get('amount', 0))
    where = str(data.get('where', '')).strip()

    if bet_amount < 5:
        return emit_alert(sid, 'error', 'Минимальная ставка — 5')

    if bet_amount > 100000:
        return emit_alert(sid, 'error', 'Максимальная ставка — 100,000')

    if where not in ('2', '3', '5', '50'):
        return emit_alert(sid, 'error', 'Неверный коэффициент!')

    if user['balance'] < bet_amount:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    user_bet_count = x50_state['users_bets_count'].get(user_id, 0)
    if user_bet_count >= 3:
        return emit_alert(sid, 'error', 'Максимум 3 ставки за раунд!')

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [bet_amount, user_id]
    )

    process_wager(user_id, bet_amount)

    game_id = x50_state['current_game_id']
    hash_val = md5_hash(get_random_string(30))

    query_exec("""
        INSERT INTO x50_bets (game_id, user_id, bet, coeff, hash)
        VALUES (%s, %s, %s, %s, %s)
    """, [game_id, user_id, bet_amount, where, hash_val])

    x50_state['users_bets_count'][user_id] = user_bet_count + 1
    x50_state['users_bets_amount'][user_id] = x50_state['users_bets_amount'].get(user_id, 0) + bet_amount
    x50_state['total'] += bet_amount

    bet_info = {
        'user': user_id,
        'name': user['name'],
        'avatar': user['avatar'],
        'amount': bet_amount,
        'where': where,
    }
    x50_state['bets'].append(bet_info)

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    sio.emit('message', {
        'type': 'updateBalance',
        'balance': new_balance,
    }, room=sid)

    emit_alert(sid, 'success', 'Ставка принята!')

    broadcast_all('message', {
        'type': 'addBetX50',
        'bet': {
            'gameId': game_id,
            'user': user_id,
            'name': user['name'],
            'avatar': user['avatar'],
            'amount': bet_amount,
            'where': where,
            'total': x50_state['total'],
        }
    })

    start_x50_timer(game_id)


# ================================================================
# X50 — TIMER
# ================================================================
def start_x50_timer(game_id):
    if x50_state['stage'] != 'WAIT':
        return

    x50_state['stage'] = 'STARTED'
    x50_state['current_game_id'] = game_id

    duration = x50_state['time_seconds']

    def timer_loop():
        start_time = time.time()
        while True:
            elapsed = time.time() - start_time
            remaining = duration - elapsed

            if remaining <= 0:
                break

            formatted = f"{remaining:.2f}"

            broadcast_all('message', {
                'type': 'setTimesX50',
                'text': f"До старта {formatted}с",
                'start': True,
            })

            eventlet.sleep(0.05)

        x50_state['stage'] = 'PICKING'

        broadcast_all('message', {
            'type': 'setTimesX50',
            'text': 'Вращение...',
            'start': False,
        })

        eventlet.sleep(0.1)
        pick_winner_x50(game_id)

    eventlet.spawn(timer_loop)


# ================================================================
# X50 — PICK WINNER
# ================================================================
def pick_winner_x50(game_id):
    try:
        bets = x50_state['bets']

        bets_x2 = sum(b['amount'] for b in bets if b['where'] == '2')
        bets_x3 = sum(b['amount'] for b in bets if b['where'] == '3')
        bets_x5 = sum(b['amount'] for b in bets if b['where'] == '5')
        bets_x50 = sum(b['amount'] for b in bets if b['where'] == '50')
        total_bets = bets_x2 + bets_x3 + bets_x5 + bets_x50

        bank = get_bank('x50')
        bank_val = bank['bank'] if bank else 0
        min_bank = bank['min_bank'] if bank else -100000

        potential_wins = [
            {'coeff': 2, 'bet': bets_x2, 'win': bets_x2 * 1},
            {'coeff': 3, 'bet': bets_x3, 'win': bets_x3 * 2},
            {'coeff': 5, 'bet': bets_x5, 'win': bets_x5 * 4},
            {'coeff': 50, 'bet': bets_x50, 'win': bets_x50 * 49},
        ]

        for p in potential_wins:
            losing_bets = total_bets - p['bet']
            net_profit = p['win'] - losing_bets
            max_payout = bank_val + abs(min_bank)
            p['can_fall'] = net_profit <= max_payout
            p['net_profit'] = net_profit

        allowed = [p for p in potential_wins if p['can_fall']]

        if not allowed:
            allowed = potential_wins
            allowed.sort(key=lambda p: p['net_profit'])
            allowed = allowed[:1]

        rand_check_plus = random.randint(0, 100)
        rand_check_minus = random.randint(0, 100)

        if (bank_val < 0 and rand_check_minus < 12) or (bank_val > 0 and rand_check_plus < 6):
            allowed.sort(key=lambda p: p['net_profit'])

        def pick_random_coeff():
            r = random.randint(1, 1000)
            if r <= 480:
                target = 2
            elif r <= 800:
                target = 3
            elif r <= 990:
                target = 5
            else:
                target = 50

            if any(p['coeff'] == target for p in allowed):
                return target

            for p in allowed:
                return p['coeff']
            return 2

        selected_coeff = pick_random_coeff()
        selected = next((p for p in potential_wins if p['coeff'] == selected_coeff), potential_wins[0])

        net_profit = selected['net_profit']
        new_bank = bank_val - net_profit

        if bank:
            if new_bank > bank['max_bank']:
                new_bank = bank['max_bank']
            if new_bank < bank['min_bank']:
                new_bank = bank['min_bank']

        query_exec("UPDATE banks SET bank = %s WHERE game = 'x50'", [new_bank])

        coeff = selected['coeff']

        x2_positions = [18, 31, 45, 58, 71, 85, 98, 111, 125, 138, 152, 165, 178, 192, 205, 218, 231, 245, 258, 271, 285, 298, 311, 325, 338, 352]
        x3_positions = [25, 38, 51, 91, 105, 118, 158, 171, 184, 198, 211, 251, 265, 278, 318, 331, 345]
        x5_positions = [12, 65, 78, 132, 145, 225, 238, 292, 304, 358]
        x50_positions = [5, 5]

        if coeff == 2:
            pos = random.choice(x2_positions)
        elif coeff == 3:
            pos = random.choice(x3_positions)
        elif coeff == 5:
            pos = random.choice(x5_positions)
        else:
            pos = random.choice(x50_positions)

        random_offset = random.randint(1, 3)
        rotate_deg = pos - random_offset - 2880

        roll_duration = 10000
        roll_start = int(time.time() * 1000)

        current_roll = {
            'type': 'rollX50',
            'rotate': rotate_deg,
            'duration': roll_duration,
            'startTime': roll_start,
            'coeff': coeff,
        }

        broadcast_all('message', current_roll)

        salt = generate_salt()
        hash_x50 = md5_hash(str(coeff) + "|" + salt)

        history_id = query_exec(
            "INSERT INTO history_x50 (coeff, hash, salt) VALUES (%s, %s, %s)",
            [str(coeff), hash_x50, salt]
        )

        if total_bets == 0:
            hash_empty = md5_hash(get_random_string(30))
            query_exec("""
                INSERT INTO x50_bets (game_id, user_id, bet, coeff, hash)
                VALUES (%s, 0, 0, '0', %s)
            """, [game_id, hash_empty])

        query_exec(
            "UPDATE x50_bets SET coeffWon = %s, status = 1 WHERE game_id = %s",
            [str(coeff), game_id]
        )

        def after_roll():
            eventlet.sleep(9.5)

            X50_ROLLS_HISTORY.insert(0, {'id': history_id, 'coeff': str(coeff)})
            if len(X50_ROLLS_HISTORY) > 15:
                X50_ROLLS_HISTORY.pop()

            broadcast_all('message', {
                'type': 'x50History',
                'history': X50_ROLLS_HISTORY[:15],
            })

            winners = {}
            for bet_item in bets:
                if bet_item['where'] == str(coeff):
                    uid = bet_item['user']
                    win_sum = bet_item['amount'] * coeff
                    if uid not in winners:
                        winners[uid] = 0
                    winners[uid] += win_sum

            for uid, total_win in winners.items():
                query_exec(
                    "UPDATE users SET balance = balance + %s WHERE id = %s",
                    [total_win, uid]
                )

                query_exec(
                    "UPDATE x50_bets SET result = %s WHERE game_id = %s AND user_id = %s AND coeff = %s",
                    [total_win, game_id, uid, str(coeff)]
                )

                user_after = get_user_by_id(uid)
                if user_after:
                    add_transaction(uid, 'x50_win', total_win, int(user_after['balance']),
                                    f'X50 выигрыш раунд #{game_id}')

                emit_to_user(uid, 'message', {
                    'type': 'updateBalance',
                    'balance': int(user_after['balance']) if user_after else 0,
                })
                emit_alert_to_user(uid, 'success',
                                   f'Вы выиграли <b>{int(total_win)}</b> монет!', 'win.mp3')

            for bet_item in bets:
                if bet_item['where'] != str(coeff):
                    uid = bet_item['user']
                    user_check = get_user_by_id(uid)
                    if user_check:
                        add_transaction(uid, 'x50_bet', -bet_item['amount'],
                                        int(user_check['balance']),
                                        f'X50 ставка раунд #{game_id}')

            eventlet.sleep(3.5)

            next_game_id = game_id + 1
            reset_x50(next_game_id)

        eventlet.spawn(after_roll)

        logger.info(f"X50 #{game_id} finished — {coeff}x | bank: {new_bank}")

    except Exception as e:
        logger.error(f"X50 pick_winner error: {e}")
        eventlet.sleep(5)
        reset_x50(game_id + 1)


# ================================================================
# X50 — RESET
# ================================================================
def reset_x50(next_game_id):
    x50_state['stage'] = 'WAIT'
    x50_state['bets'] = []
    x50_state['users_bets_count'] = {}
    x50_state['users_bets_amount'] = {}
    x50_state['total'] = 0
    x50_state['current_game_id'] = next_game_id

    broadcast_all('message', {
        'type': 'resetX50',
    })

    broadcast_all('message', {
        'type': 'setTimesX50',
        'text': str(x50_state['time_seconds']),
        'start': False,
    })

    logger.info(f"X50 reset — next game #{next_game_id}")


# ================================================================
# X50 — CHECK FAIRNESS
# ================================================================
def handle_x50_check(data, user_id, sid):
    game_id = data.get('id', '')
    if not game_id:
        return emit_alert(sid, 'error', 'Для начала сыграйте игру')

    try:
        game_id = int(game_id)
    except (ValueError, TypeError):
        return emit_alert(sid, 'error', 'Неверные данные')

    row = query_one(
        "SELECT hash, salt, coeff FROM history_x50 WHERE id = %s LIMIT 1",
        [game_id]
    )
    if not row:
        return emit_alert(sid, 'error', 'Раунд не найден')

    sio.emit('message', {
        'type': 'x50CheckResult',
        'hash': row['hash'],
        'salt': row['salt'],
        'coeff': row['coeff'],
        'game_id': game_id,
        'verify_string': f"{row['coeff']}|{row['salt']}",
    }, room=sid)


# ================================================================
# X50 — INIT ON STARTUP
# ================================================================
def init_x50_on_startup():
    last_game = query_one(
        "SELECT game_id FROM x50_bets ORDER BY id DESC LIMIT 1"
    )
    if last_game:
        x50_state['current_game_id'] = last_game['game_id'] + 1
    else:
        x50_state['current_game_id'] = 1

    history = query_all(
        "SELECT id, coeff FROM history_x50 ORDER BY id DESC LIMIT 15"
    )
    if history:
        for h in history:
            X50_ROLLS_HISTORY.append({'id': h['id'], 'coeff': h['coeff']})

    logger.info(f"X50 initialized — next game #{x50_state['current_game_id']}")


# === КОНЕЦ ЧАСТИ 5/12 ===
# Следующая часть: Финансы — депозит/вывод CryptoBot
# ============================================================
# app.py — Часть 6/12: Финансы — депозит/вывод CryptoBot
# ============================================================

# ================================================================
# DEPOSIT — CREATE INVOICE
# ================================================================
def handle_deposit(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['deposits_enabled']:
        return emit_alert(sid, 'error', 'Пополнение временно недоступно')

    amount_usd = data.get('amount', 0)
    try:
        amount_usd = float(amount_usd)
    except (ValueError, TypeError):
        return emit_alert(sid, 'error', 'Неверная сумма')

    min_dep = float(settings['min_deposit_usd']) if settings else 1.0
    max_dep = float(settings['max_deposit_usd']) if settings else 10000.0

    if amount_usd < min_dep:
        return emit_alert(sid, 'error', f'Минимальная сумма — ${min_dep:.2f}')

    if amount_usd > max_dep:
        return emit_alert(sid, 'error', f'Максимальная сумма — ${max_dep:.2f}')

    coins_per_usd = settings['coins_per_usd'] if settings else COINS_PER_USD
    amount_coins = int(amount_usd * coins_per_usd)

    crypto_token = CRYPTOBOT_TOKEN
    if settings and settings.get('cryptobot_token'):
        crypto_token = settings['cryptobot_token']

    try:
        api_url = 'https://pay.crypt.bot/api/createInvoice'
        params = {
            'asset': 'USDT',
            'amount': str(amount_usd),
            'description': f'Пополнение баланса — {amount_coins} монет',
            'hidden_message': 'Спасибо за пополнение! Монеты зачислены.',
            'payload': json.dumps({
                'user_id': user_id,
                'tg_id': user['user_id'],
                'amount_usd': amount_usd,
                'amount_coins': amount_coins,
            }),
            'allow_comments': False,
            'allow_anonymous': False,
        }

        headers = {
            'Content-Type': 'application/json',
            'Crypto-Pay-API-Token': crypto_token,
        }

        r = requests.post(api_url, json=params, headers=headers, timeout=15)
        resp = r.json()

        if not resp.get('ok'):
            error_msg = resp.get('error', {}).get('message', 'Ошибка CryptoBot')
            logger.error(f'CryptoBot createInvoice error: {error_msg}')
            return emit_alert(sid, 'error', f'Ошибка платежа: {error_msg}')

        invoice = resp['result']
        invoice_id = str(invoice.get('invoice_id', ''))
        pay_url = invoice.get('pay_url', '')

        query_exec("""
            INSERT INTO deposits (user_id, tg_id, amount_usd, amount_coins, invoice_id, status)
            VALUES (%s, %s, %s, %s, %s, 0)
        """, [user_id, user['user_id'], amount_usd, amount_coins, invoice_id])

        sio.emit('message', {
            'type': 'depositInvoice',
            'pay_url': pay_url,
            'amount_usd': amount_usd,
            'amount_coins': amount_coins,
            'invoice_id': invoice_id,
        }, room=sid)

        logger.info(f'Deposit invoice created: user={user_id}, ${amount_usd}, {amount_coins} coins, invoice={invoice_id}')

    except requests.exceptions.Timeout:
        emit_alert(sid, 'error', 'Платёжная система не отвечает. Попробуйте позже.')
    except Exception as e:
        logger.error(f'Deposit error: {e}')
        emit_alert(sid, 'error', 'Ошибка создания платежа')


# ================================================================
# DEPOSIT — WEBHOOK (CryptoBot callback)
# ================================================================
@flask_app.route('/api/cryptobot_webhook', methods=['POST'])
def cryptobot_webhook():
    try:
        raw_body = request.get_data(as_text=True)
        data = request.get_json(force=True)

        logger.info(f'CryptoBot webhook received: {json.dumps(data)[:500]}')

        update_type = data.get('update_type', '')
        if update_type != 'invoice_paid':
            return jsonify({'ok': True}), 200

        payload_data = data.get('payload', {})
        if isinstance(payload_data, str):
            try:
                payload_data = json.loads(payload_data)
            except Exception:
                payload_data = {}

        invoice_id = str(payload_data.get('invoice_id', ''))

        if not invoice_id:
            invoice_id = str(data.get('payload', {}).get('invoice_id', ''))

        pay_load_inner = data.get('payload', '')
        if isinstance(pay_load_inner, str):
            try:
                pay_load_inner = json.loads(pay_load_inner)
            except Exception:
                pay_load_inner = {}

        user_id = 0
        tg_id = 0
        amount_usd = 0
        amount_coins = 0

        if isinstance(pay_load_inner, dict):
            user_id = pay_load_inner.get('user_id', 0)
            tg_id = pay_load_inner.get('tg_id', 0)
            amount_usd = pay_load_inner.get('amount_usd', 0)
            amount_coins = pay_load_inner.get('amount_coins', 0)

        if not user_id or not amount_coins:
            deposit_row = None
            if invoice_id:
                deposit_row = query_one(
                    "SELECT * FROM deposits WHERE invoice_id = %s AND status = 0 LIMIT 1",
                    [invoice_id]
                )
            if not deposit_row:
                pi = data.get('payload', '')
                if isinstance(pi, str):
                    try:
                        pi = json.loads(pi)
                    except Exception:
                        pi = {}
                if isinstance(pi, dict):
                    user_id = pi.get('user_id', 0)
                    tg_id = pi.get('tg_id', 0)
                    amount_usd = pi.get('amount_usd', 0)
                    amount_coins = pi.get('amount_coins', 0)
            else:
                user_id = deposit_row['user_id']
                tg_id = deposit_row['tg_id']
                amount_usd = float(deposit_row['amount_usd'])
                amount_coins = int(deposit_row['amount_coins'])

        if not user_id or not amount_coins:
            logger.error(f'Webhook: cannot determine user/amount from payload')
            return jsonify({'ok': False, 'error': 'invalid payload'}), 400

        user_id = int(user_id)
        amount_coins = int(amount_coins)

        existing = query_one(
            "SELECT id FROM deposits WHERE user_id = %s AND amount_coins = %s AND status = 1 AND created_at > DATE_SUB(NOW(), INTERVAL 5 MINUTE)",
            [user_id, amount_coins]
        )
        if existing:
            logger.warning(f'Webhook: duplicate deposit detected for user {user_id}')
            return jsonify({'ok': True}), 200

        query_exec(
            "UPDATE users SET balance = balance + %s, deposit = deposit + %s WHERE id = %s",
            [amount_coins, amount_coins, user_id]
        )

        if invoice_id:
            query_exec(
                "UPDATE deposits SET status = 1 WHERE invoice_id = %s",
                [invoice_id]
            )
        else:
            query_exec("""
                INSERT INTO deposits (user_id, tg_id, amount_usd, amount_coins, invoice_id, status)
                VALUES (%s, %s, %s, %s, %s, 1)
            """, [user_id, tg_id, amount_usd, amount_coins, invoice_id or 'webhook'])

        user_after = get_user_by_id(user_id)
        new_balance = int(user_after['balance']) if user_after else 0

        add_transaction(user_id, 'deposit', amount_coins, new_balance,
                        f'Пополнение ${amount_usd:.2f} = {amount_coins} монет')

        process_ref_deposit_bonus(user_id, amount_coins)

        emit_to_user(user_id, 'message', {
            'type': 'depositSuccess',
            'balance': new_balance,
            'amount_coins': amount_coins,
            'amount_usd': float(amount_usd),
        })

        emit_alert_to_user(user_id, 'success',
                           f'Баланс пополнен на {amount_coins} монет!')

        logger.info(f'Deposit completed: user={user_id}, ${amount_usd}, {amount_coins} coins')

        return jsonify({'ok': True}), 200

    except Exception as e:
        logger.error(f'Webhook error: {e}')
        return jsonify({'ok': False, 'error': str(e)}), 500


# ================================================================
# WITHDRAW — REQUEST
# ================================================================
def handle_withdraw(data, user_id, sid):
    user = get_user_by_id(user_id)
    if not user:
        return emit_alert(sid, 'error', 'Пользователь не найден')

    settings = get_settings()
    if settings and not settings['withdrawals_enabled']:
        return emit_alert(sid, 'error', 'Выводы временно недоступны')

    amount_coins = rounded_int(data.get('amount', 0))

    min_withdraw = settings['min_withdraw_coins'] if settings else 500
    max_withdraw = settings['max_withdraw_coins'] if settings else 1000000
    coins_per_usd = settings['coins_per_usd'] if settings else COINS_PER_USD

    if amount_coins < min_withdraw:
        return emit_alert(sid, 'error', f'Минимальный вывод — {min_withdraw} монет')

    if amount_coins > max_withdraw:
        return emit_alert(sid, 'error', f'Максимальный вывод — {max_withdraw} монет')

    if user['balance'] < amount_coins:
        return emit_alert(sid, 'error', 'Недостаточно средств!')

    if user['wager'] > 0:
        return emit_alert(sid, 'error',
                          f'Не��бходимо отыграть вейджер: осталось {int(user["wager"])} монет')

    pending = query_one(
        "SELECT id FROM withdrawals WHERE user_id = %s AND status = 0 LIMIT 1",
        [user_id]
    )
    if pending:
        return emit_alert(sid, 'error', 'У вас уже есть активная заявка на вывод. Дождитесь обработки.')

    amount_usd = round(amount_coins / coins_per_usd, 2)

    query_exec(
        "UPDATE users SET balance = balance - %s WHERE id = %s",
        [amount_coins, user_id]
    )

    withdraw_id = query_exec("""
    INSERT INTO withdrawals (user_id, tg_id, user_name, amount, amount_in_usdt, system_type, status)
    VALUES (%s, %s, %s, %s, %s, 1, 0)
""", [user_id, user['user_id'], user['name'], amount_coins, amount_usd])

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    add_transaction(user_id, 'withdraw_request', -amount_coins, new_balance,
                    f'Заявка на вывод #{withdraw_id}: {amount_coins} монет = ${amount_usd}')

    sio.emit('message', {
        'type': 'withdrawSuccess',
        'balance': new_balance,
        'amount_coins': amount_coins,
        'amount_usd': amount_usd,
        'withdraw_id': withdraw_id,
    }, room=sid)

    emit_alert(sid, 'success',
               f'Заявка на вывод ${amount_usd} создана! Ожидайте обработки.')

    for admin_id_internal in get_admin_internal_ids():
        emit_alert_to_user(admin_id_internal, 'info',
                           f'Новая заявка на вывод #{withdraw_id}: {user["name"]} — ${amount_usd}')

    logger.info(f'Withdraw request: user={user_id}, {amount_coins} coins, ${amount_usd}')


# ================================================================
# WITHDRAW — CANCEL BY USER
# ================================================================
def handle_cancel_withdrawal(data, user_id, sid):
    withdraw_id = rounded_int(data.get('id', 0))
    if not withdraw_id:
        return emit_alert(sid, 'error', 'Неверный ID заявки')

    row = query_one(
        "SELECT * FROM withdrawals WHERE id = %s AND user_id = %s AND status = 0 LIMIT 1",
        [withdraw_id, user_id]
    )
    if not row:
        return emit_alert(sid, 'error', 'Заявка не найдена или уже обработана')

    amount_coins = int(row['amount'])

    query_exec(
        "UPDATE users SET balance = balance + %s WHERE id = %s",
        [amount_coins, user_id]
    )

    query_exec(
        "UPDATE withdrawals SET status = 2, admin_comment = 'Отменено пользователем' WHERE id = %s",
        [withdraw_id]
    )

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    add_transaction(user_id, 'withdraw_cancel', amount_coins, new_balance,
                    f'Отмена вывода #{withdraw_id}')

    sio.emit('message', {
        'type': 'withdrawCancelled',
        'balance': new_balance,
        'withdraw_id': withdraw_id,
    }, room=sid)

    emit_alert(sid, 'success', 'Заявка на вывод отменена, средства возвращены')


# ================================================================
# ADMIN — PROCESS WITHDRAWAL (approve/reject via CryptoBot)
# ================================================================
def admin_approve_withdrawal(withdraw_id, admin_user_id=None):
    row = query_one(
        "SELECT * FROM withdrawals WHERE id = %s AND status = 0 LIMIT 1",
        [withdraw_id]
    )
    if not row:
        return False, 'Заявка не найдена'

    amount_usd = float(row['amount_in_usdt'])
    tg_id = row['tg_id']
    user_id = row['user_id']

    crypto_token = CRYPTOBOT_TOKEN
    settings = get_settings()
    if settings and settings.get('cryptobot_token'):
        crypto_token = settings['cryptobot_token']

    try:
        api_url = 'https://pay.crypt.bot/api/transfer'
        params = {
            'user_id': int(tg_id),
            'asset': 'USDT',
            'amount': str(amount_usd),
            'spend_id': f'withdraw_{withdraw_id}',
        }

        headers = {
            'Content-Type': 'application/json',
            'Crypto-Pay-API-Token': crypto_token,
        }

        r = requests.post(api_url, json=params, headers=headers, timeout=30)
        resp = r.json()

        if resp.get('ok'):
            check_link = ''
            if resp.get('result'):
                check_link = resp['result'].get('check_url', '') or ''

            query_exec("""
                UPDATE withdrawals SET status = 1, check_link = %s,
                    processed_at = NOW() WHERE id = %s
            """, [check_link, withdraw_id])

            user_after = get_user_by_id(user_id)
            if user_after:
                add_transaction(user_id, 'withdraw_complete', -int(row['amount']),
                                int(user_after['balance']),
                                f'Вывод #{withdraw_id}: ${amount_usd} выплачен')

            emit_alert_to_user(user_id, 'success',
                               f'Вывод ${amount_usd} выплачен!')
            emit_balance(user_id)

            logger.info(f'Withdrawal #{withdraw_id} approved: ${amount_usd} to tg_id={tg_id}')
            return True, f'Выплата ${amount_usd} отправлена'

        else:
            error_msg = resp.get('error', {}).get('message', 'Unknown error')
            logger.error(f'CryptoBot transfer error: {error_msg}')
            return False, f'Ошибка CryptoBot: {error_msg}'

    except requests.exceptions.Timeout:
        return False, 'CryptoBot не отвечает'
    except Exception as e:
        logger.error(f'Admin approve error: {e}')
        return False, f'Ошибка: {str(e)}'


def admin_reject_withdrawal(withdraw_id, comment='Отклонено администратором'):
    row = query_one(
        "SELECT * FROM withdrawals WHERE id = %s AND status = 0 LIMIT 1",
        [withdraw_id]
    )
    if not row:
        return False, 'Заявка не найдена'

    user_id = row['user_id']
    amount_coins = int(row['amount'])

    query_exec(
        "UPDATE users SET balance = balance + %s WHERE id = %s",
        [amount_coins, user_id]
    )

    query_exec("""
        UPDATE withdrawals SET status = 2, admin_comment = %s,
            processed_at = NOW() WHERE id = %s
    """, [comment, withdraw_id])

    user_after = get_user_by_id(user_id)
    new_balance = int(user_after['balance']) if user_after else 0

    add_transaction(user_id, 'withdraw_rejected', amount_coins, new_balance,
                    f'Вывод #{withdraw_id} отклонён: {comment}')

    emit_to_user(user_id, 'message', {
        'type': 'updateBalance',
        'balance': new_balance,
    })

    emit_alert_to_user(user_id, 'error',
                       f'Заявка на вывод отклонена: {comment}')

    logger.info(f'Withdrawal #{withdraw_id} rejected: {comment}')
    return True, 'Заявка отклонена, средства возвращены'


# ================================================================
# HELPER — GET ADMIN INTERNAL IDS
# ================================================================
def get_admin_internal_ids():
    admins = []
    for tg_id in ADMIN_IDS:
        user = get_user_by_tg(tg_id)
        if user:
            admins.append(user['id'])
    return admins


# ================================================================
# FLASK HEALTH + STATIC
# ================================================================
@flask_app.route('/', methods=['GET'])
def health():
    return jsonify({
        'status': 'ok',
        'service': 'Casino Backend',
        'time': datetime.now().isoformat(),
        'connected': len(connected_users),
    })


@flask_app.route('/avatars/<path:filename>', methods=['GET'])
def serve_avatar(filename):
    from flask import send_from_directory
    return send_from_directory(AVATAR_DIR, filename)


@flask_app.route('/api/deposit_webhook', methods=['POST'])
def deposit_webhook_alias():
    return cryptobot_webhook()


# === КОНЕЦ ЧАСТИ 6/12 ===
# Следующая часть: Telegram бот (команды, /start, реферальная система)
# ============================================================
# app.py — Часть 7/12: Telegram бот — команды, /start, рефералка
# ============================================================

# ================================================================
# TELEGRAM BOT — /start
# ================================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    tg_id = user.id
    first_name = user.first_name or ''
    last_name = user.last_name or ''
    full_name = f'{first_name} {last_name}'.strip()

    ref_by = 0
    if context.args:
        try:
            ref_by = int(context.args[0])
        except (ValueError, IndexError):
            ref_by = 0

    if ref_by == tg_id:
        ref_by = 0

    avatar_path = ''
    try:
        photos = await context.bot.get_user_profile_photos(tg_id, limit=1)
        if photos.photos:
            best = photos.photos[0][-1]
            avatar_path = download_avatar(TOKEN, best.file_id, tg_id)
    except Exception as e:
        logger.warning(f'Avatar fetch error for {tg_id}: {e}')

    if not avatar_path:
        avatar_path = generate_default_avatar_path(tg_id, full_name)

    db_user = get_user_by_tg(tg_id)

    if db_user:
        token = str(uuid.uuid4())
        query_exec("""
            UPDATE users SET token = %s, avatar = %s, name = %s WHERE user_id = %s
        """, [token, avatar_path or db_user['avatar'], full_name, tg_id])
        db_user = get_user_by_tg(tg_id)
    else:
        token = str(uuid.uuid4())
        query_exec("""
            INSERT INTO users (user_id, name, avatar, balance, deposit, ref_by, token)
            VALUES (%s, %s, %s, 0, 0, %s, %s)
        """, [tg_id, full_name, avatar_path, ref_by, token])

        if ref_by:
            ref_user = get_user_by_tg(ref_by)
            if ref_user:
                query_exec("UPDATE users SET refs = refs + 1 WHERE user_id = %s", [ref_by])

        db_user = get_user_by_tg(tg_id)

    webapp_url = f'{SITE_URL}/?token={db_user["token"]}&user_id={tg_id}'

    caption = (
        f'➤ Добро пожаловать, <b>{full_name}</b>! 🎲\n\n'
        '• Наши возможности:\n\n'
        '🎰 <b>6 уникальных игр</b> — Mines, Dice, X50 Рулетка, Кейсы, Coinflip, Слоты\n\n'
        '🫂 <b>Реферальная система</b> — получайте 10% от каждого депозита друга\n\n'
        '✅ <b>Моментальные</b> пополнения и выводы через CryptoBot\n\n'
        '🛡️ <b>Provably Fair</b> — проверяйте честность каждой игры\n\n'
        '🎁 <b>Промокоды</b> — следите за новостями и получайте бонусы'
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('🚀 Играть', web_app={'url': webapp_url})],
        [
            InlineKeyboardButton('📰 Новости', url='https://t.me/WelpCasino'),
            InlineKeyboardButton('🎁 Бонусы', url='https://t.me/WelpBonus'),
        ],
    ])

    banner_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'botbanner.png')
    if os.path.exists(banner_path):
        try:
            with open(banner_path, 'rb') as photo:
                await update.message.reply_photo(
                    photo=photo,
                    caption=caption,
                    parse_mode='HTML',
                    reply_markup=keyboard,
                )
        except Exception:
            await update.message.reply_text(
                caption,
                parse_mode='HTML',
                reply_markup=keyboard,
            )
    else:
        await update.message.reply_text(
            caption,
            parse_mode='HTML',
            reply_markup=keyboard,
        )

    if ref_by and db_user.get('launched_mini_app', 0) == 0:
        ref_user = get_user_by_tg(ref_by)
        if ref_user:
            await update.message.reply_text(
                '🎁 Вам доступен бонус! Перейдите в mini-app, чтобы забрать его.',
                parse_mode='HTML',
            )


# ================================================================
# TELEGRAM BOT — /help
# ================================================================
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        '<b>📖 Помощь</b>\n\n'
        '<b>Команды:</b>\n'
        '/start — Запустить бота\n'
        '/help — Помощь\n'
        '/balance — Ваш баланс\n'
        '/ref — Реферальная ссылка\n\n'
        '<b>Админ команды:</b>\n'
        '/admin — Панель администратора\n\n'
        '<b>Как играть:</b>\n'
        '1. Нажмите "Играть" и откроется казино\n'
        '2. Пополните баланс через CryptoBot\n'
        '3. Выберите игру и делайте ставки\n'
        '4. Выводите выигрыш на свой кошелёк'
    )

    await update.message.reply_text(text, parse_mode='HTML')


# ================================================================
# TELEGRAM BOT — /balance
# ================================================================
async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.message.from_user.id
    db_user = get_user_by_tg(tg_id)

    if not db_user:
        await update.message.reply_text('❌ Сначала запустите бота: /start')
        return

    text = (
        f'💰 <b>Ваш баланс:</b> {int(db_user["balance"])} монет\n'
        f'💵 <b>Эквивалент:</b> ${int(db_user["balance"]) / COINS_PER_USD:.2f}\n\n'
        f'📊 <b>Всего поставлено:</b> {int(db_user["total_wagered"])} монет\n'
        f'💳 <b>Всего пополнено:</b> {int(db_user["deposit"])} монет'
    )

    await update.message.reply_text(text, parse_mode='HTML')


# ================================================================
# TELEGRAM BOT — /ref
# ================================================================
async def cmd_ref(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.message.from_user.id
    db_user = get_user_by_tg(tg_id)

    if not db_user:
        await update.message.reply_text('❌ Сначала запустите бота: /start')
        return

    ref_link = f'https://t.me/{BOT_USERNAME}?start={tg_id}'

    text = (
        f'<b>🫂 Реферальная система</b>\n\n'
        f'<b>Ваша ссылка:</b>\n<code>{ref_link}</code>\n\n'
        f'👥 <b>Рефералов:</b> {db_user["refs"]}\n'
        f'💰 <b>Заработано:</b> {int(db_user["ref_earned"])} монет\n'
        f'✅ <b>Доступно к выводу:</b> {int(db_user["ref_available"])} монет\n\n'
        f'<i>Вы получаете 10% от каждого депозита вашего реферала!</i>'
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 ��копировать ссылку', copy_text={'text': ref_link})],
    ])

    await update.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# TELEGRAM BOT — /admin
# ================================================================
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.message.from_user.id
    if tg_id not in ADMIN_IDS:
        await update.message.reply_text('❌ Доступ запрещён')
        return

    text = (
        '<b>🛠 Панель администратора</b>\n\n'
        'Выберите раздел:'
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('📊 Статистика', callback_data='admin_stats'),
            InlineKeyboardButton('👥 Юзеры', callback_data='admin_users'),
        ],
        [
            InlineKeyboardButton('💳 Выплаты', callback_data='admin_withdrawals'),
            InlineKeyboardButton('💰 Депозиты', callback_data='admin_deposits'),
        ],
        [
            InlineKeyboardButton('🎁 Промокоды', callback_data='admin_promos'),
            InlineKeyboardButton('🏦 Банки', callback_data='admin_banks'),
        ],
        [
            InlineKeyboardButton('⚙️ Настройки игр', callback_data='admin_game_settings'),
            InlineKeyboardButton('💵 Настройки фин.', callback_data='admin_fin_settings'),
        ],
        [
            InlineKeyboardButton('🔍 Найти юзера', callback_data='admin_find_user'),
        ],
    ])

    await update.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — CALLBACK HANDLER
# ================================================================
async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    tg_id = query.from_user.id

    if tg_id not in ADMIN_IDS:
        await query.answer('Нет доступа', show_alert=True)
        return

    await query.answer()
    data = query.data

    # ------- STATISTICS -------
    if data == 'admin_stats':
        await admin_show_stats(query, context)

    elif data.startswith('admin_stats_'):
        period = data.replace('admin_stats_', '')
        await admin_show_stats(query, context, period)

    # ------- WITHDRAWALS -------
    elif data == 'admin_withdrawals':
        await admin_show_withdrawals(query, context)

    elif data.startswith('admin_approve_'):
        wid = int(data.replace('admin_approve_', ''))
        success, msg = admin_approve_withdrawal(wid)
        await query.message.reply_text(f'{"✅" if success else "❌"} {msg}', parse_mode='HTML')
        await admin_show_withdrawals(query, context)

    elif data.startswith('admin_reject_'):
        wid = int(data.replace('admin_reject_', ''))
        success, msg = admin_reject_withdrawal(wid)
        await query.message.reply_text(f'{"✅" if success else "❌"} {msg}', parse_mode='HTML')
        await admin_show_withdrawals(query, context)

    # ------- DEPOSITS -------
    elif data == 'admin_deposits':
        await admin_show_deposits(query, context)

    # ------- PROMOS -------
    elif data == 'admin_promos':
        await admin_show_promos(query, context)

    elif data.startswith('admin_promo_toggle_'):
        pid = int(data.replace('admin_promo_toggle_', ''))
        promo = query_one("SELECT is_active FROM promocodes WHERE id = %s", [pid])
        if promo:
            new_status = 0 if promo['is_active'] else 1
            query_exec("UPDATE promocodes SET is_active = %s WHERE id = %s", [new_status, pid])
        await admin_show_promos(query, context)

    elif data.startswith('admin_promo_del_'):
        pid = int(data.replace('admin_promo_del_', ''))
        query_exec("DELETE FROM promocodes WHERE id = %s", [pid])
        await query.message.reply_text('🗑 Промокод удалён')
        await admin_show_promos(query, context)

    # ------- BANKS -------
    elif data == 'admin_banks':
        await admin_show_banks(query, context)

    # ------- GAME SETTINGS -------
    elif data == 'admin_game_settings':
        await admin_show_game_settings(query, context)

    elif data.startswith('admin_toggle_game_'):
        game_name = data.replace('admin_toggle_game_', '')
        # Whitelist validation to prevent SQL injection
        allowed_fields = ['mines', 'cube', 'x50', 'cases_game', 'coinflip', 'slots']
        if game_name not in allowed_fields:
            await query.answer('Недопустимое поле', show_alert=True)
            return
        settings = get_settings()
        if settings and game_name in settings:
            new_val = 0 if settings[game_name] else 1
            query_exec(f"UPDATE settings SET `{game_name}` = %s WHERE id = 1", [new_val])
        await admin_show_game_settings(query, context)
    # ------- FIN SETTINGS -------
    elif data == 'admin_fin_settings':
        await admin_show_fin_settings(query, context)

    elif data.startswith('admin_toggle_fin_'):
        field = data.replace('admin_toggle_fin_', '')
        # Whitelist validation to prevent SQL injection
        allowed_fields = ['deposits_enabled', 'withdrawals_enabled']
        if field not in allowed_fields:
            await query.answer('Недопустимое поле', show_alert=True)
            return
        settings = get_settings()
        if settings and field in settings:
            new_val = 0 if settings[field] else 1
            query_exec(f"UPDATE settings SET `{field}` = %s WHERE id = 1", [new_val])
        await admin_show_fin_settings(query, context)

    # ------- USERS -------
    elif data == 'admin_users':
        await admin_show_users(query, context)

    elif data == 'admin_find_user':
        await query.message.edit_text(
            '<b>🔍 Поиск пользователя</b>\n\n'
            'Отправьте Telegram ID или внутренний ID пользователя:',
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('◀️ Назад', callback_data='admin_back')],
            ])
        )
        context.user_data['admin_state'] = 'find_user'

    elif data.startswith('admin_user_'):
        uid = int(data.replace('admin_user_', ''))
        await admin_show_user_detail(query, context, uid)

    elif data.startswith('admin_ban_'):
        uid = int(data.replace('admin_ban_', ''))
        user = get_user_by_id(uid)
        if user:
            new_ban = 0 if user['is_banned'] else 1
            query_exec("UPDATE users SET is_banned = %s WHERE id = %s", [new_ban, uid])
        await admin_show_user_detail(query, context, uid)

    elif data.startswith('admin_setrole_'):
        parts = data.replace('admin_setrole_', '').split('_')
        uid = int(parts[0])
        new_role = int(parts[1])
        query_exec("UPDATE users SET role = %s WHERE id = %s", [new_role, uid])
        await admin_show_user_detail(query, context, uid)

    elif data.startswith('admin_addbal_'):
        uid = int(data.replace('admin_addbal_', ''))
        context.user_data['admin_state'] = f'addbal_{uid}'
        await query.message.reply_text(
            f'💰 Введите сумму для начисления пользователю #{uid}:\n'
            '<i>Отрицательное число для списания</i>',
            parse_mode='HTML',
        )

    # ------- BACK -------
    elif data == 'admin_back':
        context.user_data.pop('admin_state', None)
        text = '<b>🛠 Панель администратора</b>\n\nВыберите раздел:'
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton('📊 Статистика', callback_data='admin_stats'),
                InlineKeyboardButton('👥 Юзеры', callback_data='admin_users'),
            ],
            [
                InlineKeyboardButton('💳 Выплаты', callback_data='admin_withdrawals'),
                InlineKeyboardButton('💰 Депозиты', callback_data='admin_deposits'),
            ],
            [
                InlineKeyboardButton('🎁 Промокоды', callback_data='admin_promos'),
                InlineKeyboardButton('🏦 Банки', callback_data='admin_banks'),
            ],
            [
                InlineKeyboardButton('⚙️ Настройки игр', callback_data='admin_game_settings'),
                InlineKeyboardButton('💵 Настройки фин.', callback_data='admin_fin_settings'),
            ],
            [
                InlineKeyboardButton('🔍 Найти юзера', callback_data='admin_find_user'),
            ],
        ])
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — TEXT MESSAGE HANDLER (for find_user, addbal, create promo)
# ================================================================
async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.message.from_user.id
    if tg_id not in ADMIN_IDS:
        return

    state = context.user_data.get('admin_state', '')

    if state == 'find_user':
        context.user_data.pop('admin_state', None)
        search = update.message.text.strip()
        user = None

        if search.isdigit():
            user = get_user_by_tg(int(search))
            if not user:
                user = get_user_by_id(int(search))

        if not user:
            await update.message.reply_text('❌ Пользователь не найден')
            return

        await admin_show_user_detail_msg(update, context, user['id'])

    elif state.startswith('addbal_'):
        uid = int(state.replace('addbal_', ''))
        context.user_data.pop('admin_state', None)

        try:
            amount = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text('❌ Введите число')
            return

        query_exec("UPDATE users SET balance = balance + %s WHERE id = %s", [amount, uid])
        user = get_user_by_id(uid)
        new_balance = int(user['balance']) if user else 0

        add_transaction(uid, 'admin_balance', amount, new_balance,
                        f'Изменение баланса админом: {amount:+d}')

        emit_balance(uid)

        await update.message.reply_text(
            f'✅ Баланс пользователя #{uid} изменён на {amount:+d}\n'
            f'Новый баланс: {new_balance}',
            parse_mode='HTML',
        )

    elif state == 'create_promo':
        context.user_data.pop('admin_state', None)
        parts = update.message.text.strip().split()

        if len(parts) < 3:
            await update.message.reply_text(
                '❌ Формат: <code>КОД СУММА МАКС_ИСПОЛЬЗОВАНИЙ [ВЕЙДЖЕР_МНОЖИТЕЛЬ]</code>',
                parse_mode='HTML',
            )
            return

        code = parts[0].upper()
        try:
            reward = int(parts[1])
            max_uses = int(parts[2])
            wager_mult = float(parts[3]) if len(parts) > 3 else 1.0
        except (ValueError, IndexError):
            await update.message.reply_text('❌ Неверные данные')
            return

        existing = query_one("SELECT id FROM promocodes WHERE code = %s", [code])
        if existing:
            await update.message.reply_text('❌ Промокод с таким кодом уже существует')
            return

        query_exec("""
            INSERT INTO promocodes (code, reward, max_uses, wager_multiplier, is_active)
            VALUES (%s, %s, %s, %s, 1)
        """, [code, reward, max_uses, wager_mult])

        await update.message.reply_text(
            f'✅ Промокод создан!\n\n'
            f'📝 Код: <code>{code}</code>\n'
            f'💰 Сумма: {reward} монет\n'
            f'👥 Макс. использований: {max_uses}\n'
            f'🔄 Вейджер: x{wager_mult}',
            parse_mode='HTML',
        )


# === КОНЕЦ ЧАСТИ 7/12 ===
# Следующая часть: Админ-панель — все отображения (stats, withdrawals, users...)
# ============================================================
# app.py — Часть 8/12: Админ-панель — все show-функции
# ============================================================

# ================================================================
# ADMIN — STATISTICS
# ================================================================
async def admin_show_stats(query, context, period='today'):
    now = datetime.now()

    if period == 'today':
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name = '📅 Сегодня'
    elif period == 'week':
        start = now - timedelta(days=7)
        period_name = '📅 Неделя'
    elif period == 'month':
        start = now - timedelta(days=30)
        period_name = '📅 Месяц'
    else:
        start = datetime(2020, 1, 1)
        period_name = '📅 Всё время'

    start_str = start.strftime('%Y-%m-%d %H:%M:%S')
    end_str = now.strftime('%Y-%m-%d %H:%M:%S')

    new_users = query_one(
        "SELECT COUNT(*) as cnt FROM users WHERE created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )
    total_users = query_one("SELECT COUNT(*) as cnt FROM users")

    deps = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(amount_coins), 0) as total FROM deposits WHERE status = 1 AND created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    withs = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(amount), 0) as total FROM withdrawals WHERE status = 1 AND created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    pending_withs = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(amount), 0) as total FROM withdrawals WHERE status = 0"
    )

    mines_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(bet), 0) as bets, COALESCE(SUM(CASE WHEN result > 0 THEN result ELSE 0 END), 0) as wins FROM mines_bets WHERE created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    cube_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(bet), 0) as bets, COALESCE(SUM(win_amount), 0) as wins FROM cube_games WHERE created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    x50_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(bet), 0) as bets, COALESCE(SUM(result), 0) as wins FROM x50_bets WHERE bet > 0 AND created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    cases_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(bet), 0) as bets, COALESCE(SUM(win_amount), 0) as wins FROM cases_bets WHERE created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    coinflip_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(bet), 0) as bets, COALESCE(SUM(win_amount), 0) as wins FROM coinflip_bets WHERE created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    slots_stats = query_one(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(bet), 0) as bets, COALESCE(SUM(win_amount), 0) as wins FROM slots_bets WHERE created_at BETWEEN %s AND %s",
        [start_str, end_str]
    )

    def profit(bets, wins):
        return int(bets) - int(wins)

    def fmt_profit(val):
        if val >= 0:
            return f'+{val} 🟢'
        return f'{val} 🔴'

    mines_p = profit(mines_stats['bets'], mines_stats['wins']) if mines_stats else 0
    cube_p = profit(cube_stats['bets'], cube_stats['wins']) if cube_stats else 0
    x50_p = profit(x50_stats['bets'], x50_stats['wins']) if x50_stats else 0
    cases_p = profit(cases_stats['bets'], cases_stats['wins']) if cases_stats else 0
    coinflip_p = profit(coinflip_stats['bets'], coinflip_stats['wins']) if coinflip_stats else 0
    slots_p = profit(slots_stats['bets'], slots_stats['wins']) if slots_stats else 0
    total_profit = mines_p + cube_p + x50_p + cases_p + coinflip_p + slots_p

    dep_total = int(deps['total']) if deps else 0
    with_total = int(withs['total']) if withs else 0

    text = (
        f'<b>📊 Статистика | {period_name}</b>\n\n'
        f'<b>👥 Пользователи:</b>\n'
        f'   Новых: {new_users["cnt"] if new_users else 0}\n'
        f'   Всего: {total_users["cnt"] if total_users else 0}\n'
        f'   Онлайн: {len(connected_users)}\n\n'
        f'<b>💰 Финансы:</b>\n'
        f'   Депозиты: {deps["cnt"] if deps else 0} шт. / {dep_total} монет (${dep_total / COINS_PER_USD:.2f})\n'
        f'   Выплаты: {withs["cnt"] if withs else 0} шт. / {with_total} монет (${with_total / COINS_PER_USD:.2f})\n'
        f'   Ожидают: {pending_withs["cnt"] if pending_withs else 0} шт. / {int(pending_withs["total"]) if pending_withs else 0} монет\n\n'
        f'<b>🎮 Игры (ставки / выигрыши / профит):</b>\n'
        f'   💣 Mines: {mines_stats["cnt"] if mines_stats else 0} игр | {fmt_profit(mines_p)}\n'
        f'   🎲 Dice: {cube_stats["cnt"] if cube_stats else 0} игр | {fmt_profit(cube_p)}\n'
        f'   🎡 X50: {x50_stats["cnt"] if x50_stats else 0} игр | {fmt_profit(x50_p)}\n'
        f'   📦 Cases: {cases_stats["cnt"] if cases_stats else 0} игр | {fmt_profit(cases_p)}\n'
        f'   🪙 Coinflip: {coinflip_stats["cnt"] if coinflip_stats else 0} игр | {fmt_profit(coinflip_p)}\n'
        f'   🎰 Slots: {slots_stats["cnt"] if slots_stats else 0} игр | {fmt_profit(slots_p)}\n\n'
        f'<b>💎 Общий профит: {fmt_profit(total_profit)}</b>'
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('Сегодня', callback_data='admin_stats_today'),
            InlineKeyboardButton('Неделя', callback_data='admin_stats_week'),
        ],
        [
            InlineKeyboardButton('Месяц', callback_data='admin_stats_month'),
            InlineKeyboardButton('Всё время', callback_data='admin_stats_all'),
        ],
        [InlineKeyboardButton('◀️ Назад', callback_data='admin_back')],
    ])

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — WITHDRAWALS
# ================================================================
async def admin_show_withdrawals(query, context):
    rows = query_all(
        "SELECT w.*, u.name as user_name FROM withdrawals w LEFT JOIN users u ON w.user_id = u.id WHERE w.status = 0 ORDER BY w.id ASC LIMIT 20"
    )

    if not rows:
        text = '<b>💳 Выплаты</b>\n\nНет ожидающих заявок ✅'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('◀️ Назад', callback_data='admin_back')],
        ])
        try:
            await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
        except Exception:
            await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)
        return

    text = f'<b>💳 Ожидающие выплаты ({len(rows)})</b>\n\n'

    buttons = []
    for r in rows:
        text += (
            f'<b>#{r["id"]}</b> | {r["user_name"] or "?"} (TG:{r["tg_id"]})\n'
            f'   💰 {int(r["amount"])} монет = ${float(r["amount_in_usdt"]):.2f}\n'
            f'   📅 {r["created_at"].strftime("%d.%m %H:%M") if r["created_at"] else "?"}\n\n'
        )
        buttons.append([
            InlineKeyboardButton(f'✅ #{r["id"]}', callback_data=f'admin_approve_{r["id"]}'),
            InlineKeyboardButton(f'❌ #{r["id"]}', callback_data=f'admin_reject_{r["id"]}'),
        ])

    buttons.append([InlineKeyboardButton('◀️ Назад', callback_data='admin_back')])
    keyboard = InlineKeyboardMarkup(buttons)

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — DEPOSITS
# ================================================================
async def admin_show_deposits(query, context):
    rows = query_all(
        "SELECT d.*, u.name as user_name FROM deposits d LEFT JOIN users u ON d.user_id = u.id ORDER BY d.id DESC LIMIT 20"
    )

    statuses = {0: '⏳ Ожидание', 1: '✅ Оплачен'}

    if not rows:
        text = '<b>💰 Депозиты</b>\n\nПока нет депозитов'
    else:
        text = f'<b>💰 Последние депозиты ({len(rows)})</b>\n\n'
        for r in rows:
            text += (
                f'<b>#{r["id"]}</b> | {r["user_name"] or "?"}\n'
                f'   ${float(r["amount_usd"]):.2f} → {int(r["amount_coins"])} монет\n'
                f'   {statuses.get(r["status"], "?")} | {r["created_at"].strftime("%d.%m %H:%M") if r["created_at"] else "?"}\n\n'
            )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('◀️ Назад', callback_data='admin_back')],
    ])

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — PROMOS
# ================================================================
async def admin_show_promos(query, context):
    rows = query_all("SELECT * FROM promocodes ORDER BY id DESC LIMIT 20")

    if not rows:
        text = '<b>🎁 Промокоды</b>\n\nНет промокодов'
    else:
        text = f'<b>🎁 Промокоды ({len(rows)})</b>\n\n'
        for r in rows:
            status = '🟢' if r['is_active'] else '🔴'
            text += (
                f'{status} <code>{r["code"]}</code>\n'
                f'   💰 {int(r["reward"])} монет | '
                f'👥 {r["current_uses"]}/{r["max_uses"]} | '
                f'🔄 x{float(r["wager_multiplier"])}\n\n'
            )

    buttons = []
    for r in rows:
        toggle_text = '🔴 Выкл' if r['is_active'] else '🟢 Вкл'
        buttons.append([
            InlineKeyboardButton(f'{toggle_text} {r["code"]}', callback_data=f'admin_promo_toggle_{r["id"]}'),
            InlineKeyboardButton(f'🗑 {r["code"]}', callback_data=f'admin_promo_del_{r["id"]}'),
        ])

    buttons.append([InlineKeyboardButton('➕ Создать промокод', callback_data='admin_promo_create')])
    buttons.append([InlineKeyboardButton('◀️ Назад', callback_data='admin_back')])

    keyboard = InlineKeyboardMarkup(buttons)

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — PROMO CREATE (callback)
# ================================================================
async def admin_promo_create_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    tg_id = query.from_user.id
    if tg_id not in ADMIN_IDS:
        return

    if query.data == 'admin_promo_create':
        await query.answer()
        context.user_data['admin_state'] = 'create_promo'
        await query.message.reply_text(
            '<b>➕ Создание промокода</b>\n\n'
            'Отправьте в формате:\n'
            '<code>КОД СУММА МАКС_ИСПОЛЬЗОВАНИЙ [ВЕЙДЖЕР_МНОЖИТЕЛЬ]</code>\n\n'
            'Примеры:\n'
            '<code>BONUS500 500 100</code>\n'
            '<code>VIP1000 1000 10 3.0</code>\n\n'
            '<i>Вейджер по умолчанию: x1.0</i>',
            parse_mode='HTML',
        )


# ================================================================
# ADMIN — BANKS
# ================================================================
async def admin_show_banks(query, context):
    banks = query_all("SELECT * FROM banks ORDER BY game")

    text = '<b>🏦 Банки (House Edge)</b>\n\n'

    for b in banks:
        bank_val = int(b['bank'])
        emoji = '🟢' if bank_val >= 0 else '🔴'
        text += (
            f'{emoji} <b>{b["game"].upper()}</b>\n'
            f'   Банк: {bank_val:+d}\n'
            f'   Мин: {int(b["min_bank"])} | Макс: {int(b["max_bank"])}\n\n'
        )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('🔄 Обновить', callback_data='admin_banks')],
        [InlineKeyboardButton('◀️ Назад', callback_data='admin_back')],
    ])

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — GAME SETTINGS
# ================================================================
async def admin_show_game_settings(query, context):
    settings = get_settings()
    if not settings:
        return

    games = [
        ('mines', '💣 Mines'),
        ('cube', '🎲 Dice'),
        ('x50', '🎡 X50'),
        ('cases_game', '📦 Cases'),
        ('coinflip', '🪙 Coinflip'),
        ('slots', '🎰 Slots'),
    ]

    text = '<b>⚙️ Настройки игр</b>\n\nНажмите чтобы вкл/выкл:\n'

    buttons = []
    for key, name in games:
        is_on = settings.get(key, 0)
        emoji = '🟢' if is_on else '🔴'
        status = 'ВКЛ' if is_on else 'ВЫКЛ'
        text += f'\n{emoji} {name}: <b>{status}</b>'
        buttons.append([
            InlineKeyboardButton(
                f'{"🔴 Выключить" if is_on else "🟢 Включить"} {name}',
                callback_data=f'admin_toggle_game_{key}'
            ),
        ])

    buttons.append([InlineKeyboardButton('◀️ Назад', callback_data='admin_back')])
    keyboard = InlineKeyboardMarkup(buttons)

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — FINANCIAL SETTINGS
# ================================================================
async def admin_show_fin_settings(query, context):
    settings = get_settings()
    if not settings:
        return

    dep_on = settings.get('deposits_enabled', 0)
    with_on = settings.get('withdrawals_enabled', 0)

    text = (
        '<b>💵 Финансовые настройки</b>\n\n'
        f'{"🟢" if dep_on else "🔴"} Депозиты: <b>{"ВКЛ" if dep_on else "ВЫКЛ"}</b>\n'
        f'{"🟢" if with_on else "🔴"} Выводы: <b>{"ВКЛ" if with_on else "ВЫКЛ"}</b>\n\n'
        f'💱 Курс: {settings.get("coins_per_usd", 100)} монет = $1\n'
        f'📥 Мин. депозит: ${float(settings.get("min_deposit_usd", 1)):.2f}\n'
        f'📥 Макс. депозит: ${float(settings.get("max_deposit_usd", 10000)):.2f}\n'
        f'📤 Мин. вывод: {settings.get("min_withdraw_coins", 500)} монет\n'
        f'📤 Макс. вывод: {settings.get("max_withdraw_coins", 1000000)} монет\n'
        f'👥 Реф. процент: {float(settings.get("ref_percent", 10))}%'
    )

    buttons = [
        [InlineKeyboardButton(
            f'{"🔴 Выкл" if dep_on else "🟢 Вкл"} депозиты',
            callback_data='admin_toggle_fin_deposits_enabled'
        )],
        [InlineKeyboardButton(
            f'{"🔴 Выкл" if with_on else "🟢 Вкл"} выводы',
            callback_data='admin_toggle_fin_withdrawals_enabled'
        )],
        [InlineKeyboardButton('◀️ Назад', callback_data='admin_back')],
    ]

    keyboard = InlineKeyboardMarkup(buttons)

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — USERS LIST
# ================================================================
async def admin_show_users(query, context):
    total = query_one("SELECT COUNT(*) as cnt FROM users")
    top_balance = query_all("SELECT id, name, user_id, balance FROM users ORDER BY balance DESC LIMIT 10")
    top_deposit = query_all("SELECT id, name, user_id, deposit FROM users ORDER BY deposit DESC LIMIT 10")

    text = f'<b>👥 Пользователи</b> (всего: {total["cnt"] if total else 0})\n\n'

    text += '<b>🏆 Топ по балансу:</b>\n'
    for i, u in enumerate(top_balance, 1):
        text += f'{i}. {u["name"] or "?"} — {int(u["balance"])} монет\n'

    text += '\n<b>💳 Топ по депозитам:</b>\n'
    for i, u in enumerate(top_deposit, 1):
        text += f'{i}. {u["name"] or "?"} — {int(u["deposit"])} монет\n'

    buttons = []
    for u in top_balance[:5]:
        buttons.append([
            InlineKeyboardButton(f'👤 {u["name"] or u["user_id"]}', callback_data=f'admin_user_{u["id"]}')
        ])

    buttons.append([InlineKeyboardButton('🔍 Найти юзера', callback_data='admin_find_user')])
    buttons.append([InlineKeyboardButton('◀️ Назад', callback_data='admin_back')])

    keyboard = InlineKeyboardMarkup(buttons)

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# ================================================================
# ADMIN — USER DETAIL
# ================================================================
async def admin_show_user_detail(query, context, uid):
    user = get_user_by_id(uid)
    if not user:
        try:
            await query.message.edit_text('❌ Пользователь не найден')
        except Exception:
            await query.message.reply_text('❌ Пользователь не найден')
        return

    total_games = 0
    for tbl, col in [('mines_bets', 'user_id'), ('cube_games', 'user_id'),
                      ('x50_bets', 'user_id'), ('cases_bets', 'user_id'),
                      ('coinflip_bets', 'user_id'), ('slots_bets', 'user_id')]:
        r = query_one(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE {col} = %s", [uid])
        if r:
            total_games += r['cnt']

    ban_text = '🔴 Забанен' if user['is_banned'] else '🟢 Активен'
    role_text = '👑 Админ' if user['role'] == 1 else '👤 Юзе��'

    text = (
        f'<b>👤 Пользователь #{user["id"]}</b>\n\n'
        f'📝 Имя: {user["name"]}\n'
        f'🆔 Telegram: {user["user_id"]}\n'
        f'💰 Баланс: {int(user["balance"])} монет\n'
        f'💳 Пополнено: {int(user["deposit"])} монет\n'
        f'📊 Поставлено: {int(user["total_wagered"])} монет\n'
        f'🔄 Вейджер: {int(user["wager"])} монет\n'
        f'🎮 Всего игр: {total_games}\n'
        f'👥 Рефералов: {user["refs"]}\n'
        f'💎 Реф. заработано: {int(user["ref_earned"])}\n'
        f'📅 Регистрация: {user["created_at"].strftime("%d.%m.%Y %H:%M") if user.get("created_at") else "?"}\n\n'
        f'Статус: {ban_text}\n'
        f'Роль: {role_text}'
    )

    ban_btn_text = '🟢 Разбанить' if user['is_banned'] else '🔴 Забанить'
    role_btn = '👤 Снять админа' if user['role'] == 1 else '👑 Дать админа'
    role_val = 0 if user['role'] == 1 else 1

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(ban_btn_text, callback_data=f'admin_ban_{uid}'),
            InlineKeyboardButton(role_btn, callback_data=f'admin_setrole_{uid}_{role_val}'),
        ],
        [
            InlineKeyboardButton('💰 Изменить баланс', callback_data=f'admin_addbal_{uid}'),
        ],
        [InlineKeyboardButton('◀️ Назад', callback_data='admin_users')],
    ])

    try:
        await query.message.edit_text(text, parse_mode='HTML', reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


async def admin_show_user_detail_msg(update, context, uid):
    user = get_user_by_id(uid)
    if not user:
        await update.message.reply_text('❌ Пользователь не найден')
        return

    total_games = 0
    for tbl, col in [('mines_bets', 'user_id'), ('cube_games', 'user_id'),
                      ('x50_bets', 'user_id'), ('cases_bets', 'user_id'),
                      ('coinflip_bets', 'user_id'), ('slots_bets', 'user_id')]:
        r = query_one(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE {col} = %s", [uid])
        if r:
            total_games += r['cnt']

    ban_text = '🔴 Забанен' if user['is_banned'] else '🟢 Активен'
    role_text = '👑 Админ' if user['role'] == 1 else '👤 Юзер'

    text = (
        f'<b>👤 Пользователь #{user["id"]}</b>\n\n'
        f'📝 Имя: {user["name"]}\n'
        f'🆔 Telegram: {user["user_id"]}\n'
        f'💰 Баланс: {int(user["balance"])} монет\n'
        f'💳 Пополнено: {int(user["deposit"])} монет\n'
        f'📊 Поставлено: {int(user["total_wagered"])} монет\n'
        f'🔄 Вейджер: {int(user["wager"])} монет\n'
        f'🎮 Всего игр: {total_games}\n'
        f'👥 Рефералов: {user["refs"]}\n'
        f'💎 Реф. заработано: {int(user["ref_earned"])}\n'
        f'📅 Регистрация: {user["created_at"].strftime("%d.%m.%Y %H:%M") if user.get("created_at") else "?"}\n\n'
        f'Статус: {ban_text}\n'
        f'Роль: {role_text}'
    )

    ban_btn_text = '🟢 Разбанить' if user['is_banned'] else '🔴 Забанить'
    role_btn = '👤 Снять админа' if user['role'] == 1 else '👑 Дать админа'
    role_val = 0 if user['role'] == 1 else 1

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(ban_btn_text, callback_data=f'admin_ban_{uid}'),
            InlineKeyboardButton(role_btn, callback_data=f'admin_setrole_{uid}_{role_val}'),
        ],
        [
            InlineKeyboardButton('💰 Изменить баланс', callback_data=f'admin_addbal_{uid}'),
        ],
        [InlineKeyboardButton('◀️ Назад', callback_data='admin_users')],
    ])

    await update.message.reply_text(text, parse_mode='HTML', reply_markup=keyboard)


# === ��ОНЕЦ ЧАСТИ 8/12 ===
# Следующая часть: main() — запуск всего (Flask + Socket.IO + Telegram Bot)
# ============================================================
# app.py — Часть 9/12: main() — запуск всего
# ============================================================
# ================================================================
# NEW FEATURES — LEVEL, CASHBACK, LEADERBOARD, ADMIN API, EVENTS
# ================================================================

# ================================================================
# LEVEL SYSTEM
# ================================================================
LEVEL_XP_MULTIPLIER = 15  # XP needed = level * 15

def get_user_level_info(user_id):
    """Получить уровень и опыт пользователя"""
    user = get_user_by_id(user_id)
    if not user:
        return {'level': 1, 'exp': 0, 'exp_needed': 15}
    total_wagered = int(user.get('total_wagered', 0))
    # 1 XP за каждые 100 монет поставленных
    total_xp = total_wagered // 100
    level = 1
    xp_used = 0
    while True:
        needed = level * LEVEL_XP_MULTIPLIER
        if total_xp - xp_used >= needed:
            xp_used += needed
            level += 1
        else:
            break
    current_exp = total_xp - xp_used
    exp_needed = level * LEVEL_XP_MULTIPLIER
    return {
        'level': level,
        'exp': current_exp,
        'exp_needed': exp_needed,
        'total_xp': total_xp,
    }


def add_xp_for_bet(user_id, bet_amount):
    """Добавить XP за ставку (вызывается после process_wager)"""
    info_before = get_user_level_info(user_id)
    # XP добавляется через total_wagered, который уже обновлён
    info_after = get_user_level_info(user_id)
    level_up = info_after['level'] > info_before['level']
    return {
        'level': info_after['level'],
        'exp': info_after['exp'],
        'exp_needed': info_after['exp_needed'],
        'level_up': level_up,
    }


# ================================================================
# CASHBACK SYSTEM
# ================================================================
def get_cashback_amount(user_id):
    """Кэшбэк = 1% от проигрышей за последние 24 часа"""
    # Сумма ставок за 24ч
    bets_24h = 0
    wins_24h = 0

    tables = [
        ("SELECT COALESCE(SUM(bet),0) as b FROM mines_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)", 
         "SELECT COALESCE(SUM(CASE WHEN result>0 THEN result ELSE 0 END),0) as w FROM mines_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)"),
        ("SELECT COALESCE(SUM(bet),0) as b FROM cube_games WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)",
         "SELECT COALESCE(SUM(win_amount),0) as w FROM cube_games WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)"),
        ("SELECT COALESCE(SUM(bet),0) as b FROM coinflip_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)",
         "SELECT COALESCE(SUM(win_amount),0) as w FROM coinflip_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)"),
        ("SELECT COALESCE(SUM(bet),0) as b FROM slots_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)",
         "SELECT COALESCE(SUM(win_amount),0) as w FROM slots_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)"),
        ("SELECT COALESCE(SUM(bet),0) as b FROM x50_bets WHERE user_id=%s AND bet>0 AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)",
         "SELECT COALESCE(SUM(result),0) as w FROM x50_bets WHERE user_id=%s AND bet>0 AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)"),
        ("SELECT COALESCE(SUM(bet),0) as b FROM cases_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)",
         "SELECT COALESCE(SUM(win_amount),0) as w FROM cases_bets WHERE user_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)"),
    ]

    for bet_sql, win_sql in tables:
        b = query_one(bet_sql, [user_id])
        w = query_one(win_sql, [user_id])
        bets_24h += int(b['b']) if b else 0
        wins_24h += int(w['w']) if w else 0

    losses = max(0, bets_24h - wins_24h)
    cashback = int(losses * 0.01)  # 1%
    return cashback


# ================================================================
# LEADERBOARD
# ================================================================
def get_leaderboard(lb_type='balance', limit=20):
    if lb_type == 'balance':
        rows = query_all(
            "SELECT id, user_id, name, balance, total_wagered FROM users ORDER BY balance DESC LIMIT %s",
            [limit]
        )
    elif lb_type == 'wagered':
        rows = query_all(
            "SELECT id, user_id, name, balance, total_wagered FROM users ORDER BY total_wagered DESC LIMIT %s",
            [limit]
        )
    elif lb_type == 'level':
        rows = query_all(
            "SELECT id, user_id, name, balance, total_wagered FROM users ORDER BY total_wagered DESC LIMIT %s",
            [limit]
        )
    else:
        rows = []

    result = []
    for r in rows:
        lvl_info = get_user_level_info(r['id'])
        result.append({
            'user_id': r['user_id'],
            'name': r['name'],
            'balance': int(r['balance']),
            'level': lvl_info['level'],
            'total_wagered': int(r['total_wagered']),
        })
    return result


# ================================================================
# EVENTS SYSTEM
# ================================================================
active_event = None  # Хранится в памяти


def get_active_event():
    global active_event
    if not active_event:
        return None
    # Проверяем, не истекло ли время
    if active_event.get('expires_at'):
        if datetime.now() > active_event['expires_at']:
            active_event = None
            return None
    return active_event


def create_event(name, description, hours=24, color='red', photo=None):
    global active_event
    active_event = {
        'name': name,
        'description': description,
        'hours': hours,
        'color': color,
        'photo': photo,
        'expires_at': datetime.now() + timedelta(hours=hours),
        'created_at': datetime.now().isoformat(),
    }
    return active_event


# ================================================================
# EXTENDED MESSAGE HANDLER — NEW TYPES
# ================================================================
def handle_extended_messages(data, user_id, sid, msg_type):
    """Обработка новых типов сообщений"""

    if msg_type == 'getLeaderboard':
        lb_type = data.get('lb_type', 'balance')
        limit = min(int(data.get('limit', 20)), 50)
        result = get_leaderboard(lb_type, limit)
        sio.emit('message', {
            'type': 'leaderboardData',
            'lb_type': lb_type,
            'leaderboard': result,
        }, room=sid)

    elif msg_type == 'claimCashback':
        user = get_user_by_id(user_id)
        if not user:
            return emit_alert(sid, 'error', 'Пользователь не найден')
        cashback = get_cashback_amount(user_id)
        if cashback <= 0:
            return emit_alert(sid, 'error', 'Кэшбэк пока недоступен. Играйте больше!')
        # Проверяем, не забирал ли уже сегодня
        last_cb = query_one(
            "SELECT id FROM transactions WHERE user_id=%s AND type='cashback' AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR) LIMIT 1",
            [user_id]
        )
        if last_cb:
            return emit_alert(sid, 'error', 'Кэшбэк можно забирать раз в 24 часа')
        query_exec("UPDATE users SET balance = balance + %s WHERE id = %s", [cashback, user_id])
        user_after = get_user_by_id(user_id)
        new_balance = int(user_after['balance']) if user_after else 0
        add_transaction(user_id, 'cashback', cashback, new_balance, f'Кэшбэк: {cashback} монет')
        sio.emit('message', {
            'type': 'cashbackClaimed',
            'amount': cashback,
            'balance': new_balance,
        }, room=sid)
        emit_alert(sid, 'success', f'💸 Кэшбэк {cashback} монет зачислен!')

    elif msg_type == 'getLevelInfo':
        info = get_user_level_info(user_id)
        sio.emit('message', {
            'type': 'levelInfo',
            'level': info['level'],
            'exp': info['exp'],
            'exp_needed': info['exp_needed'],
        }, room=sid)

    elif msg_type == 'getEvent':
        event = get_active_event()
        sio.emit('message', {
            'type': 'eventData',
            'event': event,
        }, room=sid)

    elif msg_type == 'shareStats':
        user = get_user_by_id(user_id)
        if not user:
            return
        lvl = get_user_level_info(user_id)
        sio.emit('message', {
            'type': 'shareStatsData',
            'text': (
                f'🎰 Casino\n'
                f'⚡ Уровень {lvl["level"]}\n'
                f'💰 Баланс: {int(user["balance"])} монет\n'
                f'📊 Поставлено: {int(user["total_wagered"])} монет'
            ),
        }, room=sid)

    elif msg_type == 'getGameHistory':
        limit = min(int(data.get('limit', 20)), 50)
        history = []

        # Mines
        mines = query_all(
            "SELECT 'mines' as game, bet, result, created_at FROM mines_bets WHERE user_id=%s ORDER BY id DESC LIMIT %s",
            [user_id, limit]
        )
        for m in mines:
            history.append({
                'game': 'mines', 'game_name': '💣 Mines',
                'bet': int(m['bet']),
                'result': 'win' if int(m['result']) > 0 else 'loss',
                'amount': int(m['result']) if int(m['result']) > 0 else -int(m['bet']),
                'date': m['created_at'].strftime('%d.%m %H:%M') if m['created_at'] else '',
            })

        # Dice
        dice = query_all(
            "SELECT 'dice' as game, bet, win_amount, created_at FROM cube_games WHERE user_id=%s ORDER BY id DESC LIMIT %s",
            [user_id, limit]
        )
        for d in dice:
            history.append({
                'game': 'dice', 'game_name': '🎲 Dice',
                'bet': int(d['bet']),
                'result': 'win' if int(d['win_amount']) > 0 else 'loss',
                'amount': int(d['win_amount']) if int(d['win_amount']) > 0 else -int(d['bet']),
                'date': d['created_at'].strftime('%d.%m %H:%M') if d['created_at'] else '',
            })

        # Coinflip
        cf = query_all(
            "SELECT 'coinflip' as game, bet, win_amount, created_at FROM coinflip_bets WHERE user_id=%s ORDER BY id DESC LIMIT %s",
            [user_id, limit]
        )
        for c in cf:
            history.append({
                'game': 'coinflip', 'game_name': '🪙 Coinflip',
                'bet': int(c['bet']),
                'result': 'win' if int(c['win_amount']) > 0 else 'loss',
                'amount': int(c['win_amount']) if int(c['win_amount']) > 0 else -int(c['bet']),
                'date': c['created_at'].strftime('%d.%m %H:%M') if c['created_at'] else '',
            })

        # Slots
        sl = query_all(
            "SELECT 'slots' as game, bet, win_amount, created_at FROM slots_bets WHERE user_id=%s ORDER BY id DESC LIMIT %s",
            [user_id, limit]
        )
        for s in sl:
            history.append({
                'game': 'slots', 'game_name': '🎰 Slots',
                'bet': int(s['bet']),
                'result': 'win' if int(s['win_amount']) > 0 else 'loss',
                'amount': int(s['win_amount']) if int(s['win_amount']) > 0 else -int(s['bet']),
                'date': s['created_at'].strftime('%d.%m %H:%M') if s['created_at'] else '',
            })

        # X50
        x5 = query_all(
            "SELECT 'x50' as game, bet, result, created_at FROM x50_bets WHERE user_id=%s AND bet>0 ORDER BY id DESC LIMIT %s",
            [user_id, limit]
        )
        for x in x5:
            history.append({
                'game': 'x50', 'game_name': '🎡 X50',
                'bet': int(x['bet']),
                'result': 'win' if int(x['result']) > 0 else 'loss',
                'amount': int(x['result']) if int(x['result']) > 0 else -int(x['bet']),
                'date': x['created_at'].strftime('%d.%m %H:%M') if x['created_at'] else '',
            })

        # Cases
        cs = query_all(
            "SELECT 'cases' as game, bet, win_amount, created_at FROM cases_bets WHERE user_id=%s ORDER BY id DESC LIMIT %s",
            [user_id, limit]
        )
        for c in cs:
            history.append({
                'game': 'cases', 'game_name': '📦 Cases',
                'bet': int(c['bet']),
                'result': 'win' if int(c['win_amount']) > int(c['bet']) else 'loss',
                'amount': int(c['win_amount']) - int(c['bet']),
                'date': c['created_at'].strftime('%d.%m %H:%M') if c['created_at'] else '',
            })

        # Sort by date descending
        history.sort(key=lambda x: x['date'], reverse=True)
        history = history[:limit]

        sio.emit('message', {
            'type': 'gameHistory',
            'history': history,
        }, room=sid)

    # ===== ADMIN API (через socket) =====
    elif msg_type == 'adminGiveBalance':
        user = get_user_by_id(user_id)
        if not user or user['role'] != 1:
            return emit_alert(sid, 'error', 'Нет доступа')
        target_tg = data.get('target_tg_id', 0)
        amount = rounded_int(data.get('amount', 0))
        if not target_tg or not amount:
            return emit_alert(sid, 'error', 'Укажите ID и сумму')
        target = get_user_by_tg(int(target_tg))
        if not target:
            return emit_alert(sid, 'error', 'Пользователь не найден')
        query_exec("UPDATE users SET balance = balance + %s WHERE id = %s", [amount, target['id']])
        target_after = get_user_by_id(target['id'])
        new_bal = int(target_after['balance']) if target_after else 0
        add_transaction(target['id'], 'admin_balance', amount, new_bal, f'Админ: {amount:+d}')
        emit_balance(target['id'])
        emit_alert(sid, 'success', f'Баланс {target["name"]}: {amount:+d} → {new_bal}')

    elif msg_type == 'adminCreateEvent':
        user = get_user_by_id(user_id)
        if not user or user['role'] != 1:
            return emit_alert(sid, 'error', 'Нет доступа')
        name = data.get('name', '')
        desc = data.get('description', '')
        hours = int(data.get('hours', 24))
        color = data.get('color', 'red')
        photo = data.get('photo', None)
        if not name:
            return emit_alert(sid, 'error', 'Укажите название')
        ev = create_event(name, desc, hours, color, photo)
        broadcast_all('message', {'type': 'eventData', 'event': ev})
        emit_alert(sid, 'success', f'Событие "{name}" создано!')

    elif msg_type == 'adminCreatePromo':
        user = get_user_by_id(user_id)
        if not user or user['role'] != 1:
            return emit_alert(sid, 'error', 'Нет доступа')
        code = str(data.get('code', '')).strip().upper()
        if not code:
            code = get_random_string(8).upper()
        reward = rounded_int(data.get('reward', 0))
        max_uses = rounded_int(data.get('max_uses', 1))
        wager_mult = float(data.get('wager_multiplier', 1.0))
        if reward <= 0:
            return emit_alert(sid, 'error', 'Укажите сумму')
        existing = query_one("SELECT id FROM promocodes WHERE code=%s", [code])
        if existing:
            return emit_alert(sid, 'error', 'Код уже существует')
        query_exec(
            "INSERT INTO promocodes (code, reward, max_uses, wager_multiplier, is_active) VALUES (%s,%s,%s,%s,1)",
            [code, reward, max_uses, wager_mult]
        )
        sio.emit('message', {
            'type': 'adminPromoCreated',
            'code': code,
            'reward': reward,
            'max_uses': max_uses,
        }, room=sid)
        emit_alert(sid, 'success', f'Промокод {code}: {reward} монет, макс: {max_uses}')

    elif msg_type == 'adminGetStats':
        user = get_user_by_id(user_id)
        if not user or user['role'] != 1:
            return emit_alert(sid, 'error', 'Нет доступа')
        total_users = query_one("SELECT COUNT(*) as c FROM users")
        total_deps = query_one("SELECT COUNT(*) as c, COALESCE(SUM(amount_coins),0) as t FROM deposits WHERE status=1")
        total_withs = query_one("SELECT COUNT(*) as c, COALESCE(SUM(amount),0) as t FROM withdrawals WHERE status=1")
        pending_withs = query_one("SELECT COUNT(*) as c, COALESCE(SUM(amount),0) as t FROM withdrawals WHERE status=0")
        banks = query_all("SELECT * FROM banks")
        sio.emit('message', {
            'type': 'adminStats',
            'users': total_users['c'] if total_users else 0,
            'online': len(connected_users),
            'deposits_count': total_deps['c'] if total_deps else 0,
            'deposits_total': int(total_deps['t']) if total_deps else 0,
            'withdrawals_count': total_withs['c'] if total_withs else 0,
            'withdrawals_total': int(total_withs['t']) if total_withs else 0,
            'pending_count': pending_withs['c'] if pending_withs else 0,
            'pending_total': int(pending_withs['t']) if pending_withs else 0,
            'banks': [{'game': b['game'], 'bank': int(b['bank'])} for b in banks],
        }, room=sid)

    else:
        return False  # не обработано

    return True  # обработано
# ================================================================
# MAIN — ENTRY POINT
# ================================================================
# ================================================================
# MAIN — ENTRY POINT
# ================================================================
def run_telegram_bot_process():
    """Запуск Telegram бота в отдельном процессе (не потоке!)"""
    import asyncio

    async def start_bot():
        application = Application.builder().token(TOKEN).build()

        application.add_handler(CommandHandler("start", cmd_start))
        application.add_handler(CommandHandler("help", cmd_help))
        application.add_handler(CommandHandler("balance", cmd_balance))
        application.add_handler(CommandHandler("ref", cmd_ref))
        application.add_handler(CommandHandler("admin", cmd_admin))

        application.add_handler(CallbackQueryHandler(admin_promo_create_callback, pattern='^admin_promo_create$'))
        application.add_handler(CallbackQueryHandler(admin_callback, pattern='^admin_'))

        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            admin_text_handler
        ))

        logger.info("Telegram bot starting polling...")
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )
        logger.info("Telegram bot polling started OK")

        # Держим бота запущенным
        try:
            while True:
                await asyncio.sleep(3600)
        except (KeyboardInterrupt, SystemExit):
            await application.updater.stop()
            await application.stop()
            await application.shutdown()

    try:
        asyncio.run(start_bot())
    except Exception as e:
        logger.error(f"Telegram bot error: {e}")


def main():
    logger.info("=" * 60)
    logger.info("CASINO BOT — STARTING")
    logger.info("=" * 60)

    # 1. Инициализация БД
    init_db()
    logger.info("Database ready")

    # 2. Инициализация X50
    init_x50_on_startup()
    logger.info("X50 game state loaded")

    # 3. Создание папки аватарок
    ensure_avatar_dir()

    # 4. Telegram Bot — в отдельном ПРОЦЕССЕ (не потоке!)
    # eventlet ломает asyncio в потоках, поэтому используем multiprocessing
    import multiprocessing
    bot_process = multiprocessing.Process(target=run_telegram_bot_process, daemon=True)
    bot_process.start()
    logger.info("Telegram bot process started")

    # 5. Flask + Socket.IO (main thread)
    logger.info(f"Starting Socket.IO server on 0.0.0.0:80")
    logger.info(f"Site URL: {SITE_URL}")
    logger.info(f"Webhook: {SITE_URL}/api/cryptobot_webhook")
    logger.info("=" * 60)

    eventlet.wsgi.server(
        eventlet.listen(('0.0.0.0', 80)),
        app,
        log_output=False,
    )


if __name__ == '__main__':
    main()