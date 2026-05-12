#!/usr/bin/env python3
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from database import (
    create_fetch_run,
    finish_fetch_run,
    get_active_products_count,
    get_active_product_keys,
    get_error_snapshots_for_run_window,
    get_latest_success_snapshots,
    get_latest_fetch_run,
    get_latest_successful_fetch_run,
    get_previous_success_snapshot,
    get_success_snapshots_for_product_since,
    init_db,
    is_user_allowed,
    save_price_snapshots,
    sync_allowed_users,
)
from charts import build_price_chart
from products import PRODUCTS, REQUEST_HEADERS, REQUEST_TIMEOUT_SECONDS


LOGS_DIR = "Logs"
LOG_FILE = os.path.join(LOGS_DIR, "logs.log")
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 5
DEFAULT_SCHEDULED_FETCH_TIMES = "09:00,18:00"
DEFAULT_SCHEDULE_TIMEZONE = "Europe/Moscow"
CURRENT_PRICES_BUTTON = "Текущие цены"
LATEST_MEASUREMENTS_BUTTON = "Последние замеры"
CHART_BUTTON = "График"
SUMMARY_BUTTON = "Сводка"
STATUS_BUTTON = "Статус"
ACCESS_DENIED_MESSAGE = "Доступ запрещен"

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            RotatingFileHandler(
                LOG_FILE,
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding="utf-8",
            ),
        ],
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def get_scheduled_fetch_times() -> list[time]:
    raw_times = os.getenv("SCHEDULED_FETCH_TIMES", DEFAULT_SCHEDULED_FETCH_TIMES)
    timezone_name = os.getenv("SCHEDULE_TIMEZONE", DEFAULT_SCHEDULE_TIMEZONE)

    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        logger.warning(
            "Schedule timezone not found timezone=%s, using default timezone=%s",
            timezone_name,
            DEFAULT_SCHEDULE_TIMEZONE,
        )
        timezone = ZoneInfo(DEFAULT_SCHEDULE_TIMEZONE)

    scheduled_times = []
    for raw_time in raw_times.split(","):
        raw_time = raw_time.strip()
        if not raw_time:
            continue

        try:
            hour, minute = raw_time.split(":", maxsplit=1)
            scheduled_times.append(time(hour=int(hour), minute=int(minute), tzinfo=timezone))
        except ValueError:
            logger.warning("Invalid scheduled fetch time skipped value=%s", raw_time)

    if scheduled_times:
        return scheduled_times

    logger.warning("No valid scheduled fetch times configured, using defaults=%s", DEFAULT_SCHEDULED_FETCH_TIMES)
    return [
        time(hour=9, minute=0, tzinfo=timezone),
        time(hour=18, minute=0, tzinfo=timezone),
    ]


def get_initial_allowed_user_ids() -> list[int]:
    raw_user_ids = os.getenv("ALLOWED_TELEGRAM_USER_IDS", "")
    user_ids = []
    for raw_user_id in raw_user_ids.split(","):
        raw_user_id = raw_user_id.strip()
        if not raw_user_id:
            continue

        try:
            user_ids.append(int(raw_user_id))
        except ValueError:
            logger.warning("Invalid Telegram user id skipped value=%s", raw_user_id)

    return user_ids


class AsyncParsing:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch_product_price(self, product):
        await asyncio.sleep(1)
        product_key = product["key"]
        product_title = product["title"]
        fetched_at = datetime.now().isoformat(timespec="seconds")

        try:
            params = {"codes": product["codes"]}

            async with self.session.get(
                product["api_url"],
                params=params,
                headers=REQUEST_HEADERS,
            ) as response:
                if response.status != 200:
                    logger.warning(
                        "HTTP error while fetching product key=%s title=%s status=%s url=%s",
                        product_key,
                        product_title,
                        response.status,
                        product["api_url"],
                    )
                    return self._error_result(
                        product,
                        fetched_at,
                        f"HTTP ошибка {response.status} при получении цены",
                    )

                text = await response.text()
                price = self._extract_price(text, product)
                numeric_price = self._parse_price(price)
                if numeric_price is None:
                    logger.warning(
                        "Price cannot be converted to number product key=%s title=%s raw_price=%s",
                        product_key,
                        product_title,
                        price,
                    )
                    return self._error_result(
                        product,
                        fetched_at,
                        f"Некорректный формат цены: {price}",
                    )

                logger.info(
                    "Successfully fetched price for product key=%s title=%s price=%s",
                    product_key,
                    product_title,
                    numeric_price,
                )
                return self._success_result(product, fetched_at, numeric_price, price)
        except asyncio.TimeoutError:
            logger.warning(
                "Timeout while fetching product key=%s title=%s timeout_seconds=%s url=%s",
                product_key,
                product_title,
                REQUEST_TIMEOUT_SECONDS,
                product["api_url"],
            )
            return self._error_result(product, fetched_at, "Timeout при получении цены")
        except aiohttp.ClientError:
            logger.exception(
                "Connection error while fetching product key=%s title=%s url=%s",
                product_key,
                product_title,
                product["api_url"],
            )
            return self._error_result(product, fetched_at, "Ошибка соединения при получении цены")
        except json.JSONDecodeError:
            logger.exception(
                "JSON parsing error for product key=%s title=%s",
                product_key,
                product_title,
            )
            return self._error_result(product, fetched_at, "Ошибка разбора JSON-ответа")
        except IndexError:
            logger.exception(
                "Price index is out of range for product key=%s title=%s price_index=%s",
                product_key,
                product_title,
                product.get("price_index"),
            )
            return self._error_result(product, fetched_at, "Индекс цены отсутствует в ответе")
        except KeyError:
            logger.exception(
                "Missing field while parsing product key=%s title=%s price_path=%s",
                product_key,
                product_title,
                product.get("price_path"),
            )
            return self._error_result(product, fetched_at, "Нужное поле цены отсутствует в ответе")
        except ValueError:
            logger.exception(
                "Unexpected response format for product key=%s title=%s parser_type=%s",
                product_key,
                product_title,
                product.get("parser_type"),
            )
            return self._error_result(product, fetched_at, "Неожиданный формат ответа")
        except Exception:
            logger.exception(
                "Unexpected error while fetching product key=%s title=%s",
                product_key,
                product_title,
            )
            return self._error_result(product, fetched_at, "Неожиданная ошибка при получении цены")

    @staticmethod
    def _extract_price(text, product):
        response_prefix = product.get("response_prefix", "")
        if response_prefix and response_prefix not in text:
            raise ValueError(f"Response prefix not found: {response_prefix}")

        cleaned_text = text.replace(response_prefix, "").strip()

        if product["parser_type"] == "json":
            data = json.loads(cleaned_text)
            value = data
            for path_part in product["price_path"]:
                value = value[path_part]
            return value

        if product["parser_type"] == "csv":
            values = cleaned_text.split(",")
            return values[product["price_index"]]

        raise ValueError(f"Неизвестный тип парсинга: {product['parser_type']}")

    @staticmethod
    def _parse_price(price):
        if isinstance(price, (int, float)):
            return float(price)

        normalized_price = str(price).strip().replace(" ", "").replace(",", ".")
        try:
            return float(normalized_price)
        except ValueError:
            return None

    @staticmethod
    def _success_result(product, fetched_at, numeric_price, raw_price):
        display_time = datetime.fromisoformat(fetched_at).strftime("%d.%m.%Y %H:%M:%S")
        return {
            "key": product["key"],
            "title": product["title"],
            "price": numeric_price,
            "raw_price": raw_price,
            "fetched_at": fetched_at,
            "source_name": product.get("source_name"),
            "status": "success",
            "error_message": None,
            "user_message": (
                f"{display_time}\n"
                f'Название: {product["title"]}\n'
                f"Цена: {raw_price}\n"
            ),
        }

    @staticmethod
    def _error_result(product, fetched_at, error_message):
        return {
            "key": product["key"],
            "title": product["title"],
            "price": None,
            "raw_price": None,
            "fetched_at": fetched_at,
            "source_name": product.get("source_name"),
            "status": "error",
            "error_message": error_message,
            "user_message": f'Ошибка при получении {product["title"].lower()}. Попробуйте позже.',
        }


async def execute_fetch_run(run_type: str) -> list[dict]:
    products = get_active_products()
    logger.info("Starting fetch run run_type=%s total_products=%s", run_type, len(products))
    run_started_at = datetime.now().isoformat(timespec="seconds")
    run_id = None
    try:
        run_id = create_fetch_run(
            run_type=run_type,
            total_products=len(products),
            started_at=run_started_at,
        )
    except Exception:
        logger.exception("Failed to create fetch run run_type=%s", run_type)

    async with AsyncParsing() as parser:
        results = await asyncio.gather(
            *(parser.fetch_product_price(product) for product in products)
        )

    snapshots = [
        {
            "product_key": result["key"],
            "product_title": result["title"],
            "price": result["price"],
            "fetched_at": result["fetched_at"],
            "source_name": result["source_name"],
            "run_type": run_type,
            "status": result["status"],
            "error_message": result["error_message"],
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }
        for result in results
    ]

    success_count = sum(1 for result in results if result["status"] == "success")
    error_count = len(results) - success_count
    try:
        save_price_snapshots(snapshots)
        if run_id is not None:
            finish_fetch_run(
                run_id=run_id,
                finished_at=datetime.now().isoformat(timespec="seconds"),
                success_count=success_count,
                error_count=error_count,
                total_products=len(results),
            )
    except Exception:
        logger.exception("Failed to save fetch run history run_type=%s run_id=%s", run_type, run_id)

    logger.info(
        "Finished fetch run run_type=%s run_id=%s success_count=%s error_count=%s total_products=%s",
        run_type,
        run_id,
        success_count,
        error_count,
        len(results),
    )
    return results


def get_active_products() -> list[dict]:
    try:
        active_keys = get_active_product_keys()
    except Exception:
        logger.exception("Failed to load active products from database, using config products")
        return PRODUCTS

    products = [product for product in PRODUCTS if product["key"] in active_keys]
    logger.info("Loaded active products count=%s", len(products))
    return products


async def scheduled_fetch_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Scheduled fetch job started")
    try:
        await execute_fetch_run(run_type="scheduled")
        logger.info("Scheduled fetch job finished")
    except Exception:
        logger.exception("Scheduled fetch job failed")


def build_latest_measurements_message() -> str:
    latest_snapshots = get_latest_success_snapshots()
    lines = ["Последние замеры:"]

    for product in PRODUCTS:
        latest_snapshot = latest_snapshots.get(product["key"])
        lines.append("")
        lines.append(product["title"])

        if latest_snapshot is None:
            lines.append("Данные пока отсутствуют.")
            continue

        lines.append(f"Цена: {latest_snapshot['price']}")
        lines.append(f"Время: {format_snapshot_time(latest_snapshot['fetched_at'])}")

        previous_snapshot = get_previous_success_snapshot(product["key"], latest_snapshot["id"])
        if previous_snapshot is None:
            lines.append("Предыдущего замера нет.")
            continue

        lines.append(f"Изменение: {format_price_change(latest_snapshot['price'], previous_snapshot['price'])}")

    return "\n".join(lines)


def format_snapshot_time(value: str) -> str:
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y %H:%M:%S")
    except ValueError:
        return value


def format_price_change(current_price, previous_price) -> str:
    if current_price is None or previous_price is None:
        return "недостаточно данных"

    delta = current_price - previous_price
    if previous_price == 0:
        return f"{delta:+.2f}, процентное изменение недоступно"

    percent = delta / previous_price * 100
    return f"{delta:+.2f} ({percent:+.2f}%)"


def build_summary_message() -> str:
    latest_snapshots = get_latest_success_snapshots()
    lines = ["Сводка:"]

    for product in get_active_products():
        latest_snapshot = latest_snapshots.get(product["key"])
        lines.append("")
        lines.append(product["title"])

        if latest_snapshot is None:
            lines.append("Цена: данных пока нет")
            continue

        previous_snapshot = get_previous_success_snapshot(product["key"], latest_snapshot["id"])
        lines.append(f"Цена: {latest_snapshot['price']}")

        if previous_snapshot is None:
            lines.append("Изменение: предыдущего замера нет")
            continue

        change_label = get_change_label(latest_snapshot["price"], previous_snapshot["price"])
        lines.append(f"Изменение: {format_price_change(latest_snapshot['price'], previous_snapshot['price'])}")
        lines.append(f"Метка: {change_label}")

    return "\n".join(lines)


def build_status_message() -> str:
    latest_scheduled_run = get_latest_successful_fetch_run("scheduled")
    latest_manual_run = get_latest_fetch_run("manual")
    latest_run = get_latest_fetch_run()
    active_products_count = get_active_products_count()

    lines = [
        "Статус:",
        f"Последний успешный автосбор: {format_run_time(latest_scheduled_run)}",
        f"Последний ручной сбор: {format_run_time(latest_manual_run)}",
        f"Активных товаров: {active_products_count}",
    ]

    if latest_run is None:
        lines.extend([
            "Последний запуск: данных пока нет",
            "Успешно в последнем запуске: 0",
            "Ошибок в последнем запуске: 0",
        ])
        return "\n".join(lines)

    lines.extend([
        f"Последний запуск: {latest_run['run_type']} от {format_run_time(latest_run)}",
        f"Успешно в последнем запуске: {latest_run['success_count']}",
        f"Ошибок в последнем запуске: {latest_run['error_count']}",
    ])

    error_snapshots = get_error_snapshots_for_run_window(latest_run)
    if error_snapshots:
        problem_titles = []
        for snapshot in error_snapshots:
            title = snapshot["product_title"]
            if title not in problem_titles:
                problem_titles.append(title)
        lines.append("Проблемные товары: " + ", ".join(problem_titles[:5]))

    return "\n".join(lines)


def get_change_label(current_price, previous_price) -> str:
    if current_price is None or previous_price is None:
        return "недостаточно данных"

    delta = current_price - previous_price
    if delta > 0:
        return "рост"
    if delta < 0:
        return "снижение"
    return "без изменений"


def format_run_time(run: dict | None) -> str:
    if run is None:
        return "данных пока нет"
    return format_snapshot_time(run.get("finished_at") or run["started_at"])


class PriceBot:
    def __init__(self, token):
        self.token = token
        self.awaiting_chart_product_users = set()
        self.application = Application.builder().token(token).build()

        # Регистрация обработчиков
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self._schedule_fetch_jobs()

    def _schedule_fetch_jobs(self):
        job_queue = self.application.job_queue
        if job_queue is None:
            logger.error("JobQueue is not available. Install python-telegram-bot with job-queue extra.")
            raise RuntimeError("JobQueue is not available")

        for scheduled_time in get_scheduled_fetch_times():
            job_queue.run_daily(
                scheduled_fetch_job,
                time=scheduled_time,
                name=f"scheduled_fetch_{scheduled_time.strftime('%H_%M')}",
            )
            logger.info("Scheduled fetch job registered time=%s", scheduled_time.isoformat())

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._ensure_access(update):
            return

        keyboard = [
            [CURRENT_PRICES_BUTTON],
            [LATEST_MEASUREMENTS_BUTTON],
            [CHART_BUTTON],
            [SUMMARY_BUTTON, STATUS_BUTTON],
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "Нажмите кнопку ниже для получения актуальных цен:",
            reply_markup=reply_markup
        )

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._ensure_access(update):
            return

        if self._is_waiting_for_chart_product(update):
            await self.send_price_chart(update)
            return

        if update.message.text == CURRENT_PRICES_BUTTON:
            user = update.effective_user
            logger.info(
                "User requested prices user_id=%s username=%s",
                user.id if user else None,
                user.username if user else None,
            )
            await self.send_prices(update)
        elif update.message.text == LATEST_MEASUREMENTS_BUTTON:
            user = update.effective_user
            logger.info(
                "User requested latest measurements user_id=%s username=%s",
                user.id if user else None,
                user.username if user else None,
            )
            await self.send_latest_measurements(update)
        elif update.message.text == CHART_BUTTON:
            user = update.effective_user
            logger.info(
                "User requested chart product selection user_id=%s username=%s",
                user.id if user else None,
                user.username if user else None,
            )
            await self.ask_chart_product(update)
        elif update.message.text == SUMMARY_BUTTON:
            user = update.effective_user
            logger.info(
                "User requested summary user_id=%s username=%s",
                user.id if user else None,
                user.username if user else None,
            )
            await self.send_summary(update)
        elif update.message.text == STATUS_BUTTON:
            user = update.effective_user
            logger.info(
                "User requested status user_id=%s username=%s",
                user.id if user else None,
                user.username if user else None,
            )
            await self.send_status(update)

    async def send_prices(self, update: Update):
        results = await execute_fetch_run(run_type="manual")

        message = (
            "Актуальные цены:\n\n"
            + "\n".join(result["user_message"] for result in results)
        )

        await update.message.reply_text(message)

    async def send_latest_measurements(self, update: Update):
        try:
            message = build_latest_measurements_message()
        except Exception:
            logger.exception("Failed to build latest measurements message")
            message = "Не удалось получить последние замеры. Попробуйте позже."

        await update.message.reply_text(message)

    async def send_summary(self, update: Update):
        try:
            message = build_summary_message()
        except Exception:
            logger.exception("Failed to build summary message")
            message = "Не удалось сформировать сводку. Попробуйте позже."

        await update.message.reply_text(message)

    async def send_status(self, update: Update):
        try:
            message = build_status_message()
        except Exception:
            logger.exception("Failed to build status message")
            message = "Не удалось получить статус. Попробуйте позже."

        await update.message.reply_text(message)

    async def ask_chart_product(self, update: Update):
        user = update.effective_user
        if user:
            self.awaiting_chart_product_users.add(user.id)

        keyboard = [[product["title"]] for product in PRODUCTS]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("Выберите товар для графика:", reply_markup=reply_markup)

    async def send_price_chart(self, update: Update):
        user = update.effective_user
        if user:
            self.awaiting_chart_product_users.discard(user.id)

        product = self._find_product_by_title(update.message.text)
        if product is None:
            await update.message.reply_text("Не удалось найти товар. Нажмите «График» и выберите товар из списка.")
            return

        since = (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")
        chart_path = None
        try:
            snapshots = get_success_snapshots_for_product_since(product["key"], since)
            if len(snapshots) < 2:
                await update.message.reply_text(f'Недостаточно данных для графика по товару "{product["title"]}" за последние 7 дней.')
                return

            chart_path = build_price_chart(product["title"], snapshots)
            caption = f'График цены: {product["title"]} за последние 7 дней'
            logger.info(
                "Sending price chart user_id=%s product_key=%s points=%s",
                user.id if user else None,
                product["key"],
                len(snapshots),
            )
            with open(chart_path, "rb") as chart_file:
                await update.message.reply_photo(photo=chart_file, caption=caption)
        except Exception:
            logger.exception("Failed to build or send price chart product_key=%s", product["key"])
            await update.message.reply_text("Не удалось построить график. Попробуйте позже.")
        finally:
            if chart_path:
                try:
                    Path(chart_path).unlink(missing_ok=True)
                except OSError:
                    logger.exception("Failed to delete temporary chart file path=%s", chart_path)

    def _is_waiting_for_chart_product(self, update: Update) -> bool:
        user = update.effective_user
        return bool(user and user.id in self.awaiting_chart_product_users)

    @staticmethod
    def _find_product_by_title(title: str):
        return next((product for product in PRODUCTS if product["title"] == title), None)

    async def _ensure_access(self, update: Update) -> bool:
        user = update.effective_user
        if user is None:
            logger.warning("Access denied for update without effective user")
            if update.message:
                await update.message.reply_text(ACCESS_DENIED_MESSAGE)
            return False

        if is_user_allowed(user.id):
            logger.info("Access granted user_id=%s username=%s", user.id, user.username)
            return True

        logger.warning("Access denied user_id=%s username=%s", user.id, user.username)
        if update.message:
            await update.message.reply_text(ACCESS_DENIED_MESSAGE)
        return False

    def run(self):
        self.application.run_polling()


def main():
    setup_logging()
    logger.info("Bot starting")

    load_dotenv()
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN is not set")
        raise SystemExit("Не задан TELEGRAM_TOKEN в переменных окружения")

    init_db(PRODUCTS)
    sync_allowed_users(get_initial_allowed_user_ids())

    bot = PriceBot(TELEGRAM_TOKEN)
    logger.info("Bot started, polling is starting")
    bot.run()


if __name__ == "__main__":
    main()
