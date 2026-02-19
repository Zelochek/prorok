import os
import psycopg2
import asyncio
import logging
import json
import os
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import pytz
from collections import defaultdict
from pathlib import Path  # Добавляем для работы с путями

# Загружаем переменные из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# === ИЗМЕНЕНИЕ 1: Конфигурация через переменные окружения ===
# Токен бота берем из переменных окружения (будет в .env файле)
# Конфигурация бота
# Конфигурация бота
BOT_TOKEN = "8379585357:AAGHT8AuaK-OBH6qa2clWmTUJYuyCvL06xw"
MAIN_CREATOR_ID = 5349062051  # Главный создатель 

if not BOT_TOKEN:
    logger.error("BOT_TOKEN не найден!")
    raise ValueError("BOT_TOKEN не установлен")

# ID главного создателя тоже можно сделать настраиваемым
MAIN_CREATOR_ID = int(os.getenv('MAIN_CREATOR_ID', '5349062051'))

# === ИЗМЕНЕНИЕ 2: Настройка путей для Docker ===
# В контейнере данные будут храниться в /app/data
# Эта папка будет примонтирована с хоста через volume в docker-compose
#DATA_DIR = Path("/app/data")  # Путь в контейнере
# DATA_DIR = Path("data")  # Для локального теста (раскомментируйте при необходимости)
DATA_DIR = Path("data")
# Создаем папку для данных, если её нет
DATA_DIR.mkdir(exist_ok=True, parents=True)
logger.info(f"Директория для данных: {DATA_DIR}")

# === ИЗМЕНЕНИЕ 3: Пути к файлам данных ===
USERS_FILE = DATA_DIR / "users.json"
CREATORS_FILE = DATA_DIR / "creators.json"
BOOKINGS_FILE = DATA_DIR / "bookings.json"
TIME_SLOTS_FILE = DATA_DIR / "time_slots.json"

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Константа для максимального количества записей на слот
MAX_BOOKINGS_PER_SLOT = 9

# Состояния для FSM
class RegistrationState(StatesGroup):
    waiting_for_first_name = State()
    waiting_for_last_name = State()

class AddDateTimeState(StatesGroup):
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_description = State()

class AddCreatorState(StatesGroup):
    waiting_for_user_id = State()

# === ИЗМЕНЕНИЕ 4: Улучшенные функции загрузки/сохранения с обработкой ошибок ===
def load_data():
    """Загрузка всех данных из JSON файлов"""
    data = {
        "users": {},
        "creators": [],
        "bookings": [],
        "time_slots": []
    }
    
    # Загружаем пользователей
    if USERS_FILE.exists():
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                data["users"] = json.load(f)
            logger.info(f"Загружено {len(data['users'])} пользователей")
        except Exception as e:
            logger.error(f"Ошибка загрузки users.json: {e}")
            data["users"] = {}
    else:
        logger.info(f"Файл {USERS_FILE} не найден, будет создан при сохранении")
        data["users"] = {}
    
    # Загружаем создателей
    if CREATORS_FILE.exists():
        try:
            with open(CREATORS_FILE, 'r', encoding='utf-8') as f:
                data["creators"] = json.load(f)
            logger.info(f"Загружено {len(data['creators'])} создателей")
        except Exception as e:
            logger.error(f"Ошибка загрузки creators.json: {e}")
            data["creators"] = []
    else:
        data["creators"] = []
    
    # Загружаем записи
    if BOOKINGS_FILE.exists():
        try:
            with open(BOOKINGS_FILE, 'r', encoding='utf-8') as f:
                raw_bookings = json.load(f)
                # Конвертируем строки дат обратно в datetime
                data["bookings"] = []
                for booking in raw_bookings:
                    if isinstance(booking.get("selected_at"), str):
                        booking["selected_at"] = datetime.fromisoformat(booking["selected_at"])
                    data["bookings"].append(booking)
            logger.info(f"Загружено {len(data['bookings'])} записей")
        except Exception as e:
            logger.error(f"Ошибка загрузки bookings.json: {e}")
            data["bookings"] = []
    else:
        data["bookings"] = []
    
    # Загружаем слоты времени
    if TIME_SLOTS_FILE.exists():
        try:
            with open(TIME_SLOTS_FILE, 'r', encoding='utf-8') as f:
                raw_slots = json.load(f)
                # Конвертируем строки дат обратно в datetime
                data["time_slots"] = []
                for slot in raw_slots:
                    if isinstance(slot.get("created_at"), str):
                        slot["created_at"] = datetime.fromisoformat(slot["created_at"])
                    data["time_slots"].append(slot)
            logger.info(f"Загружено {len(data['time_slots'])} временных слотов")
        except Exception as e:
            logger.error(f"Ошибка загрузки time_slots.json: {e}")
            data["time_slots"] = []
    else:
        data["time_slots"] = []
    
    # Всегда добавляем главного создателя
    if MAIN_CREATOR_ID not in data["creators"]:
        data["creators"].append(MAIN_CREATOR_ID)
        save_creators(data["creators"])
    
    return data

def save_users(users):
    """Сохранить пользователей в JSON"""
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено {len(users)} пользователей")
    except Exception as e:
        logger.error(f"Ошибка сохранения users.json: {e}")

def save_creators(creators):
    """Сохранить создателей в JSON"""
    try:
        with open(CREATORS_FILE, 'w', encoding='utf-8') as f:
            json.dump(creators, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено {len(creators)} создателей")
    except Exception as e:
        logger.error(f"Ошибка сохранения creators.json: {e}")

def save_bookings(bookings):
    """Сохранить записи в JSON (конвертируя datetime в строки)"""
    try:
        # Конвертируем datetime в строки для JSON
        serializable_bookings = []
        for booking in bookings:
            booking_copy = booking.copy()
            if isinstance(booking_copy.get("selected_at"), datetime):
                booking_copy["selected_at"] = booking_copy["selected_at"].isoformat()
            serializable_bookings.append(booking_copy)
        
        with open(BOOKINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(serializable_bookings, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено {len(bookings)} записей")
    except Exception as e:
        logger.error(f"Ошибка сохранения bookings.json: {e}")

def save_time_slots(time_slots):
    """Сохранить временные слоты в JSON (конвертируя datetime в строки)"""
    try:
        # Конвертируем datetime в строки для JSON
        serializable_slots = []
        for slot in time_slots:
            slot_copy = slot.copy()
            if isinstance(slot_copy.get("created_at"), datetime):
                slot_copy["created_at"] = slot_copy["created_at"].isoformat()
            serializable_slots.append(slot_copy)
        
        with open(TIME_SLOTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(serializable_slots, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено {len(time_slots)} временных слотов")
    except Exception as e:
        logger.error(f"Ошибка сохранения time_slots.json: {e}")

# === ИЗМЕНЕНИЕ 5: Функция автосохранения ===
async def auto_save_periodically():
    """Автоматически сохранять данные каждые 5 минут"""
    while True:
        await asyncio.sleep(300)  # 5 минут
        save_users(users)
        save_creators(creators)
        save_bookings(user_time_selections)
        save_time_slots(available_datetimes)
        logger.info("Автосохранение данных выполнено")

# === ИЗМЕНЕНИЕ 6: Функция для проверки доступности слота ===
def is_slot_available(date_str, time_str):
    """Проверяет, есть ли свободные места на слоте"""
    booked_count = len([r for r in user_time_selections 
                       if r['date_str'] == date_str and r['time_str'] == time_str])
    return booked_count < MAX_BOOKINGS_PER_SLOT

def get_available_slots_count(date_str, time_str):
    """Возвращает количество свободных мест на слоте"""
    booked_count = len([r for r in user_time_selections 
                       if r['date_str'] == date_str and r['time_str'] == time_str])
    return MAX_BOOKINGS_PER_SLOT - booked_count

# Загружаем данные при старте
data = load_data()
users = data["users"]
creators = data["creators"]
user_time_selections = data["bookings"]
available_datetimes = data["time_slots"]

# Проверка, является ли пользователь создателем
def is_creator(user_id):
    return user_id in creators

def is_main_creator(user_id):
    return user_id == MAIN_CREATOR_ID

# Главное меню (для обычных пользователей)
def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 Записаться")],
            [KeyboardButton(text="📋 Мои записи")],
            [KeyboardButton(text="ℹ️ О боте")]
        ],
        resize_keyboard=True
    )
    return keyboard

# Главное меню для создателя
def get_creator_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 Записаться")],
            [KeyboardButton(text="➕ Добавить время")],
            [KeyboardButton(text="📋 Все записи")],
            [KeyboardButton(text="👥 Пользователи")],
            [KeyboardButton(text="👑 Создатели")],
            [KeyboardButton(text="👁️ Кто записался")],
            [KeyboardButton(text="🗑️ Управление временем")],
            [KeyboardButton(text="ℹ️ О боте")]
        ],
        resize_keyboard=True
    )
    return keyboard

# Клавиатура для регистрации
def get_registration_keyboard():
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="📝 Зарегистрироваться", callback_data="register")
    keyboard.button(text="ℹ️ Зачем регистрация?", callback_data="why_register")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для управления пользователями
def get_users_management_keyboard():
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="📊 Статистика пользователей", callback_data="users_stats")
    keyboard.button(text="👁️ Просмотр всех пользователей", callback_data="view_all_users_list")
    keyboard.button(text="🔍 Поиск пользователя", callback_data="search_user")
    keyboard.button(text="◀️ Назад", callback_data="back_to_creator_menu_from_users")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для управления создателями
def get_creators_management_keyboard():
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="👑 Все создатели", callback_data="view_all_creators")
    keyboard.button(text="➕ Добавить создателя", callback_data="add_creator")
    keyboard.button(text="🗑️ Удалить создателя", callback_data="remove_creator")
    keyboard.button(text="◀️ Назад", callback_data="back_to_creator_menu_from_creators")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для управления временем
def get_time_management_keyboard():
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="👁️ Просмотр всех слотов", callback_data="view_all_slots")
    keyboard.button(text="🗑️ Удалить слот", callback_data="delete_slot")
    keyboard.button(text="🧹 Очистить все слоты", callback_data="clear_all_slots")
    keyboard.button(text="◀️ Назад", callback_data="back_to_creator_menu_from_time")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для просмотра кто записался
def get_who_booked_keyboard():
    keyboard = InlineKeyboardBuilder()
    
    # Группируем записи по датам
    dates_with_bookings = set()
    for record in user_time_selections:
        dates_with_bookings.add(record['date_str'])
    
    if not dates_with_bookings:
        keyboard.button(text="📭 Нет записей", callback_data="no_bookings")
    else:
        for date_str in sorted(dates_with_bookings):
            # Считаем записи на эту дату
            bookings_count = len([r for r in user_time_selections if r['date_str'] == date_str])
            keyboard.button(text=f"📅 {date_str} ({bookings_count})", callback_data=f"view_date_{date_str}")
    
    keyboard.button(text="📋 Все записи по времени", callback_data="view_all_by_time")
    keyboard.button(text="👥 Все пользователи", callback_data="view_all_users_booking")
    keyboard.button(text="◀️ Назад", callback_data="back_to_creator_menu_from_who")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для выбора даты из доступных
def get_available_dates_keyboard():
    keyboard = InlineKeyboardBuilder()
    
    # Группируем доступные даты
    dates = set()
    for dt in available_datetimes:
        dates.add(dt['date_str'])
    
    if not dates:
        keyboard.button(text="📭 Нет доступных дат", callback_data="no_dates")
    else:
        for date_str in sorted(dates):
            # Считаем количество времен на эту дату
            times_count = len([dt for dt in available_datetimes if dt['date_str'] == date_str])
            keyboard.button(text=f"📅 {date_str} ({times_count})", callback_data=f"select_date_{date_str}")
    
    keyboard.button(text="◀️ Назад", callback_data="back_to_main_menu_from_dates")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для выбора времени на выбранную дату
def get_available_times_keyboard(selected_date_str):
    keyboard = InlineKeyboardBuilder()
    
    # Получаем все времена для этой даты
    times_for_date = [dt for dt in available_datetimes if dt['date_str'] == selected_date_str]
    
    if not times_for_date:
        keyboard.button(text="🕐 Нет доступного времени", callback_data="no_times")
    else:
        for dt_item in sorted(times_for_date, key=lambda x: x['time_str']):
            # Считаем сколько уже записалось на это время
            booked_count = len([record for record in user_time_selections 
                              if record['date_str'] == selected_date_str 
                              and record['time_str'] == dt_item['time_str']])
            
            # Показываем время и количество записавшихся
            text = f"🕐 {dt_item['time_str']}"
            if dt_item.get('description'):
                text += f" - {dt_item['description']}"
            if booked_count > 0:
                text += f" ({booked_count} чел.)"
            
            keyboard.button(text=text, callback_data=f"select_time_{selected_date_str}_{dt_item['time_str']}")
    
    keyboard.button(text="◀️ Назад к датам", callback_data="back_to_dates_from_time")
    keyboard.button(text="🏠 В главное меню", callback_data="back_to_main_menu_from_time")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для выбора времени на конкретную дату (для просмотра записей)
def get_time_for_date_keyboard(selected_date_str):
    keyboard = InlineKeyboardBuilder()
    
    # Получаем все времена с записями на эту дату
    times_with_bookings = set()
    for record in user_time_selections:
        if record['date_str'] == selected_date_str:
            times_with_bookings.add(record['time_str'])
    
    if not times_with_bookings:
        keyboard.button(text="🕐 Нет записей", callback_data="no_bookings_for_date")
    else:
        for time_str in sorted(times_with_bookings):
            # Считаем записи на это время
            bookings_count = len([r for r in user_time_selections 
                                if r['date_str'] == selected_date_str 
                                and r['time_str'] == time_str])
            
            # Находим описание слота
            description = ""
            for dt_item in available_datetimes:
                if dt_item['date_str'] == selected_date_str and dt_item['time_str'] == time_str:
                    description = dt_item.get('description', '')
                    break
            
            text = f"🕐 {time_str}"
            if description:
                text += f" - {description}"
            text += f" ({bookings_count} чел.)"
            
            keyboard.button(text=text, callback_data=f"view_time_{selected_date_str}_{time_str}")
    
    keyboard.button(text="◀️ Назад к датам", callback_data="back_to_who_booked_from_date")
    keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu_from_date")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Клавиатура для удаления конкретных слотов
def get_delete_slots_keyboard():
    keyboard = InlineKeyboardBuilder()
    
    if not available_datetimes:
        keyboard.button(text="📭 Нет слотов для удаления", callback_data="no_slots_to_delete")
    else:
        for i, dt_item in enumerate(available_datetimes):
            # Считаем записи на этот слот
            booked_count = len([record for record in user_time_selections 
                              if record['date_str'] == dt_item['date_str'] 
                              and record['time_str'] == dt_item['time_str']])
            
            text = f"🗑️ {dt_item['date_str']} {dt_item['time_str']}"
            if dt_item.get('description'):
                text += f" - {dt_item['description']}"
            if booked_count > 0:
                text += f" ({booked_count} зап.)"
            
            keyboard.button(text=text, callback_data=f"delete_slot_{i}")
    
    keyboard.button(text="◀️ Назад", callback_data="back_to_time_management_from_delete")
    keyboard.adjust(1)
    return keyboard.as_markup()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    if str(user_id) not in users:
        welcome_text = (
            "👋 Привет! Я бот для записи на время.\n\n"
            "📝 Для использования бота необходимо зарегистрироваться.\n"
            "Регистрация нужна для того, чтобы администратор знал, кто записывается."
        )
        await message.answer(welcome_text, reply_markup=get_registration_keyboard())
    else:
        welcome_text = (
            f"👋 Привет, {users[str(user_id)]['first_name']}!\n"
            f"Добро пожаловать в бот для записи на время."
        )
        
        if is_creator(user_id):
            await message.answer(welcome_text, reply_markup=get_creator_keyboard())
        else:
            await message.answer(welcome_text, reply_markup=get_main_keyboard())

# ============ РЕГИСТРАЦИЯ ============
@dp.callback_query(F.data == "register")
async def start_registration(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📝 Регистрация\n\n"
        "Введите ваше имя:",
        reply_markup=None
    )
    await state.set_state(RegistrationState.waiting_for_first_name)
    await callback.answer()

@dp.callback_query(F.data == "why_register")
async def why_registration(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "📝 Зачем нужна регистрация?\n\n"
        "1. 📋 Администратор знает кто записывается\n"
        "2. 📊 Ведется статистика пользователей\n"
        "3. 🔒 Гарантия что записывается реальный человек\n"
        "4. 📞 Возможность связи при необходимости\n\n"
        "Ваши данные используются только для учета записей.",
        reply_markup=get_registration_keyboard()
    )
    await callback.answer()

@dp.message(RegistrationState.waiting_for_first_name)
async def process_first_name(message: types.Message, state: FSMContext):
    if len(message.text.strip()) < 2:
        await message.answer("❌ Имя должно содержать хотя бы 2 символа.\nВведите ваше имя:")
        return
    
    await state.update_data(first_name=message.text.strip())
    await message.answer(
        "✅ Имя принято!\n\n"
        "Теперь введите вашу фамилию:"
    )
    await state.set_state(RegistrationState.waiting_for_last_name)

@dp.message(RegistrationState.waiting_for_last_name)
async def process_last_name(message: types.Message, state: FSMContext):
    if len(message.text.strip()) < 2:
        await message.answer("❌ Фамилия должна содержать хотя бы 2 символа.\nВведите вашу фамилию:")
        return
    
    user_data = await state.get_data()
    first_name = user_data['first_name']
    last_name = message.text.strip()
    user_id = message.from_user.id
    username = message.from_user.username or ""
    
    # Сохраняем пользователя
    users[str(user_id)] = {
        "first_name": first_name,
        "last_name": last_name,
        "username": username,
        "full_name": f"{first_name} {last_name}",
        "registered_at": datetime.now().isoformat(),
        "is_creator": is_creator(user_id)
    }
    
    save_users(users)
    
    await state.clear()
    
    welcome_text = (
        f"✅ Регистрация завершена!\n\n"
        f"👤 Ваши данные:\n"
        f"• Имя: {first_name}\n"
        f"• Фамилия: {last_name}\n"
        f"• Username: @{username if username else 'не указан'}\n"
        f"• ID: {user_id}\n\n"
        f"Теперь вы можете пользоваться ботом!"
    )
    
    if is_creator(user_id):
        await message.answer(welcome_text, reply_markup=get_creator_keyboard())
    else:
        await message.answer(welcome_text, reply_markup=get_main_keyboard())
    
    # Уведомляем создателей о новой регистрации
    await notify_creators_about_new_user(user_id, f"{first_name} {last_name}", username)

# Уведомление создателей о новом пользователе
async def notify_creators_about_new_user(user_id, full_name, username):
    notification_text = (
        f"📝 НОВЫЙ ПОЛЬЗОВАТЕЛЬ ЗАРЕГИСТРИРОВАЛСЯ!\n\n"
        f"👤 Имя: {full_name}\n"
        f"📱 Username: @{username if username else 'нет'}\n"
        f"🆔 ID: {user_id}\n"
        f"🕐 Время регистрации: {datetime.now().strftime('%H:%M:%S')}"
    )
    
    for creator_id in creators:
        if creator_id != user_id:  # Не отправляем уведомление самому пользователю
            try:
                await bot.send_message(creator_id, notification_text)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление создателю {creator_id}: {e}")

# ============ ЗАПИСЬ ============
@dp.message(F.text == "📅 Записаться")
async def book_time(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем регистрацию
    if str(user_id) not in users:
        await message.answer(
            "❌ Для записи необходимо зарегистрироваться.\n"
            "Нажмите /start для начала регистрации."
        )
        return
    
    if not available_datetimes:
        if is_creator(user_id):
            await message.answer("📭 Нет доступных дат для записи.\n\n"
                              "Добавьте время через меню '➕ Добавить время'",
                              reply_markup=get_creator_keyboard())
        else:
            await message.answer("📭 В данный момент нет доступных дат для записи.\n\n"
                              "Попробуйте позже или свяжитесь с администратором.",
                              reply_markup=get_main_keyboard())
        return
    
    await message.answer("📅 Выберите дату для записи:", 
                        reply_markup=get_available_dates_keyboard())

@dp.callback_query(F.data.startswith("select_date_"))
async def select_date(callback: types.CallbackQuery):
    date_str = callback.data.replace("select_date_", "")
    
    await callback.message.edit_text(
        f"📅 Дата: {date_str}\n"
        f"🕐 Выберите время для записи:\n\n"
        f"В скобках указано количество уже записавшихся",
        reply_markup=get_available_times_keyboard(date_str)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("select_time_"))
async def select_time_slot(callback: types.CallbackQuery):
    data_parts = callback.data.replace("select_time_", "").split("_")
    date_str = data_parts[0]
    time_str = data_parts[1]
    
    user_id = callback.from_user.id
    user_info = users.get(str(user_id), {})
    
    # Проверяем регистрацию
    if not user_info:
        await callback.answer("❌ Вы не зарегистрированы!", show_alert=True)
        return
    
    # Находим описание слота
    slot_description = ""
    for dt_item in available_datetimes:
        if dt_item['date_str'] == date_str and dt_item['time_str'] == time_str:
            slot_description = dt_item.get('description', '')
            break
    
    # Создаем запись
    record = {
        "user_id": user_id,
        "first_name": user_info["first_name"],
        "last_name": user_info["last_name"],
        "username": user_info["username"],
        "full_name": user_info["full_name"],
        "time_str": time_str,
        "date_str": date_str,
        "selected_at": datetime.now(),
        "is_creator": is_creator(user_id)
    }
    
    # Добавляем запись
    user_time_selections.append(record)
    save_bookings(user_time_selections)
    
    # Подтверждение пользователю
    confirm_text = f"✅ Вы успешно записались!\n\n"
    confirm_text += f"📅 Дата: {date_str}\n"
    confirm_text += f"🕐 Время: {time_str}\n"
    if slot_description:
        confirm_text += f"📝 Описание: {slot_description}\n"
    
    # Считаем сколько всего записалось на это время
    total_on_this_slot = len([r for r in user_time_selections 
                            if r['date_str'] == date_str and r['time_str'] == time_str])
    confirm_text += f"👥 Всего записано на это время: {total_on_this_slot} чел.\n\n"
    
    if is_creator(user_id):
        confirm_text += "👑 Вы записались как создатель бота."
    else:
        confirm_text += "Спасибо за запись!"
    
    await callback.message.edit_text(confirm_text, reply_markup=None)
    
    # Уведомление создателям (если записывается не создатель)
    if not is_creator(user_id):
        await notify_creators_about_booking(record, slot_description, total_on_this_slot)
    
    # Показываем соответствующее меню после записи
    if is_creator(user_id):
        await callback.message.answer("Выберите действие:", reply_markup=get_creator_keyboard())
    else:
        await callback.message.answer("Выберите действие:", reply_markup=get_main_keyboard())
    
    await callback.answer()

# Уведомление создателям о новой записи
async def notify_creators_about_booking(record, slot_description, total_on_this_slot):
    try:
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_time_moscow = datetime.now(moscow_tz).strftime('%H:%M:%S')
        
        creator_message = (
            f"📋 НОВАЯ ЗАПИСЬ!\n\n"
            f"👤 Пользователь: {record['full_name']}\n"
            f"📱 Username: @{record['username'] if record['username'] else 'нет'}\n"
            f"🆔 ID: {record['user_id']}\n"
            f"📅 Дата: {record['date_str']}\n"
            f"🕐 Время: {record['time_str']}\n"
        )
        
        if slot_description:
            creator_message += f"📝 Описание: {slot_description}\n"
        
        creator_message += (
            f"🕐 Запись создана: {current_time_moscow}\n\n"
            f"📊 Статистика по слоту:\n"
            f"• Всего записей на это время: {total_on_this_slot}\n"
            f"• Всего записей всего: {len(user_time_selections)}\n\n"
            f"Для просмотра всех записей нажмите '👁️ Кто записался'"
        )
        
        # Отправляем всем создателям, кроме того кто записался (если он создатель)
        for creator_id in creators:
            if creator_id != record['user_id']:  # Не отправляем самому себе
                try:
                    await bot.send_message(creator_id, creator_message)
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление создателю {creator_id}: {e}")
        
        logger.info(f"Уведомление отправлено создателям о записи пользователя {record['full_name']}")
        
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление создателям: {e}")

# ============ ДОБАВЛЕНИЕ ВРЕМЕНИ ============
@dp.message(F.text == "➕ Добавить время")
async def add_time_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if not is_creator(user_id):
        await message.answer("⛔ У вас нет доступа к этой функции.")
        return
    
    await message.answer(
        "➕ Добавление нового времени для записи\n\n"
        "Введите дату в формате ДД.ММ.ГГГГ\n"
        "Например: 25.12.2023\n\n"
        "Или отправьте '❌ Отмена' для отмены:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(AddDateTimeState.waiting_for_date)

@dp.message(F.text == "❌ Отмена")
async def cancel_adding_time(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if is_creator(user_id):
        await state.clear()
        await message.answer("❌ Добавление отменено.", reply_markup=get_creator_keyboard())

@dp.message(AddDateTimeState.waiting_for_date)
async def process_date(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Добавление отменено.", reply_markup=get_creator_keyboard())
        return
    
    try:
        # Пробуем распарсить дату
        date_obj = datetime.strptime(message.text, "%d.%m.%Y").date()
        date_str = date_obj.strftime("%d.%m.%Y")
        
        await state.update_data(date_str=date_str)
        
        await message.answer(
            f"📅 Дата сохранена: {date_str}\n\n"
            "Теперь введите время в формате ЧЧ:ММ\n"
            "Например: 14:30 или 09:00\n\n"
            "Или отправьте '❌ Отмена' для отмены:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(AddDateTimeState.waiting_for_time)
    except ValueError:
        await message.answer(
            "❌ Неверный формат даты!\n"
            "Введите дату в формате ДД.ММ.ГГГГ\n"
            "Например: 25.12.2023\n\n"
            "Или отправьте '❌ Отмена' для отмены:"
        )

@dp.message(AddDateTimeState.waiting_for_time)
async def process_time(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Добавление отменено.", reply_markup=get_creator_keyboard())
        return
    
    try:
        # Пробуем распарсить время
        time_str = message.text
        # Проверяем формат
        datetime.strptime(time_str, "%H:%M")
        
        await state.update_data(time_str=time_str)
        
        await message.answer(
            f"🕐 Время сохранено: {time_str}\n\n"
            "Теперь введите описание (необязательно)\n"
            "Например: 'Консультация', 'Встреча', 'Урок'\n"
            "Или отправьте '-' чтобы пропустить\n\n"
            "Или отправьте '❌ Отмена' для отмены:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="-")],
                    [KeyboardButton(text="❌ Отмена")]
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(AddDateTimeState.waiting_for_description)
    except ValueError:
        await message.answer(
            "❌ Неверный формат времени!\n"
            "Введите время в формате ЧЧ:ММ\n"
            "Например: 14:30 или 09:00\n\n"
            "Или отправьте '❌ Отмена' для отмены:"
        )

@dp.message(AddDateTimeState.waiting_for_description)
async def process_description(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Добавление отменено.", reply_markup=get_creator_keyboard())
        return
    
    data = await state.get_data()
    
    description = message.text if message.text != "-" else ""
    
    # Создаем новый слот
    new_slot = {
        "date_str": data['date_str'],
        "time_str": data['time_str'],
        "description": description,
        "created_at": datetime.now(),
        "created_by": message.from_user.id
    }
    
    # Добавляем в список доступных слотов
    available_datetimes.append(new_slot)
    save_time_slots(available_datetimes)
    
    await state.clear()
    
    response_text = f"✅ Новое время успешно добавлено!\n\n"
    response_text += f"📅 Дата: {data['date_str']}\n"
    response_text += f"🕐 Время: {data['time_str']}\n"
    if description:
        response_text += f"📝 Описание: {description}\n"
    response_text += f"\n📊 Теперь доступно слотов: {len(available_datetimes)}"
    
    await message.answer(response_text, reply_markup=get_creator_keyboard())

# ============ ПОЛЬЗОВАТЕЛИ ============
@dp.message(F.text == "👥 Пользователи")
async def users_management(message: types.Message):
    user_id = message.from_user.id
    
    if not is_creator(user_id):
        await message.answer("⛔ У вас нет доступа к этой функции.")
        return
    
    total_users = len(users)
    active_today = len([u for u in users.values() 
                       if datetime.fromisoformat(u['registered_at']).date() == date.today()])
    
    await message.answer(
        f"👥 Управление пользователями\n\n"
        f"📊 Статистика:\n"
        f"• Всего пользователей: {total_users}\n"
        f"• Зарегистрировано сегодня: {active_today}\n"
        f"• Создателей: {len(creators)}\n\n"
        f"Выберите действие:",
        reply_markup=get_users_management_keyboard()
    )

@dp.callback_query(F.data == "users_stats")
async def view_users_stats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    total_users = len(users)
    
    # Статистика по дням
    reg_by_day = defaultdict(int)
    for user_data in users.values():
        reg_date = datetime.fromisoformat(user_data['registered_at']).date()
        reg_by_day[reg_date] += 1
    
    # Записи пользователей
    bookings_by_user = defaultdict(int)
    for booking in user_time_selections:
        bookings_by_user[booking['user_id']] += 1
    
    stats_text = "📊 Статистика пользователей:\n\n"
    stats_text += f"👥 Всего пользователей: {total_users}\n"
    
    # За последние 7 дней
    week_ago = date.today() - timedelta(days=7)
    recent_users = sum(1 for user_data in users.values() 
                      if datetime.fromisoformat(user_data['registered_at']).date() >= week_ago)
    stats_text += f"📈 Зарегистрировано за 7 дней: {recent_users}\n"
    
    # Активные пользователи (имеющие записи)
    active_users = len(bookings_by_user)
    stats_text += f"📅 Пользователей с записями: {active_users}\n"
    
    # Среднее количество записей
    if active_users > 0:
        avg_bookings = sum(bookings_by_user.values()) / active_users
        stats_text += f"📝 Среднее записей на пользователя: {avg_bookings:.1f}\n"
    
    # Самый активный пользователь
    if bookings_by_user:
        most_active_id = max(bookings_by_user.items(), key=lambda x: x[1])[0]
        most_active_user = users.get(str(most_active_id), {})
        if most_active_user:
            stats_text += f"🏆 Самый активный: {most_active_user['full_name']} ({bookings_by_user[most_active_id]} зап.)\n"
    
    # Последние 5 регистраций
    stats_text += f"\n📋 Последние регистрации:\n"
    sorted_users = sorted(users.items(), 
                         key=lambda x: datetime.fromisoformat(x[1]['registered_at']), 
                         reverse=True)[:5]
    
    for i, (uid, user_data) in enumerate(sorted_users, 1):
        reg_time = datetime.fromisoformat(user_data['registered_at']).strftime('%d.%m.%Y')
        user_bookings = bookings_by_user.get(int(uid), 0)
        stats_text += f"{i}. {user_data['full_name']} - {reg_time} ({user_bookings} зап.)\n"
    
    back_keyboard = InlineKeyboardBuilder()
    back_keyboard.button(text="◀️ Назад", callback_data="back_to_users_management")
    back_keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu")
    back_keyboard.adjust(1)
    
    await callback.message.edit_text(stats_text, reply_markup=back_keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "view_all_users_list")
async def view_all_users_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    if not users:
        await callback.message.edit_text("📭 Нет зарегистрированных пользователей.", 
                                       reply_markup=get_users_management_keyboard())
        return
    
    # Считаем записи для каждого пользователя
    bookings_by_user = defaultdict(int)
    for booking in user_time_selections:
        bookings_by_user[booking['user_id']] += 1
    
    users_text = "👥 Все пользователи:\n\n"
    
    # Сортируем по дате регистрации
    sorted_users = sorted(users.items(), 
                         key=lambda x: datetime.fromisoformat(x[1]['registered_at']), 
                         reverse=True)
    
    for i, (uid, user_data) in enumerate(sorted_users, 1):
        is_creator_mark = "👑 " if int(uid) in creators else ""
        reg_date = datetime.fromisoformat(user_data['registered_at']).strftime('%d.%m.%Y')
        user_bookings = bookings_by_user.get(int(uid), 0)
        
        users_text += f"{i}. {is_creator_mark}{user_data['full_name']}\n"
        users_text += f"   📱 @{user_data['username'] if user_data['username'] else 'нет'}\n"
        users_text += f"   🆔 {uid}\n"
        users_text += f"   📅 Регистрация: {reg_date}\n"
        users_text += f"   📝 Записей: {user_bookings}\n"
        users_text += f"{'-'*30}\n"
    
    users_text += f"\n📊 Итого: {len(users)} пользователей"
    
    back_keyboard = InlineKeyboardBuilder()
    back_keyboard.button(text="◀️ Назад", callback_data="back_to_users_management")
    back_keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu")
    back_keyboard.adjust(1)
    
    await callback.message.edit_text(users_text, reply_markup=back_keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "search_user")
async def search_user_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔍 Поиск пользователя\n\n"
        "Функция поиска будет добавлена в следующем обновлении.\n\n"
        "Пока вы можете просмотреть всех пользователей через '👁️ Просмотр всех пользователей'",
        reply_markup=get_users_management_keyboard()
    )
    await callback.answer()

# ============ СОЗДАТЕЛИ ============
@dp.message(F.text == "👑 Создатели")
async def creators_management(message: types.Message):
    user_id = message.from_user.id
    
    if not is_main_creator(user_id):
        await message.answer("⛔ Только главный создатель может управлять создателями.")
        return
    
    await message.answer(
        f"👑 Управление создателями\n\n"
        f"📊 Статистика:\n"
        f"• Всего создателей: {len(creators)}\n"
        f"• Главный создатель: {MAIN_CREATOR_ID}\n\n"
        f"Выберите действие:",
        reply_markup=get_creators_management_keyboard()
    )

@dp.callback_query(F.data == "view_all_creators")
async def view_all_creators(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_main_creator(user_id):
        await callback.answer("⛔ Только главный создатель может просматривать создателей", show_alert=True)
        return
    
    creators_text = "👑 Все создатели:\n\n"
    
    for i, creator_id in enumerate(creators, 1):
        is_main = "🌟 " if creator_id == MAIN_CREATOR_ID else "   "
        user_info = users.get(str(creator_id), {})
        
        if user_info:
            creators_text += f"{i}. {is_main}{user_info['full_name']}\n"
            creators_text += f"   📱 @{user_info['username'] if user_info['username'] else 'нет'}\n"
            creators_text += f"   🆔 {creator_id}\n"
        else:
            creators_text += f"{i}. {is_main}Не зарегистрирован в боте\n"
            creators_text += f"   🆔 {creator_id}\n"
        
        # Считаем записи создателя
        creator_bookings = len([b for b in user_time_selections if b['user_id'] == creator_id])
        creators_text += f"   📝 Записей: {creator_bookings}\n"
        creators_text += f"{'-'*30}\n"
    
    creators_text += f"\n📊 Итого: {len(creators)} создателей"
    
    back_keyboard = InlineKeyboardBuilder()
    back_keyboard.button(text="◀️ Назад", callback_data="back_to_creators_management")
    back_keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu")
    back_keyboard.adjust(1)
    
    await callback.message.edit_text(creators_text, reply_markup=back_keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "add_creator")
async def add_creator_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if not is_main_creator(user_id):
        await callback.answer("⛔ Только главный создатель может добавлять создателей", show_alert=True)
        return
    
    await callback.message.edit_text(
        "➕ Добавление нового создателя\n\n"
        "Введите ID пользователя, которого хотите сделать создателем.\n\n"
        "ℹ️ Как получить ID пользователя:\n"
        "1. Попросите пользователя написать @userinfobot\n"
        "2. Скопируйте цифровой ID\n"
        "3. Отправьте его сюда\n\n"
        "Или отправьте '❌ Отмена' для отмены:",
        reply_markup=None
    )
    await state.set_state(AddCreatorState.waiting_for_user_id)

@dp.message(AddCreatorState.waiting_for_user_id)
async def process_creator_id(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Добавление отменено.", reply_markup=get_creator_keyboard())
        return
    
    try:
        new_creator_id = int(message.text)
        
        # Проверяем, не является ли уже создателем
        if new_creator_id in creators:
            await message.answer(f"❌ Пользователь с ID {new_creator_id} уже является создателем.")
            return
        
        # Проверяем, не является ли главным создателем
        if new_creator_id == MAIN_CREATOR_ID:
            await message.answer(f"❌ Этот пользователь уже является главным создателем.")
            return
        
        # Добавляем в список создателей
        creators.append(new_creator_id)
        save_creators(creators)
        
        # Обновляем статус в информации о пользователе, если он зарегистрирован
        if str(new_creator_id) in users:
            users[str(new_creator_id)]["is_creator"] = True
            save_users(users)
        
        # Пытаемся получить информацию о пользователе
        try:
            user_chat = await bot.get_chat(new_creator_id)
            user_name = user_chat.full_name
            user_username = user_chat.username
        except:
            user_name = "Неизвестный пользователь"
            user_username = "нет"
        
        await message.answer(
            f"✅ Пользователь успешно добавлен как создатель!\n\n"
            f"👤 Имя: {user_name}\n"
            f"📱 Username: @{user_username if user_username else 'нет'}\n"
            f"🆔 ID: {new_creator_id}\n\n"
            f"📊 Теперь создателей: {len(creators)}",
            reply_markup=get_creator_keyboard()
        )
        
        # Уведомляем нового создателя
        try:
            await bot.send_message(
                new_creator_id,
                f"🎉 Поздравляем! Вы были добавлены как создатель бота!\n\n"
                f"Теперь у вас есть доступ к функциям создания:\n"
                f"• Добавление времени для записи\n"
                f"• Просмотр всех записей\n"
                f"• Управление пользователями\n"
                f"• И многое другое!\n\n"
                f"Напишите /start чтобы обновить меню."
            )
        except:
            pass  # Не удалось отправить уведомление
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Неверный формат ID! ID должен состоять только из цифр.\nПопробуйте еще раз или отправьте '❌ Отмена':")

@dp.callback_query(F.data == "remove_creator")
async def remove_creator_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_main_creator(user_id):
        await callback.answer("⛔ Только главный создатель может удалять создателей", show_alert=True)
        return
    
    if len(creators) <= 1:
        await callback.message.edit_text("❌ Нельзя удалить всех создателей! Должен остаться хотя бы один создатель.", 
                                       reply_markup=get_creators_management_keyboard())
        return
    
    # Создаем клавиатуру с создателями для удаления (кроме главного)
    keyboard = InlineKeyboardBuilder()
    
    for creator_id in creators:
        if creator_id != MAIN_CREATOR_ID:  # Не показываем главного создателя
            user_info = users.get(str(creator_id), {})
            if user_info:
                text = f"🗑️ {user_info['full_name']} (@{user_info['username'] or 'нет'})"
            else:
                text = f"🗑️ ID: {creator_id}"
            
            keyboard.button(text=text, callback_data=f"remove_creator_{creator_id}")
    
    if keyboard._markup:  # Проверяем, есть ли кнопки
        keyboard.button(text="◀️ Назад", callback_data="back_to_creators_management")
        keyboard.adjust(1)
        await callback.message.edit_text(
            "🗑️ Выберите создателя для удаления:\n\n"
            "⚠️ Главный создатель не может быть удален.",
            reply_markup=keyboard.as_markup()
        )
    else:
        await callback.message.edit_text("❌ Нет создателей для удаления (кроме главного).", 
                                       reply_markup=get_creators_management_keyboard())
    
    await callback.answer()

@dp.callback_query(F.data.startswith("remove_creator_"))
async def confirm_remove_creator(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_main_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    creator_id_to_remove = int(callback.data.replace("remove_creator_", ""))
    
    # Проверяем, не является ли главным создателем
    if creator_id_to_remove == MAIN_CREATOR_ID:
        await callback.answer("❌ Нельзя удалить главного создателя!", show_alert=True)
        return
    
    # Получаем информацию о создателе
    user_info = users.get(str(creator_id_to_remove), {})
    creator_name = user_info.get('full_name', f'ID: {creator_id_to_remove}') if user_info else f'ID: {creator_id_to_remove}'
    
    # Создаем клавиатуру подтверждения
    confirm_keyboard = InlineKeyboardBuilder()
    confirm_keyboard.button(text="✅ Да, удалить", callback_data=f"confirm_remove_{creator_id_to_remove}")
    confirm_keyboard.button(text="❌ Нет, отмена", callback_data="remove_creator")
    confirm_keyboard.adjust(2)
    
    await callback.message.edit_text(
        f"⚠️ Вы уверены, что хотите удалить создателя?\n\n"
        f"👤 Создатель: {creator_name}\n"
        f"🆔 ID: {creator_id_to_remove}\n\n"
        f"После удаления пользователь потеряет доступ к функциям создателя.\n"
        f"Это действие можно отменить только повторным добавлением.",
        reply_markup=confirm_keyboard.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_remove_"))
async def actually_remove_creator(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_main_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    creator_id_to_remove = int(callback.data.replace("confirm_remove_", ""))
    
    # Удаляем из списка создателей
    if creator_id_to_remove in creators:
        creators.remove(creator_id_to_remove)
        save_creators(creators)
        
        # Обновляем статус в информации о пользователе
        if str(creator_id_to_remove) in users:
            users[str(creator_id_to_remove)]["is_creator"] = False
            save_users(users)
        
        # Уведомляем удаленного создателя
        try:
            await bot.send_message(
                creator_id_to_remove,
                "ℹ️ Ваши права создателя были отозваны.\n"
                "Теперь у вас есть доступ только к функциям обычного пользователя."
            )
        except:
            pass
        
        await callback.message.edit_text(
            f"✅ Создатель успешно удален!\n\n"
            f"📊 Теперь создателей: {len(creators)}",
            reply_markup=get_creators_management_keyboard()
        )
    else:
        await callback.message.edit_text("❌ Создатель не найден.", 
                                       reply_markup=get_creators_management_keyboard())
    
    await callback.answer()

# ============ КТО ЗАПИСАЛСЯ ============
@dp.message(F.text == "👁️ Кто записался")
async def who_booked_menu(message: types.Message):
    user_id = message.from_user.id
    
    if not is_creator(user_id):
        await message.answer("⛔ У вас нет доступа к этой функции.")
        return
    
    if not user_time_selections:
        await message.answer("📭 Пока никто не записался.", reply_markup=get_creator_keyboard())
        return
    
    total_users = len(set(r['user_id'] for r in user_time_selections))
    creator_bookings = len([r for r in user_time_selections if r.get('is_creator')])
    
    await message.answer(
        f"👁️ Просмотр записей\n\n"
        f"📊 Статистика:\n"
        f"• Всего записей: {len(user_time_selections)}\n"
        f"• Уникальных пользователей: {total_users}\n"
        f"• Ваших записей: {creator_bookings}\n\n"
        f"Выберите способ просмотра:",
        reply_markup=get_who_booked_keyboard()
    )

@dp.callback_query(F.data.startswith("view_date_"))
async def view_bookings_by_date(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    date_str = callback.data.replace("view_date_", "")
    
    # Получаем записи на эту дату
    date_bookings = [r for r in user_time_selections if r['date_str'] == date_str]
    
    if not date_bookings:
        await callback.answer(f"На дату {date_str} нет записей", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"📅 Дата: {date_str}\n"
        f"👥 Всего записей: {len(date_bookings)}\n\n"
        f"Выберите время для просмотра записавшихся:",
        reply_markup=get_time_for_date_keyboard(date_str)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("view_time_"))
async def view_bookings_by_time(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    data_parts = callback.data.replace("view_time_", "").split("_")
    date_str = data_parts[0]
    time_str = data_parts[1]
    
    # Получаем записи на это время
    time_bookings = [r for r in user_time_selections 
                    if r['date_str'] == date_str and r['time_str'] == time_str]
    
    # Находим описание
    description = ""
    for dt_item in available_datetimes:
        if dt_item['date_str'] == date_str and dt_item['time_str'] == time_str:
            description = dt_item.get('description', '')
            break
    
    bookings_text = f"👥 Записавшиеся\n\n"
    bookings_text += f"📅 Дата: {date_str}\n"
    bookings_text += f"🕐 Время: {time_str}\n"
    if description:
        bookings_text += f"📝 Описание: {description}\n"
    bookings_text += f"👥 Всего записей: {len(time_bookings)}\n\n"
    
    # Сортируем по времени записи
    time_bookings.sort(key=lambda x: x['selected_at'])
    
    for i, booking in enumerate(time_bookings, 1):
        creator_mark = "👑 " if booking.get('is_creator') else ""
        username_display = f"(@{booking['username']})" if booking.get('username') and booking['username'] != booking['full_name'] else ""
        
        bookings_text += f"{i}. {creator_mark}{booking['full_name']} {username_display}\n"
        bookings_text += f"   🆔 ID: {booking['user_id']}\n"
        bookings_text += f"   🕐 Записался: {booking['selected_at'].strftime('%H:%M')}\n"
        
        # Вычисляем сколько времени назад записался
        time_ago = datetime.now() - booking['selected_at']
        hours_ago = time_ago.seconds // 3600
        minutes_ago = (time_ago.seconds % 3600) // 60
        
        if hours_ago > 0:
            bookings_text += f"   ⏰ {hours_ago} ч. {minutes_ago} мин. назад\n"
        else:
            bookings_text += f"   ⏰ {minutes_ago} мин. назад\n"
        
        bookings_text += f"{'-'*30}\n"
    
    # Клавиатура для возврата
    back_keyboard = InlineKeyboardBuilder()
    back_keyboard.button(text="◀️ Назад к времени", callback_data=f"view_date_{date_str}")
    back_keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu")
    back_keyboard.adjust(1)
    
    await callback.message.edit_text(bookings_text, reply_markup=back_keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "view_all_by_time")
async def view_all_bookings_by_time(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    if not user_time_selections:
        await callback.message.edit_text("📭 Нет записей.", reply_markup=get_who_booked_keyboard())
        return
    
    # Группируем по дате и времени
    bookings_by_datetime = defaultdict(list)
    for record in user_time_selections:
        key = f"{record['date_str']} {record['time_str']}"
        bookings_by_datetime[key].append(record)
    
    bookings_text = "📋 Все записи по дате и времени:\n\n"
    
    for key, bookings in sorted(bookings_by_datetime.items()):
        date_str, time_str = key.split(" ")
        
        # Находим описание
        description = ""
        for dt_item in available_datetimes:
            if dt_item['date_str'] == date_str and dt_item['time_str'] == time_str:
                description = dt_item.get('description', '')
                break
        
        bookings_text += f"📅 {date_str} 🕐 {time_str}\n"
        if description:
            bookings_text += f"📝 {description}\n"
        
        creator_count = len([b for b in bookings if b.get('is_creator')])
        if creator_count > 0:
            bookings_text += f"👥 Всего: {len(bookings)} чел. (👑 {creator_count})\n"
        else:
            bookings_text += f"👥 Всего: {len(bookings)} чел.\n"
        
        # Показываем первых 3 пользователя
        for booking in bookings[:3]:
            creator_mark = "👑 " if booking.get('is_creator') else ""
            name = booking['full_name'][:15] + "..." if len(booking['full_name']) > 15 else booking['full_name']
            bookings_text += f"   {creator_mark}{name}\n"
        
        if len(bookings) > 3:
            bookings_text += f"   ... и ещё {len(bookings) - 3} чел.\n"
        
        bookings_text += f"{'-'*40}\n"
    
    total_records = len(user_time_selections)
    unique_users = len(set(r['user_id'] for r in user_time_selections))
    unique_dates = len(set(r['date_str'] for r in user_time_selections))
    
    bookings_text += f"\n📊 Итого:\n"
    bookings_text += f"• Записей: {total_records}\n"
    bookings_text += f"• Пользователей: {unique_users}\n"
    bookings_text += f"• Дней с записями: {unique_dates}"
    
    # Клавиатура
    back_keyboard = InlineKeyboardBuilder()
    back_keyboard.button(text="◀️ Назад", callback_data="back_to_who_booked")
    back_keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu")
    back_keyboard.adjust(1)
    
    await callback.message.edit_text(bookings_text, reply_markup=back_keyboard.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "view_all_users_booking")
async def view_all_users_booking(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    if not user_time_selections:
        await callback.message.edit_text("📭 Нет пользователей.", reply_markup=get_who_booked_keyboard())
        return
    
    # Группируем по пользователям
    user_stats = defaultdict(lambda: {"count": 0, "is_creator": False, "last_booking": None})
    
    for record in user_time_selections:
        user_id_key = record['user_id']
        user_stats[user_id_key]["count"] += 1
        user_stats[user_id_key]["is_creator"] = record.get('is_creator', False)
        user_stats[user_id_key]["name"] = record['full_name']
        user_stats[user_id_key]["username"] = record.get('username', '')
        
        # Обновляем последнюю запись
        if not user_stats[user_id_key]["last_booking"] or record['selected_at'] > user_stats[user_id_key]["last_booking"]:
            user_stats[user_id_key]["last_booking"] = record['selected_at']
    
    users_text = "👥 Все пользователи (по записям):\n\n"
    
    # Сортируем по количеству записей
    sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["count"], reverse=True)
    
    for i, (user_id_key, stats) in enumerate(sorted_users, 1):
        creator_mark = "👑 " if stats["is_creator"] else ""
        username_display = f"(@{stats['username']})" if stats['username'] and stats['username'] != stats['name'] else ""
        
        users_text += f"{i}. {creator_mark}{stats['name']} {username_display}\n"
        users_text += f"   🆔 ID: {user_id_key}\n"
        users_text += f"   📊 Записей: {stats['count']}\n"
        
        if stats["last_booking"]:
            last_booking_str = stats["last_booking"].strftime('%d.%m.%Y %H:%M')
            users_text += f"   🕐 Последняя запись: {last_booking_str}\n"
        
        users_text += f"{'-'*30}\n"
    
    total_users = len(user_stats)
    total_bookings = len(user_time_selections)
    avg_bookings = total_bookings / total_users if total_users > 0 else 0
    
    users_text += f"\n📊 Статистика:\n"
    users_text += f"• Всего пользователей: {total_users}\n"
    users_text += f"• Всего записей: {total_bookings}\n"
    users_text += f"• Среднее записей на пользователя: {avg_bookings:.1f}"
    
    # Клавиатура
    back_keyboard = InlineKeyboardBuilder()
    back_keyboard.button(text="◀️ Назад", callback_data="back_to_who_booked")
    back_keyboard.button(text="🏠 В меню", callback_data="back_to_creator_menu")
    back_keyboard.adjust(1)
    
    await callback.message.edit_text(users_text, reply_markup=back_keyboard.as_markup())
    await callback.answer()

# ============ УПРАВЛЕНИЕ ВРЕМЕНЕМ ============
@dp.message(F.text == "🗑️ Управление временем")
async def time_management(message: types.Message):
    user_id = message.from_user.id
    
    if not is_creator(user_id):
        await message.answer("⛔ У вас нет доступа к этой функции.")
        return
    
    await message.answer(
        "🗑️ Управление доступным временем\n\n"
        f"📊 Всего слотов: {len(available_datetimes)}\n"
        f"📈 Всего записей: {len(user_time_selections)}\n\n"
        "Выберите действие:",
        reply_markup=get_time_management_keyboard()
    )

@dp.callback_query(F.data == "view_all_slots")
async def view_all_slots(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    if not available_datetimes:
        await callback.message.edit_text("📭 Нет доступных слотов.", reply_markup=get_time_management_keyboard())
        return
    
    slots_text = "👁️ Все доступные слоты:\n\n"
    
    # Сортируем слоты по дате и времени
    sorted_slots = sorted(available_datetimes, key=lambda x: (x['date_str'], x['time_str']))
    
    for i, slot in enumerate(sorted_slots, 1):
        # Считаем записи на этот слот
        booked_count = len([record for record in user_time_selections 
                          if record['date_str'] == slot['date_str'] 
                          and record['time_str'] == slot['time_str']])
        
        slots_text += f"{i}. 📅 {slot['date_str']} 🕐 {slot['time_str']}\n"
        if slot.get('description'):
            slots_text += f"   📝 {slot['description']}\n"
        slots_text += f"   👥 Записей: {booked_count}\n"
        slots_text += f"   📅 Добавлен: {slot['created_at'].strftime('%d.%m.%Y %H:%M')}\n"
        slots_text += f"{'-'*30}\n"
    
    slots_text += f"\n📊 Итого: {len(available_datetimes)} слотов"
    
    await callback.message.edit_text(slots_text, reply_markup=get_time_management_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "delete_slot")
async def delete_slot_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    if not available_datetimes:
        await callback.message.edit_text("📭 Нет слотов для удаления.", reply_markup=get_time_management_keyboard())
        return
    
    await callback.message.edit_text(
        "🗑️ Выберите слот для удаления:\n\n"
        "В скобках указано количество записей на этот слот",
        reply_markup=get_delete_slots_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_slot_"))
async def confirm_delete_slot(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    slot_index = int(callback.data.replace("delete_slot_", ""))
    
    if 0 <= slot_index < len(available_datetimes):
        slot = available_datetimes[slot_index]
        
        # Создаем клавиатуру подтверждения
        confirm_keyboard = InlineKeyboardBuilder()
        confirm_keyboard.button(text="✅ Да, удалить", callback_data=f"confirm_delete_{slot_index}")
        confirm_keyboard.button(text="❌ Нет, отмена", callback_data="delete_slot")
        confirm_keyboard.adjust(2)
        
        # Считаем записи на этот слот
        booked_count = len([record for record in user_time_selections 
                          if record['date_str'] == slot['date_str'] 
                          and record['time_str'] == slot['time_str']])
        
        warning_text = ""
        if booked_count > 0:
            warning_text = f"⚠️ На этот слот записано {booked_count} человек!\n"
        
        await callback.message.edit_text(
            f"{warning_text}"
            f"Вы уверены, что хотите удалить этот слот?\n\n"
            f"📅 Дата: {slot['date_str']}\n"
            f"🕐 Время: {slot['time_str']}\n"
            f"📝 Описание: {slot.get('description', 'нет')}\n\n"
            f"Это действие нельзя отменить!",
            reply_markup=confirm_keyboard.as_markup()
        )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def actually_delete_slot(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    slot_index = int(callback.data.replace("confirm_delete_", ""))
    
    if 0 <= slot_index < len(available_datetimes):
        deleted_slot = available_datetimes.pop(slot_index)
        
        # Удаляем все записи на этот слот
        deleted_records_count = 0
        original_count = len(user_time_selections)
        user_time_selections[:] = [record for record in user_time_selections 
                                  if not (record['date_str'] == deleted_slot['date_str'] 
                                         and record['time_str'] == deleted_slot['time_str'])]
        deleted_records_count = original_count - len(user_time_selections)
        
        # Сохраняем изменения
        save_time_slots(available_datetimes)
        save_bookings(user_time_selections)
        
        await callback.message.edit_text(
            f"✅ Слот успешно удален!\n\n"
            f"📅 Дата: {deleted_slot['date_str']}\n"
            f"🕐 Время: {deleted_slot['time_str']}\n"
            f"🗑️ Удалено записей на этот слот: {deleted_records_count}\n\n"
            f"📊 Осталось слотов: {len(available_datetimes)}\n"
            f"📈 Всего записей: {len(user_time_selections)}",
            reply_markup=get_time_management_keyboard()
        )
    await callback.answer()

@dp.callback_query(F.data == "clear_all_slots")
async def clear_all_slots(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    # Создаем клавиатуру подтверждения
    confirm_keyboard = InlineKeyboardBuilder()
    confirm_keyboard.button(text="✅ Да, удалить все", callback_data="confirm_clear_all_slots")
    confirm_keyboard.button(text="❌ Нет, отмена", callback_data="back_to_time_management")
    confirm_keyboard.adjust(2)
    
    await callback.message.edit_text(
        f"⚠️ Внимание! Это удалит:\n"
        f"• {len(available_datetimes)} слотов для записи\n"
        f"• {len(user_time_selections)} записей пользователей\n\n"
        f"Это действие нельзя отменить!",
        reply_markup=confirm_keyboard.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "confirm_clear_all_slots")
async def confirm_clear_all_slots(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    slots_count = len(available_datetimes)
    records_count = len(user_time_selections)
    
    available_datetimes.clear()
    user_time_selections.clear()
    
    # Сохраняем изменения
    save_time_slots(available_datetimes)
    save_bookings(user_time_selections)
    
    await callback.message.edit_text(
        f"✅ Все данные очищены!\n\n"
        f"🗑️ Удалено:\n"
        f"• Слотов: {slots_count}\n"
        f"• Записей: {records_count}",
        reply_markup=get_time_management_keyboard()
    )
    await callback.answer()

# ============ ОБРАБОТКА КНОПОК "НАЗАД" ============
@dp.callback_query(F.data == "back_to_users_management")
async def back_to_users_management(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await users_management(types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=datetime.now()
    ))
    await callback.answer()

@dp.callback_query(F.data == "back_to_creators_management")
async def back_to_creators_management(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_main_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await creators_management(types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=datetime.now()
    ))
    await callback.answer()

@dp.callback_query(F.data == "back_to_who_booked")
async def back_to_who_booked(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await who_booked_menu(types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=datetime.now()
    ))
    await callback.answer()

@dp.callback_query(F.data == "back_to_time_management")
async def back_to_time_management(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await time_management(types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=datetime.now()
    ))
    await callback.answer()

@dp.callback_query(F.data == "back_to_creator_menu")
async def back_to_creator_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer("Главное меню создателя:", reply_markup=get_creator_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_main_menu_from_dates")
async def back_to_main_menu_from_dates(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if is_creator(user_id):
        await callback.message.edit_text("Возврат в меню создателя...")
        await callback.message.answer("Выберите действие:", reply_markup=get_creator_keyboard())
    else:
        await callback.message.edit_text("Возврат в главное меню...")
        await callback.message.answer("Выберите действие:", reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_main_menu_from_time")
async def back_to_main_menu_from_time(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if is_creator(user_id):
        await callback.message.edit_text("Возврат в меню создателя...")
        await callback.message.answer("Выберите действие:", reply_markup=get_creator_keyboard())
    else:
        await callback.message.edit_text("Возврат в главное меню...")
        await callback.message.answer("Выберите действие:", reply_markup=get_main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_dates_from_time")
async def back_to_dates_from_time(callback: types.CallbackQuery):
    await callback.message.edit_text("📅 Выберите дату для записи:", 
                                   reply_markup=get_available_dates_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_creator_menu_from_users")
async def back_to_creator_menu_from_users(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer("Главное меню создателя:", reply_markup=get_creator_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_creator_menu_from_creators")
async def back_to_creator_menu_from_creators(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer("Главное меню создателя:", reply_markup=get_creator_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_creator_menu_from_time")
async def back_to_creator_menu_from_time(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer("Главное меню создателя:", reply_markup=get_creator_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_creator_menu_from_who")
async def back_to_creator_menu_from_who(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer("Главное меню создателя:", reply_markup=get_creator_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_creator_menu_from_date")
async def back_to_creator_menu_from_date(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text("Возврат в меню...")
    await callback.message.answer("Главное меню создателя:", reply_markup=get_creator_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_who_booked_from_date")
async def back_to_who_booked_from_date(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await who_booked_menu(types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=datetime.now()
    ))
    await callback.answer()

@dp.callback_query(F.data == "back_to_time_management_from_delete")
async def back_to_time_management_from_delete(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if not is_creator(user_id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    
    await time_management(types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=datetime.now()
    ))
    await callback.answer()

# ============ ПРОСМОТР ЗАПИСЕЙ ============
@dp.message(F.text == "📋 Мои записи")
async def view_my_bookings(message: types.Message):
    user_id = message.from_user.id
    
    # Получаем все записи пользователя
    user_bookings = [record for record in user_time_selections 
                    if record['user_id'] == user_id]
    
    if not user_bookings:
        if is_creator(user_id):
            await message.answer("📭 У вас пока нет записей.\n\n"
                              "Нажмите '📅 Записаться', чтобы создать первую запись.",
                              reply_markup=get_creator_keyboard())
        else:
            await message.answer("📭 У вас пока нет записей.\n\n"
                              "Нажмите '📅 Записаться', чтобы создать первую запись.",
                              reply_markup=get_main_keyboard())
        return
    
    # Сортируем по дате и времени
    user_bookings.sort(key=lambda x: (x['date_str'], x['time_str']))
    
    bookings_text = "📋 Ваши записи:\n\n"
    for i, booking in enumerate(user_bookings, 1):
        # Находим описание слота
        description = ""
        for dt_item in available_datetimes:
            if dt_item['date_str'] == booking['date_str'] and dt_item['time_str'] == booking['time_str']:
                description = dt_item.get('description', '')
                break
        
        bookings_text += (
            f"{i}. 📅 {booking['date_str']}\n"
            f"   🕐 {booking['time_str']}\n"
        )
        if description:
            bookings_text += f"   📝 {description}\n"
        bookings_text += f"   🕐 Записано: {booking['selected_at'].strftime('%H:%M')}\n"
        bookings_text += f"{'-'*30}\n"
    
    bookings_text += f"\n📊 Всего ваших записей: {len(user_bookings)}"
    
    if is_creator(user_id):
        await message.answer(bookings_text, reply_markup=get_creator_keyboard())
    else:
        await message.answer(bookings_text, reply_markup=get_main_keyboard())

@dp.message(F.text == "📋 Все записи")
async def show_all_bookings(message: types.Message):
    user_id = message.from_user.id
    
    if not is_creator(user_id):
        await message.answer("⛔ У вас нет доступа к этой функции.")
        return
    
    if not user_time_selections:
        await message.answer("📭 Записей пока нет.", reply_markup=get_creator_keyboard())
        return
    
    # Группируем по дате и времени
    bookings_by_datetime = defaultdict(list)
    for record in user_time_selections:
        key = f"{record['date_str']} {record['time_str']}"
        bookings_by_datetime[key].append(record)
    
    bookings_text = "📋 Все записи (сгруппировано):\n\n"
    
    for key, bookings in sorted(bookings_by_datetime.items()):
        date_str, time_str = key.split(" ")
        
        # Находим описание
        description = ""
        for dt_item in available_datetimes:
            if dt_item['date_str'] == date_str and dt_item['time_str'] == time_str:
                description = dt_item.get('description', '')
                break
        
        bookings_text += f"📅 {date_str} 🕐 {time_str}\n"
        if description:
            bookings_text += f"📝 {description}\n"
        bookings_text += f"👥 Записано: {len(bookings)} чел.\n\n"
        
        for booking in bookings:
            creator_mark = "👑 " if booking.get('is_creator') else "   "
            username_display = f"(@{booking['username']})" if booking.get('username') and booking['username'] != booking['full_name'] else ""
            bookings_text += f"   {creator_mark}{booking['full_name']} {username_display}\n"
            bookings_text += f"       🕐 {booking['selected_at'].strftime('%H:%M')}\n"
        
        bookings_text += f"{'-'*40}\n"
    
    creator_count = len([r for r in user_time_selections if r.get('is_creator')])
    unique_users = len(set(r['user_id'] for r in user_time_selections))
    
    bookings_text += f"\n📊 Итого:\n"
    bookings_text += f"• Всего записей: {len(user_time_selections)}\n"
    bookings_text += f"• Уникальных пользователей: {unique_users}\n"
    bookings_text += f"• Записей создателя: {creator_count}\n"
    bookings_text += f"• Уникальных дат: {len(set(r['date_str'] for r in user_time_selections))}"
    
    await message.answer(bookings_text, reply_markup=get_creator_keyboard())

# ============ ОБРАБОТКА ПУСТЫХ ДАННЫХ ============
@dp.callback_query(F.data == "no_dates")
async def no_dates_available(callback: types.CallbackQuery):
    await callback.answer("📭 Нет доступных дат для записи", show_alert=True)

@dp.callback_query(F.data == "no_times")
async def no_times_available(callback: types.CallbackQuery):
    await callback.answer("🕐 Нет доступного времени на эту дату", show_alert=True)

@dp.callback_query(F.data == "no_slots_to_delete")
async def no_slots_to_delete(callback: types.CallbackQuery):
    await callback.answer("📭 Нет слотов для удаления", show_alert=True)

@dp.callback_query(F.data == "no_bookings")
async def no_bookings_available(callback: types.CallbackQuery):
    await callback.answer("📭 Нет записей для просмотра", show_alert=True)

@dp.callback_query(F.data == "no_bookings_for_date")
async def no_bookings_for_date(callback: types.CallbackQuery):
    await callback.answer("📭 На эту дату нет записей", show_alert=True)

# ============ О БОТЕ ============
@dp.message(F.text == "ℹ️ О боте")
async def about_bot(message: types.Message):
    user_id = message.from_user.id
    
    if is_creator(user_id):
        creator_bookings = len([r for r in user_time_selections if r.get('is_creator')])
        unique_users = len(set(r['user_id'] for r in user_time_selections))
        available_dates = len(set(dt['date_str'] for dt in available_datetimes))
        
        text = (
            "🤖 Бот для записи на время\n\n"
            f"👑 Вы {'главный ' if is_main_creator(user_id) else ''}создатель бота!\n\n"
            "Ваши функции:\n"
            "• 📅 Записаться (на любое доступное время)\n"
            "• ➕ Добавить время (конкретные дата и время)\n"
            "• 📋 Все записи (просмотр всех записей)\n"
            "• 👥 Пользователи (управление пользователями)\n"
        )
        
        if is_main_creator(user_id):
            text += "• 👑 Создатели (управление создателями)\n"
        
        text += (
            "• 👁️ Кто записался (детальный просмотр)\n"
            "• 🗑️ Управление временем (удаление слотов)\n\n"
            f"📊 Статистика:\n"
            f"• Ваших записей: {creator_bookings}\n"
            f"• Всего записей: {len(user_time_selections)}\n"
            f"• Зарегистрированных пользователей: {len(users)}\n"
            f"• Доступных слотов: {len(available_datetimes)}"
        )
        
        await message.answer(text, reply_markup=get_creator_keyboard())
    else:
        user_info = users.get(str(user_id), {})
        if user_info:
            user_name = user_info['first_name']
        else:
            user_name = "друг"
        
        available_dates_count = len(set(dt['date_str'] for dt in available_datetimes))
        text = (
            f"🤖 Бот для записи на время\n\n"
            f"Привет, {user_name}!\n\n"
            "Вы можете:\n"
            "• 📅 Записаться на доступное время\n"
            "• 📋 Посмотреть свои записи\n\n"
            "Особенности:\n"
            "• Можно записываться на одно время с другими\n"
            "• Администратор добавляет конкретные даты и время\n"
            "• Вы получите подтверждение записи\n\n"
            f"📊 Доступно: {available_dates_count} дат для записи"
        )
        await message.answer(text, reply_markup=get_main_keyboard())

# ============ ОБРАБОТКА НЕИЗВЕСТНЫХ СООБЩЕНИЙ ============
@dp.message()
async def handle_unknown(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем регистрацию
    if str(user_id) not in users:
        await message.answer(
            "❌ Для использования бота необходимо зарегистрироваться.\n"
            "Нажмите /start для начала регистрации."
        )
        return
    
    if is_creator(user_id):
        await message.answer(
            "Пожалуйста, используйте кнопки меню.\n"
            "Для начала работы нажмите /start",
            reply_markup=get_creator_keyboard()
        )
    else:
        await message.answer(
            "Пожалуйста, используйте кнопки меню.\n"
            "Для начала работы нажмите /start",
            reply_markup=get_main_keyboard()
        )

# ============ ОСНОВНАЯ ФУНКЦИЯ ============
async def main():
    logger.info("Бот запущен...")
    
    # Проверяем и создаем файлы если их нет
    if not os.path.exists(USERS_FILE):
        save_users({})
    
    if not os.path.exists(CREATORS_FILE):
        save_creators([MAIN_CREATOR_ID])
    
    if not os.path.exists(BOOKINGS_FILE):
        save_bookings([])
    
    if not os.path.exists(TIME_SLOTS_FILE):
        save_time_slots([])
    
    # Запускаем бота
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())