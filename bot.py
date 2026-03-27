import os
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from db import Database

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
db = Database()

# ==================== FSM STATES ====================
class AddMovie(StatesGroup):
    number = State()
    title = State()
    category = State()
    language = State()
    file_id = State()

class AddChannel(StatesGroup):
    name = State()
    link = State()

class RemoveNumber(StatesGroup):
    number = State()

# ==================== MAJBURIY KANAL TEKSHIRISH ====================
async def check_user_subscription(user_id):
    channels = await db.get_channels()
    if not channels:
        return True, []
    
    not_subscribed = []
    for ch in channels:
        try:
            username = ch['link'].replace("https://t.me/", "")
            chat_member = await bot.get_chat_member(f"@{username}", user_id)
            if chat_member.status in ['left', 'kicked']:
                not_subscribed.append(ch)
        except:
            not_subscribed.append(ch)
    
    return len(not_subscribed) == 0, not_subscribed

async def send_channels_list(message, not_subscribed):
    buttons = []
    for ch in not_subscribed:
        buttons.append([InlineKeyboardButton(text=f"📢 {ch['name']}", url=ch['link'])])
    buttons.append([InlineKeyboardButton(text="✅ TEKSHIRISH", callback_data="check_sub")])
    
    text = "⛔️ KIRISH TA'QIQLANADI!\n\nBotdan foydalanish uchun quyidagi kanallarga a'zo bo'ling:\n\n"
    for i, ch in enumerate(not_subscribed, 1):
        text += f"{i}. {ch['name']}\n"
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# ==================== ADMIN PANEL ====================
@dp.message(Command("panel"))
async def panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Siz admin emassiz!")
        return
    
    buttons = [
        [InlineKeyboardButton(text="➕ KINO QO'SHISH", callback_data="add_movie")],
        [InlineKeyboardButton(text="➖ KINO O'CHIRISH", callback_data="remove_movie")],
        [InlineKeyboardButton(text="📢 KANAL QO'SHISH", callback_data="add_channel")],
        [InlineKeyboardButton(text="🔇 KANAL O'CHIRISH", callback_data="remove_channel")],
        [InlineKeyboardButton(text="📊 STATISTIKA", callback_data="stats")],
        [InlineKeyboardButton(text="📋 KANALLAR", callback_data="list_channels")]
    ]
    await message.answer("🔧 ADMIN PANEL", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# ==================== KINO QO'SHISH ====================
@dp.callback_query(F.data == "add_movie")
async def add_movie_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("📝 KINO RAQAMI:")
    await state.set_state(AddMovie.number)
    await callback.answer()

@dp.message(AddMovie.number)
async def get_number(message: Message, state: FSMContext):
    try:
        number = int(message.text)
        existing = await db.get_movie(number)
        if existing:
            await message.answer(f"❌ {number} raqamli kino bor! Boshqa raqam:")
            return
        await state.update_data(number=number)
        await message.answer("📌 KINO NOMI:")
        await state.set_state(AddMovie.title)
    except:
        await message.answer("❌ Raqam kiriting!")

@dp.message(AddMovie.title)
async def get_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text)
    await message.answer("📂 KATEGORIYA:")
    await state.set_state(AddMovie.category)

@dp.message(AddMovie.category)
async def get_category(message: Message, state: FSMContext):
    await state.update_data(category=message.text)
    await message.answer("🌍 TIL:")
    await state.set_state(AddMovie.language)

@dp.message(AddMovie.language)
async def get_language(message: Message, state: FSMContext):
    await state.update_data(language=message.text)
    await message.answer("🎬 VIDEO FAYLNI YUBORING:")
    await state.set_state(AddMovie.file_id)

@dp.message(AddMovie.file_id, F.video)
async def get_video(message: Message, state: FSMContext):
    data = await state.get_data()
    await db.add_movie(
        number=data['number'],
        title=data['title'],
        category=data['category'],
        language=data['language'],
        link=message.video.file_id
    )
    await message.answer(f"✅ KINO QO'SHILDI!\n\n🎬 {data['title']}\n🔢 #{data['number']}")
    await state.clear()

@dp.message(AddMovie.file_id)
async def wrong_file(message: Message):
    await message.answer("❌ VIDEO fayl yuboring!")

# ==================== KINO O'CHIRISH ====================
@dp.callback_query(F.data == "remove_movie")
async def remove_movie_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("🗑 O'CHIRISH UCHUN KINO RAQAMI:")
    await state.set_state(RemoveNumber.number)
    await callback.answer()

@dp.message(RemoveNumber.number)
async def remove_number(message: Message, state: FSMContext):
    number = int(message.text)
    movie = await db.get_movie(number)
    if movie:
        await db.delete_movie(number)
        await message.answer(f"✅ O'CHIRILDI: {movie['title']}")
    else:
        await message.answer(f"❌ {number} raqamli kino topilmadi!")
    await state.clear()

# ==================== KANAL QO'SHISH ====================
@dp.callback_query(F.data == "add_channel")
async def add_channel_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("📢 KANAL NOMI (foydalanuvchiga ko'rsatiladi):")
    await state.set_state(AddChannel.name)
    await callback.answer()

@dp.message(AddChannel.name)
async def add_channel_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("🔗 KANAL LINKI (https://t.me/...):")
    await state.set_state(AddChannel.link)

@dp.message(AddChannel.link)
async def add_channel_link(message: Message, state: FSMContext):
    data = await state.get_data()
    name = data['name']
    link = message.text.strip()
    
    if link.startswith("@"):
        link = f"https://t.me/{link[1:]}"
    elif not link.startswith("http"):
        link = f"https://t.me/{link}"
    
    await db.add_channel(name, link)
    await message.answer(f"✅ KANAL QO'SHILDI!\n\n📢 {name}\n🔗 {link}")
    await state.clear()

# ==================== KANAL O'CHIRISH ====================
@dp.callback_query(F.data == "remove_channel")
async def remove_channel_start(callback: CallbackQuery):
    channels = await db.get_channels()
    if not channels:
        await callback.message.delete()
        await callback.message.answer("📭 Hech qanday kanal yo'q!")
        await callback.answer()
        return
    
    buttons = []
    for ch in channels:
        buttons.append([InlineKeyboardButton(text=f"🗑 {ch['name']}", callback_data=f"del_{ch['link']}")])
    buttons.append([InlineKeyboardButton(text="❌ BEKOR", callback_data="cancel_del")])
    
    await callback.message.delete()
    await callback.message.answer("🔇 O'CHIRISH UCHUN KANALNI TANLANG:", 
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@dp.callback_query(F.data.startswith("del_"))
async def remove_channel_confirm(callback: CallbackQuery):
    link = callback.data.replace("del_", "")
    await db.remove_channel(link)
    await callback.message.delete()
    await callback.message.answer("✅ KANAL O'CHIRILDI!")
    await callback.answer()

@dp.callback_query(F.data == "cancel_del")
async def cancel_remove(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer("Bekor qilindi")

# ==================== STATISTIKA ====================
@dp.callback_query(F.data == "stats")
async def stats(callback: CallbackQuery):
    users = await db.count_users()
    cats = await db.stats_by_category()
    langs = await db.stats_by_language()
    channels = await db.get_channels()
    
    text = f"📊 STATISTIKA\n\n👥 Foydalanuvchilar: {users}\n\n📢 Majburiy kanallar: {len(channels)} ta\n"
    for ch in channels:
        text += f"  • {ch['name']}\n"
    text += "\n📂 Kategoriyalar:\n"
    for c, v in cats.items():
        text += f"  • {c}: {v} ta\n"
    text += "\n🌍 Tillar:\n"
    for l, v in langs.items():
        text += f"  • {l}: {v} ta\n"
    
    await callback.message.delete()
    await callback.message.answer(text)
    await callback.answer()

# ==================== KANALLAR RO'YXATI ====================
@dp.callback_query(F.data == "list_channels")
async def list_channels(callback: CallbackQuery):
    channels = await db.get_channels()
    if channels:
        text = "📢 MAJBURIY KANALLAR:\n\n"
        for i, ch in enumerate(channels, 1):
            text += f"{i}. {ch['name']}\n   {ch['link']}\n\n"
    else:
        text = "📭 Hech qanday majburiy kanal yo'q!"
    
    await callback.message.delete()
    await callback.message.answer(text)
    await callback.answer()

# ==================== TEKSHIRISH ====================
@dp.callback_query(F.data == "check_sub")
async def check_subscription(callback: CallbackQuery):
    user_id = callback.from_user.id
    await db.add_user(user_id, callback.from_user.username, callback.from_user.first_name)
    
    is_subscribed, not_subscribed = await check_user_subscription(user_id)
    
    if is_subscribed:
        await callback.message.delete()
        await callback.message.answer("✅ KANALLARGA A'ZOLIK TASDIQLANDI!\n\n🎬 Endi kino raqamini yuboring:")
    else:
        buttons = []
        for ch in not_subscribed:
            buttons.append([InlineKeyboardButton(text=f"📢 {ch['name']}", url=ch['link'])])
        buttons.append([InlineKeyboardButton(text="✅ TEKSHIRISH", callback_data="check_sub")])
        
        text = "⛔️ SIZ HAMON QUYIDAGI KANALLARGA A'ZO EMASSIZ!\n\n"
        for i, ch in enumerate(not_subscribed, 1):
            text += f"{i}. {ch['name']}\n"
        text += "\nA'zo bo'ling va qayta tekshiring!"
        
        try:
            await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        except:
            await callback.message.delete()
            await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    
    await callback.answer()

# ==================== FOYDALANUVCHI QISMI ====================
@dp.message(CommandStart())
async def start(message: Message):
    user_id = message.from_user.id
    await db.add_user(user_id, message.from_user.username, message.from_user.first_name)
    
    is_subscribed, not_subscribed = await check_user_subscription(user_id)
    
    if is_subscribed:
        await message.answer(f"🎬 Salom {message.from_user.first_name}!\n\nKino raqamini yuboring:")
    else:
        await send_channels_list(message, not_subscribed)

@dp.message()
async def get_movie(message: Message):
    user_id = message.from_user.id
    
    is_subscribed, not_subscribed = await check_user_subscription(user_id)
    
    if not is_subscribed:
        await send_channels_list(message, not_subscribed)
        return
    
    try:
        number = int(message.text)
        movie = await db.get_movie(number)
        
        if movie:
            await message.answer_video(
                video=movie['link'],
                caption=f"🎬 {movie['title']}\n\n📂 {movie['category']}\n🌍 {movie['language']}\n🔢 #{movie['number']}"
            )
        else:
            await message.answer(f"❌ {number} raqamli kino topilmadi!")
    except:
        await message.answer("❌ Kino raqamini yuboring! Masalan: 101")

# ==================== MAIN ====================
async def main():
    await db.connect()
    print("✅ BOT ISHLADI!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
