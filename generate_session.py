import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = 22275520          # <- put your real API_ID here (int)
API_HASH = "2fa908c209c73b52096afb82a18342b2"  # <- put your real API_HASH here (string)


async def main():
    # Create client with a temporary in-memory StringSession
    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        # This will open the login flow in the console (phone, code, 2FA etc.)
        string = client.session.save()
        print("\n=== YOUR TELEGRAM STRING SESSION ===\n")
        print(string)
        print("\n=== SAVE THIS STRING SOMEWHERE SAFE ===\n")


if __name__ == "__main__":
    asyncio.run(main())
