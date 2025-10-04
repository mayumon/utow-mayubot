import logging
import discord
from discord import app_commands
from discord.ext import commands
from .config import DISCORD_TOKEN

# initialize bot
intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"✅ logged in as {bot.user} (ID: {bot.user.id})")
    try:
        synced = await bot.tree.sync()
        print(f"✅ synced {len(synced)} command(s)")
    except Exception as e:
        print(f"❌ sync failed: {e}")


@bot.tree.command(name="ping", description="health check")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong", ephemeral=True)


def run():
    logging.basicConfig(level=logging.INFO)
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    run()