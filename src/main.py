import logging
import discord
from discord import app_commands
from discord.ext import commands
from .config import DISCORD_TOKEN
from .storage import *

# initialize bot
intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"✅ logged in as {bot.user} (ID: {bot.user.id})")
    try:
        init_db()
        synced = await bot.tree.sync()
        print(f"✅ synced {len(synced)} command(s)")
    except Exception as e:
        print(f"❌ sync failed: {e}")


# ------------------------ diagnostics ------------------------
@bot.tree.command(name="ping", description="health check")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong", ephemeral=True)


# ------------------------ helpers ------------------------
def staff_only(inter: discord.Interaction) -> bool:
    m = inter.user if isinstance(inter.user, discord.Member) else None
    return bool(m and m.guild_permissions.manage_guild)


# ------------------------ /setup ------------------------

setup = app_commands.Group(name="setup", description="configure tournaments")


# /setup new <slug>
@setup.command(name="new", description="create or update a tournament slug")
@app_commands.describe(slug="challonge tournament slug")
async def setup_new(inter: discord.Interaction, slug: str):

    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    upsert_settings(slug)
    await inter.response.send_message(f"linked slug `{slug}` to a new tournament", ephemeral=True)


# /setup channels <announcements> <match-chats>
@setup.command(name="channels", description="set announcement and match channels")
@app_commands.describe(slug="tournament slug",
                        announcements="announcements channel",
                        match_chats="match-chats channel",)
async def setup_channels(inter: discord.Interaction,
                         slug: str,
                         announcements: discord.TextChannel,
                         match_chats: discord.TextChannel):

    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)
    if not get_settings(slug):
        return await inter.response.send_message(f"`{slug}` not found; run `/setup new` first", ephemeral=True)
    set_channels(slug, announcements.id, match_chats.id)
    await inter.response.send_message(
        f"saved channels {announcements.mention} (announcements) and {match_chats.mention} (match threads) for `{slug}` tournament",
        ephemeral=True
    )

# /setup team add <@role> <challonge-team-id> TODO
# /setup team remove [@role] [challonge-team-id] TODO
# /setup team list TODO


# /setup status
@setup.command(name="status", description="show current config")
@app_commands.describe(slug="tournament slug")
async def setup_status(inter: discord.Interaction, slug: str):

    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    s = get_settings(slug)

    if not s:
        return await inter.response.send_message(f"tournament `{slug}` not found", ephemeral=True)

    teams = list_teams(slug)

    ann = f"<#{s['announcements_ch']}>" if s['announcements_ch'] else "-"
    mc = f"<#{s['match_chats_ch']}>" if s['match_chats_ch'] else "-"

    desc = "\n".join([
        f"**Tournament Slug:** `{slug}`",
        f"**Announcements Channel:** {ann}",
        f"**Match-chats Channel:** {mc}",
        f"**Teams mapped:** {len(teams)}",
    ])
    await inter.response.send_message(embed=discord.Embed(
        title="Setup status", description=desc, color=0xeec6db), ephemeral=True)


bot.tree.add_command(setup)

# ------------------------ /sync ------------------------

# /sync TODO

# ------------------------ /reminders ------------------------

# /reminders set [match_id] TODO

# /reminders list TODO

# ------------------------ /match ------------------------
# /match start <match_id> TODO

# /match poke <match_id> [time|standard] TODO

# /match settime <match_id> <YYYY-MM-DD HH:MM> TODO

# /match report <match_id> TODO TODO need to develop structure

# ------------------------ /tournament ------------------------

# /tournament list TODO

# /tournament standings TODO

# /tournament announcement <post:true|false> TODO

# ------------------------ misc. ------------------------

# /help TODO

def run():
    logging.basicConfig(level=logging.INFO)
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    run()