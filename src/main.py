import logging
import discord
from discord import app_commands
from discord.ext import commands, tasks
from .config import DISCORD_TOKEN
from .storage import *
from .swiss_helpers import *
from collections import defaultdict
import random
from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo
import asyncio

# initialize bot
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

log = logging.getLogger("reminders")


@bot.event
async def on_ready():
    print(f"✅ logged in as {bot.user} (ID: {bot.user.id})")
    try:
        init_db()
        synced = await bot.tree.sync()
        print(f"✅ synced {len(synced)} command(s)")
    except Exception as e:
        print(f"❌ sync failed: {e}")

    await asyncio.create_task(reminder_worker(bot))


# ------------------------ diagnostics ------------------------
@bot.tree.command(name="ping", description="health check")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong", ephemeral=True)


# ------------------------ helpers ------------------------
def staff_only(inter: discord.Interaction) -> bool:
    m = inter.user if isinstance(inter.user, discord.Member) else None
    return bool(m and m.guild_permissions.manage_guild)


def _parse_local_dt(s: str, tzname: str) -> datetime | None:
    # s: "YYYY-MM-DD HH:MM" (local)
    try:
        naive = datetime.strptime(s, "%Y-%m-%d %H:%M")
        return naive.replace(tzinfo=ZoneInfo(tzname))
    except Exception:
        return None

# ------------------------ /setup ------------------------


setup = app_commands.Group(name="setup", description="configure tournaments")


# /setup new <tournament_id>
@setup.command(name="new", description="create a new tournament")
@app_commands.describe(tournament_id="tournament ID")
async def setup_new(inter: discord.Interaction, tournament_id: str):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    upsert_settings(tournament_id)
    await inter.response.send_message(f"linked ID `{tournament_id}` to a new tournament.", ephemeral=False)


# /setup channels <announcements> <match-chats>
@setup.command(name="channels", description="set announcement and match channels")
@app_commands.describe(tournament_id="tournament ID",
                       announcements="announcements channel",
                       match_chats="match-chats channel", )
async def setup_channels(inter: discord.Interaction,
                         tournament_id: str,
                         announcements: discord.TextChannel,
                         match_chats: discord.TextChannel):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.",
                                                 ephemeral=True)

    set_channels(tournament_id, announcements.id, match_chats.id)

    await inter.response.send_message(
        f"saved channels {announcements.mention} (announcements) and {match_chats.mention} (match threads) for `{tournament_id}` tournament.",
        ephemeral=False
    )

team = app_commands.Group(name="team", description="map/manage teams", parent=setup)


# /setup team add <@role>
@team.command(name="add", description="map a discord team role (auto-assigns team id)")
@app_commands.describe(
    tournament_id="tournament ID",
    role="discord team role"
)
async def setup_team_add(
        inter: discord.Interaction,
        tournament_id: str,
        role: discord.Role
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.",
                                                 ephemeral=True)

    try:
        assigned_id = link_team(tournament_id, team_role_id=role.id, team_id=None)
    except TeamIdInUseError:
        return await inter.response.send_message(
            f"could not assign a unique team id for `{tournament_id}`. try again.",
            ephemeral=True
        )

    await inter.response.send_message(
        embed=discord.Embed(
            title="new team mapped",
            description="\n".join([
                f"**tournament ID:** `{tournament_id}`",
                f"**role:** {role.mention} (`{role.id}`)",
                f"**team ID:** `{assigned_id}`",
            ]),
            color=0xB54882
        ),
        ephemeral=False
    )


# /setup team remove [@role] [challonge-team-id]
@team.command(name="remove", description="unmap a team by Discord role or team ID")
@app_commands.describe(
    tournament_id="tournament ID",
    role="discord team role (optional)",
    team_id="team ID (optional)"
)
async def setup_team_remove(
    inter: discord.Interaction,
    tournament_id: str,
    role: discord.Role | None = None,
    team_id: int | None = None
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    # tournament must exist
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.",
                                                 ephemeral=True)

    # need at least one selector
    if role is None and team_id is None:
        return await inter.response.send_message(
            "provide either a **role** or a **team_id**.", ephemeral=True
        )

    # resolve mapping
    mapping = None
    if role is not None:
        mapping = get_team_by_role(role.id)
        if mapping and mapping.get("tournament_name") != tournament_id:
            mapping = None
    elif team_id is not None:
        mapping = get_team_by_participant(tournament_id, team_id)

    if not mapping:
        return await inter.response.send_message(
            "no matching team mapping found for the given input.", ephemeral=True
        )

    rid = int(mapping["team_role_id"])
    tid = int(mapping["team_id"])

    # unlink
    unlink_team(rid)

    await inter.response.send_message(
        embed=discord.Embed(
            title="team mapping removed",
            description="\n".join([
                f"**tournament:** `{tournament_id}`",
                f"**role:** <@&{rid}> (`{rid}`)",
                f"**team id:** `{tid}`",
            ]),
            color=0xB54882
        ),
        ephemeral=False
    )


# /setup team list
@team.command(name="list", description="list mapped teams for a tournament")
@app_commands.describe(tournament_id="tournament ID")
async def setup_team_list(inter: discord.Interaction, tournament_id: str):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.",
                                                 ephemeral=True)

    rows = list_teams(tournament_id)
    if not rows:
        return await inter.response.send_message(f"no teams mapped yet for `{tournament_id}`.", ephemeral=True)

    plain_map = team_label_map(tournament_id, inter.guild, plain=True)

    lines = []
    for r in sorted(rows, key=lambda r: int(r["team_id"])):
        rid = int(r["team_role_id"])
        tid = int(r["team_id"])
        label = plain_map.get(rid, f"role:{rid}")
        lines.append(f"• **{label}** - team_id `{tid}` - role <@&{rid}>")

    embed = discord.Embed(title=f"Teams - {tournament_id}", color=0xB54882)

    chunk = []
    count = 0
    for i, line in enumerate(lines, 1):
        chunk.append(line);
        count += 1
        if count >= 20:
            embed.add_field(name="\u200b", value="\n".join(chunk), inline=False)
            chunk, count = [], 0
    if chunk:
        embed.add_field(name="\u200b", value="\n".join(chunk), inline=False)

    await inter.response.send_message(embed=embed, ephemeral=True)


# /setup status
@setup.command(name="status", description="show current configurations")
@app_commands.describe(tournament_id="tournament ID")
async def setup_status(inter: discord.Interaction, tournament_id: str):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    s = get_settings(tournament_id)

    if not s:
        return await inter.response.send_message(f"tournament `{tournament_id}` not found", ephemeral=True)

    teams = list_teams(tournament_id)

    ann = f"<#{s['announcements_ch']}>" if s['announcements_ch'] else "-"
    mc = f"<#{s['match_chats_ch']}>" if s['match_chats_ch'] else "-"

    desc = "\n".join([
        f"**tournament ID:** `{tournament_id}`",
        f"**announcements channel:** {ann}",
        f"**match-chats channel:** {mc}",
        f"**teams mapped:** {len(teams)}",
    ])
    await inter.response.send_message(embed=discord.Embed(
        title="setup status", description=desc, color=0xeec6db), ephemeral=True)


bot.tree.add_command(setup)

# ------------------------ /reminders ------------------------

reminders = app_commands.Group(name="reminders", description="match reminders to match threads")


# /reminders set [match_id]
@reminders.command(name="set", description="schedule reminders (1h + noon/2h) for a match or all matches")
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match ID (optional; if omitted, schedules for all matches with times)"
)
async def reminders_set(
    inter: discord.Interaction,
    tournament_id: str,
    match_id: int | None = None
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.", ephemeral=True)

    if match_id is not None:
        m = get_match(tournament_id, match_id)
        if not m:
            return await inter.response.send_message(
                f"match `#{match_id}` not found for `{tournament_id}`.", ephemeral=True
            )
        if not m.get("start_time_local"):
            return await inter.response.send_message(
                f"match `#{match_id}` has no scheduled time yet.", ephemeral=True
            )
        n = schedule_match_reminders(tournament_id, match_id)
        m2 = get_match(tournament_id, match_id)
        has_thread = bool(m2 and m2.get("thread_id"))
        suffix = " (no thread - reminders will not post)" if not has_thread else ""
        return await inter.response.send_message(
            f"scheduled {n} reminder(s) for match `#{match_id}`{suffix}.",
            ephemeral=True
        )
    else:
        matches_updated, reminders_total = schedule_all_match_reminders(tournament_id)
        rows = list_matches(tournament_id, with_time_only=True)
        no_thread = [r["match_id"] for r in rows if r.get("start_time_local") and not r.get("thread_id")]
        suffix = ""
        if no_thread:
            preview = ", ".join(f"#{int(x)}" for x in no_thread[:5])
            more = "" if len(no_thread) <= 5 else f" (+{len(no_thread) - 5} more)"
            suffix = f"\n⚠ {len(no_thread)} match(es) have no thread: {preview}{more}. Reminders for these will not post."

        return await inter.response.send_message(
            f"scheduled reminders for **{matches_updated}** match(es), **{reminders_total}** reminder(s) total.{suffix}",
            ephemeral=True
        )


# /reminders list
@reminders.command(name="list", description="list scheduled reminders (pending & sent)")
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match ID (optional)"
)
async def reminders_list(
    inter: discord.Interaction,
    tournament_id: str,
    match_id: int | None = None
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.", ephemeral=True)

    rows = list_reminders(tournament_id, match_id)
    if not rows:
        return await inter.response.send_message("no reminders scheduled.", ephemeral=True)

    # resolve tournament timezone
    s = get_settings(tournament_id)
    tz = s["tz"] if s and s.get("tz") else "America/Toronto"
    tzinfo = safe_zoneinfo(tz)

    # group by match
    from collections import defaultdict
    by_match = defaultdict(list)
    for r in rows:
        by_match[int(r["match_id"])].append(r)

    embed = discord.Embed(
        title=f"Reminders - {tournament_id}",
        color=0xB54882
    )
    embed.set_footer(text=f"All times shown in {tz}")

    def fmt_row(r):
        status = "✅ sent" if int(r["sent"]) else "⏳ pending"
        # convert stored UTC (string) -> local display
        dt_utc = datetime.strptime(r["when_utc"], "%Y-%m-%d %H:%M").replace(tzinfo=safe_zoneinfo("UTC"))
        dt_loc = dt_utc.astimezone(tzinfo)
        local_txt = dt_loc.strftime("%Y-%m-%d %H:%M")
        return f"- `{r['kind']}` at `{local_txt}` - {status}", dt_utc

    for mid in sorted(by_match.keys()):

        triples = []
        for r in by_match[mid]:
            line, dt_utc = fmt_row(r)
            kind = r.get("kind", "")
            triples.append((dt_utc, kind, line))

        triples.sort(key=lambda x: (x[0], x[1]))
        lines = [t[2] for t in triples]

        if not lines:
            continue

        chunk, acc_len = [], 0
        for ln in lines:
            chunk.append(ln)
            acc_len += len(ln)
            if acc_len > 900:
                embed.add_field(name=f"match #{mid}", value="\n".join(chunk), inline=False)
                chunk, acc_len = [], 0
        if chunk:
            embed.add_field(name=f"match #{mid}", value="\n".join(chunk), inline=False)

    await inter.response.send_message(embed=embed, ephemeral=True)


bot.tree.add_command(reminders)

# ------------------------ /match ------------------------

match = app_commands.Group(name="match", description="match utilities")

thread = app_commands.Group(name="thread", description="manage match threads", parent=match)


# /match thread create <match_id>
@thread.command(name="create", description="create a private match thread for the two teams in a match.")
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match ID"
)
async def match_thread(
    inter: discord.Interaction,
    tournament_id: str,
    match_id: int
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    # --- config checks ---
    s = get_settings(tournament_id)
    if not s:
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.", ephemeral=True)

    ch_id = s.get("match_chats_ch")
    if not ch_id:
        return await inter.response.send_message(
            "match-chats channel not set. run `/setup channels` first.",
            ephemeral=True
        )

    if not inter.guild:
        return await inter.response.send_message("this command must be used in a server.", ephemeral=True)

    channel = inter.guild.get_channel(ch_id)
    if not isinstance(channel, discord.TextChannel):
        return await inter.response.send_message("configured match-chats channel is not a text channel.", ephemeral=True)

    m = get_match(tournament_id, match_id)
    if not m:
        return await inter.response.send_message(f"match `#{match_id}` not found for `{tournament_id}`.", ephemeral=True)

    a_id = m.get("team_a_role_id")
    b_id = m.get("team_b_role_id")
    if not a_id or not b_id:
        return await inter.response.send_message(
            f"match `#{match_id}` does not have both teams assigned yet.", ephemeral=True
        )

    a_role = inter.guild.get_role(a_id)
    b_role = inter.guild.get_role(b_id)
    if not a_role or not b_role:
        return await inter.response.send_message(
            "one or both team roles no longer exist on this server.", ephemeral=True
        )

    # ---------- title ----------
    def _phase_prefix(phase: str, round_no: int | None) -> str:
        rn = str(round_no) if round_no is not None else "?"
        p = (phase or "").lower()
        if p == "playoff":
            return f"playoffs round {rn}"
        if p == "swiss":
            return f"swiss round {rn}"
        if p == "roundrobin":
            return f"round robin {rn}"
        if p == "double_elim":
            return f"double elimination {rn}"
        return f"round {rn}"

    phase = (m.get("phase") or "").lower()
    round_no = m.get("round_no")
    prefix = _phase_prefix(phase, round_no)

    base_title = f"{prefix}: {a_role.name} vs {b_role.name}"
    title = base_title[:100]

    # ---------- create thread ----------
    try:
        thread = await channel.create_thread(
            name=title,
            type=discord.ChannelType.private_thread,
            invitable=True,
            auto_archive_duration=10080
        )
    except discord.Forbidden:
        return await inter.response.send_message(
            "i dont have permission to create private threads in the configured channel.",
            ephemeral=True
        )
    except Exception as e:
        return await inter.response.send_message(f"failed to create thread: {e}", ephemeral=True)

    # save thread id
    try:
        set_thread(tournament_id, match_id, thread.id)
    except Exception:
        pass

    # format scheduled time
    when_txt = ""
    raw = m.get("start_time_local")
    if raw:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M")
            when_txt = f"{dt.strftime('%B')} {dt.day}, {dt.strftime('%A')} at {dt.strftime('%I:%M%p').lstrip('0')}"
        except Exception:
            when_txt = raw  # fallback

    # mention roles
    mention_text = f"{a_role.mention} {b_role.mention}"
    body_lines = [
        mention_text,
        "use this thread to coordinate your match time!",
    ]
    if when_txt:
        body_lines.append(f"current scheduled time: **{when_txt}**")

    allowed = discord.AllowedMentions(roles=True, users=False, everyone=False)
    try:
        await thread.send("\n".join(body_lines), allowed_mentions=allowed)
    except Exception:
        pass

    # proactively invite role members
    invited = 0
    for role in (a_role, b_role):
        try:
            for member in role.members:
                try:
                    await thread.add_user(member)
                    invited += 1
                except discord.Forbidden:
                    continue
                except Exception:
                    continue
        except Exception:
            continue

    no_pings = discord.AllowedMentions(everyone=False, users=False, roles=False)
    try:
        await channel.send(f"**▶ match thread created:** {thread.mention}", allowed_mentions=no_pings)
    except Exception:
        pass

    await inter.response.send_message(
        embed=discord.Embed(
            title="match thread created",
            description="\n".join([
                f"**channel:** {thread.mention}",
                f"**title:** {title}",
                f"**invited members:** {invited}",
            ]),
            color=0xB54882
        ),
        ephemeral=False
    )


# /match poke <match_id> [time|standard] TODO

# /match settime <tournament_id> <match_id> <YYYY-MM-DD HH:MM>
@match.command(
    name="settime",
    description="set (or update) the scheduled start time for a match."
)
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match ID",
    when="date/time in 24h format: YYYY-MM-DD HH:MM"
)
async def match_settime(
    inter: discord.Interaction,
    tournament_id: str,
    match_id: int,
    when: str
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms,", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.", ephemeral=True)

    m = get_match(tournament_id, match_id)
    if not m:
        return await inter.response.send_message(f"match `#{match_id}` not found for `{tournament_id}`.", ephemeral=True)

    # parse time input
    try:
        dt = datetime.strptime(when, "%Y-%m-%d %H:%M")
    except ValueError:
        return await inter.response.send_message(
            "invalid time. use **YYYY-MM-DD HH:MM** (24h).",
            ephemeral=True
        )

    # save to db
    set_match_time(tournament_id, match_id, dt.strftime("%Y-%m-%d %H:%M"))

    # regenerate this match's reminders
    try:
        scheduled = schedule_match_reminders(tournament_id, match_id)
        scheduled_msg = f"scheduled {scheduled} reminder(s)."
    except Exception as e:
        scheduled = 0
        scheduled_msg = f"couldn't schedule reminders ({e})."

    # format
    pretty = f"{dt.strftime('%B')} {dt.day}, {dt.strftime('%A')} at {dt.strftime('%I:%M%p').lstrip('0')}"

    # announce in thread (if exists)
    posted_update = False
    mentioned = False
    try:
        a_id, b_id = m.get("team_a_role_id"), m.get("team_b_role_id")
        a_mention = f"<@&{a_id}>" if a_id else ""
        b_mention = f"<@&{b_id}>" if b_id else ""
        mention_text = f"{a_mention} {b_mention}".strip()

        thread_id = m.get("thread_id")
        if thread_id and inter.guild:
            thread = inter.guild.get_thread(thread_id)
            if thread:
                await thread.send(
                    f"{mention_text}\n🕑 match date/time updated: **{pretty}**",
                    allowed_mentions=discord.AllowedMentions(roles=True, users=False, everyone=False)
                )
                posted_update = True
                mentioned = True
    except Exception:
        pass

    await inter.response.send_message(
        embed=discord.Embed(
            title="match time set",
            description="\n".join([
                f"**match:** #{match_id}",
                f"**time:** {pretty}",
                ("_update posted and teams pinged in thread_" if mentioned else
                 "_update posted in thread (no pings)_" if posted_update else
                 "_no thread to update_"),
                f"**reminders:** {scheduled_msg}",
            ]),
            color=0xB54882
        ),
        ephemeral=True
    )

# /match setteam <tournament_id> <match_id> <@team_a> <@team_b>
@match.command(
    name="setteam",
    description="assign Team A and Team B (roles) to a match placeholder"
)
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match ID",
    team_a="Discord role for Team A",
    team_b="Discord role for Team B",
)
async def match_setteam(
    inter: discord.Interaction,
    tournament_id: str,
    match_id: int,
    team_a: discord.Role,
    team_b: discord.Role,
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.", ephemeral=True)

    m = get_match(tournament_id, match_id)
    if not m:
        return await inter.response.send_message(f"match `#{match_id}` not found for `{tournament_id}`.", ephemeral=True)
    if team_a.id == team_b.id:
        return await inter.response.send_message("team A and team B must be different roles.", ephemeral=True)

    # optional: validate the roles are mapped to this tournament
    mapped_ids = {int(r["team_role_id"]) for r in list_teams(tournament_id)}
    if team_a.id not in mapped_ids or team_b.id not in mapped_ids:
        return await inter.response.send_message(
            "both roles must be mapped to this tournament (`/setup team add`).", ephemeral=True
        )

    # write
    set_match_teams(tournament_id, match_id, team_a_role_id=team_a.id, team_b_role_id=team_b.id)

    await inter.response.send_message(
        embed=discord.Embed(
            title="teams assigned",
            description="\n".join([
                f"**match:** #{match_id}",
                f"**team A:** {team_a.mention}",
                f"**team B:** {team_b.mention}",
            ]),
            color=0xB54882
        ),
        ephemeral=True
    )


# /match report <tournament_id> <match_id> <score_a> <score_b>
@match.command(name="report", description="report a match score")
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match ID",
    score_a="maps for team A",
    score_b="maps for team B"
)
async def match_report(inter: discord.Interaction, tournament_id: str, match_id: int, score_a: int, score_b: int):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)
    if score_a < 0 or score_b < 0:
        return await inter.response.send_message("scores must be non-negative integers.", ephemeral=True)

    m = get_match(tournament_id, match_id)
    if not m:
        return await inter.response.send_message(
            f"match `#{match_id}` not found for `{tournament_id}`.", ephemeral=True
        )

    # guard: both teams assigned
    a = m.get("team_a_role_id")
    b = m.get("team_b_role_id")
    if not a or not b:
        return await inter.response.send_message(
            "cannot report yet: this match does not have both teams assigned.", ephemeral=True
        )

    # write scores + update team records atomically
    try:
        record_result_and_update_team_records(tournament_id, match_id, score_a, score_b)
    except MatchUpdateError as e:
        return await inter.response.send_message(f"couldn't record result: {e}", ephemeral=True)
    except Exception as e:
        # unexpected error path
        return await inter.response.send_message(f"error recording result: {e}", ephemeral=True)

    label = f"<@&{a}> vs <@&{b}>"
    tie_note = " (tie - no W/L changes)" if score_a == score_b else ""
    await inter.response.send_message(
        f"recorded result for {label}: **{score_a}–{score_b}**{tie_note}.",
        ephemeral=True
    )


# /match add <tournament_id> <swiss|double_elim|roundrobin> [rounds] <start_time>
@match.command(
    name="add",
    description="generate and add rounds for swiss, round-robin, or double-elim"
)
@app_commands.describe(
    tournament_id="tournament ID",
    kind="swiss/double_elim/roundrobin",
    rounds="number of rounds (default 1)",
    start_time="local start time for round 1 in YYYY-MM-DD HH:MM (24h) format"
)
async def match_add(
    inter: discord.Interaction,
    tournament_id: str,
    kind: Literal["swiss", "double_elim", "roundrobin"],
    rounds: int = 1,
    start_time: str = "",
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.",
                                                 ephemeral=True)
    if rounds < 1:
        return await inter.response.send_message("`rounds` must be ≥ 1.", ephemeral=True)

    # parse round 1 baseline time
    try:
        base_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
    except Exception:
        return await inter.response.send_message(
            "invalid `start_time` format. use `YYYY-MM-DD HH:MM`", ephemeral=True
        )

    created_blocks: list[str] = []
    phase = kind
    latest = get_latest_round(tournament_id, phase)  # highest round number already created

    # team pool
    mapped = list_teams(tournament_id)
    team_ids = [int(row["team_role_id"]) for row in mapped]
    if len(team_ids) < 2:
        return await inter.response.send_message("need at least 2 mapped teams. map with `/setup team`.", ephemeral=True)

    # helper display
    name_map_plain = team_label_map(tournament_id, inter.guild, plain=True)

    def label(role_id: int | None) -> str:
        if role_id is None:
            return "BYE"
        return name_map_plain.get(role_id, f"role:{role_id}")

    def gen_roundrobin_pairs(ids: list[int]) -> list[list[tuple[int | None, int | None]]]:
        players = ids[:]
        if len(players) % 2 == 1:
            players.append(None)  # BYE
        n = len(players)
        rounds = n - 1

        schedule: list[list[tuple[int | None, int | None]]] = []
        arr = players[:]  # working order

        for _ in range(rounds):
            # pair ends toward the middle
            pairs: list[tuple[int | None, int | None]] = []
            for i in range(n // 2):
                a = arr[i]
                b = arr[n - 1 - i]
                pairs.append((a, b))
            schedule.append(pairs)

            # rotate everyone except the first element
            arr = [arr[0]] + [arr[-1]] + arr[1:-1]

        return schedule

    def seeded_team_ids_for_de(tournament_id: str) -> list[int]:
        """Seeding = ascending team_id. Returns role_ids in seed order 1..N."""
        rows = list_teams(tournament_id)
        rows.sort(key=lambda r: int(r["team_id"]))
        return [int(r["team_role_id"]) for r in rows]

    def de_template_counts(num_teams: int, round_no: int) -> list[tuple[str, int]]:
        """
        return list of (bracket_tag, count) for the given round_no (1..4).
        Supported sizes: 4, 6, 8. Bracket tags:
          - WB, LB, GF as usual
          - LCQ = lower-seed play-ins (6-team Round 1)
          - 3P  = 3rd place match (6-team Round 4)
          - 4P  = 4th place match (8-team Round 3)
        """
        if num_teams == 4:
            plan = {
                1: [("WB", 2)],  # 1v4, 2v3
                2: [("WB", 1), ("LB", 1)],  # upper final + LB
                3: [("LB", 1)],  # loser final
                4: [("GF", 1)],  # grand final
            }
            return plan.get(round_no, [])

        if num_teams == 6:
            plan = {
                1: [("LCQ", 2)],  # 3v6, 4v5
                2: [("WB", 2)],  # winners vs 1st/2nd seeds
                3: [("WB", 1), ("LB", 1)],  # upper final + LB
                4: [("3P", 1), ("GF", 1)],  # 3rd place + grand final
            }
            return plan.get(round_no, [])

        if num_teams == 8:
            plan = {
                1: [("WB", 4)],  # quarters
                2: [("WB", 2), ("LB", 2)],  # semis + LB (2 upper, 4 lower participants)
                3: [("WB", 1), ("LB", 1), ("4P", 1)],  # upper final + loser semi + 4th-place match
                4: [("LB", 1), ("GF", 1)],  # loser final + grand final
            }
            return plan.get(round_no, [])

        return []

    # Create rounds
    for _ in range(rounds):
        target_round = latest + 1

        # compute start times
        r_start_dt = base_dt + timedelta(weeks=(target_round - 1))
        r_start_str = r_start_dt.strftime("%Y-%m-%d %H:%M")
        round_date_pretty = r_start_dt.strftime("%B %d, %Y at %I:%M%p")

        if round_exists(tournament_id, target_round, phase):
            created_blocks.append(f"》round {target_round}\n(already exists)\n\n[{round_date_pretty}]")
            latest = target_round
            continue

        lines: list[str] = [f"》round {target_round}"]

        if phase == "swiss":
            # swiss requires an even number of teams
            if len(team_ids) % 2 != 0:
                return await inter.response.send_message(
                    "swiss requires an **even** number of teams.", ephemeral=True
                )
            if latest == 0 and target_round == 1:
                # round 1: random pairings now
                ids = team_ids[:]
                random.shuffle(ids)
                pairings = []
                for i in range(0, len(ids), 2):
                    a, b = ids[i], ids[i+1]
                    pairings.append({
                        "match_id": None,
                        "team_a_role_id": a,
                        "team_b_role_id": b,
                        "start_time_local": r_start_str
                    })
                assigned = create_round(tournament_id, target_round, pairings, phase=phase)
                for i, mid in enumerate(assigned):
                    a = label(pairings[i]["team_a_role_id"]); b = label(pairings[i]["team_b_role_id"])
                    lines.append(f"☆ match #{mid}:\n{a} vs {b} ━ score: -")
            else:
                # later rounds: placeholders
                match_count = len(team_ids) // 2
                pairings = [{
                    "match_id": None,
                    "team_a_role_id": None,
                    "team_b_role_id": None,
                    "start_time_local": r_start_str
                } for _ in range(match_count)]
                assigned = create_round(tournament_id, target_round, pairings, phase=phase)
                for mid in assigned:
                    lines.append(f"☆ match #{mid}: TBD vs TBD ━ score: -")

        elif phase == "roundrobin":
            # RR can handle odd teams (BYE).
            rr = gen_roundrobin_pairs(team_ids)
            rr_round = rr[(target_round - 1) % len(rr)]
            pairings = []
            for a, b in rr_round:
                if a is None or b is None:
                    continue  # skip BYE match
                pairings.append({
                    "match_id": None,
                    "team_a_role_id": a,
                    "team_b_role_id": b,
                    "start_time_local": r_start_str
                })
            if not pairings:
                pairings = [{
                    "match_id": None,
                    "team_a_role_id": None,
                    "team_b_role_id": None,
                    "start_time_local": r_start_str
                }]
            assigned = create_round(tournament_id, target_round, pairings, phase=phase)
            for i, mid in enumerate(assigned):
                a = label(pairings[i]["team_a_role_id"]); b = label(pairings[i]["team_b_role_id"])
                a = a if a != "BYE" else "TBD"; b = b if b != "BYE" else "TBD"
                lines.append(f"☆ match #{mid}:\n{a} vs {b} ━ score: -")


        elif phase == "double_elim":

            if target_round > 4:
                created_blocks.append(f"》round {target_round}\n(skipped: DE is capped to 4 rounds)\n")
                latest = target_round
                continue
            n = len(team_ids)
            if n not in (4, 6, 8):
                return await inter.response.send_message(
                    "double_elim supports **4**, **6**, or **8** teams (4 rounds only).", ephemeral=True
                )
            # Round 1: create seeded pairings when specification requires them
            if target_round == 1:
                seeds = seeded_team_ids_for_de(tournament_id)
                pairings = []
                if n == 4:
                    # 1v4, 2v3
                    pairs = [(seeds[0], seeds[3]), (seeds[1], seeds[2])]
                    for a, b in pairs:
                        pairings.append({
                            "match_id": None, "team_a_role_id": a, "team_b_role_id": b,
                            "start_time_local": r_start_str, "bracket": "WB"
                        })

                elif n == 6:
                    # LCQ: 3v6, 4v5
                    pairs = [(seeds[2], seeds[5]), (seeds[3], seeds[4])]
                    for a, b in pairs:
                        pairings.append({
                            "match_id": None, "team_a_role_id": a, "team_b_role_id": b,
                            "start_time_local": r_start_str, "bracket": "LCQ"
                        })

                else:  # n == 8
                    # Quarters (typical seeding path): 1v8, 4v5, 3v6, 2v7
                    pairs = [(seeds[0], seeds[7]), (seeds[3], seeds[4]),
                             (seeds[2], seeds[5]), (seeds[1], seeds[6])]
                    for a, b in pairs:
                        pairings.append({
                            "match_id": None, "team_a_role_id": a, "team_b_role_id": b,
                            "start_time_local": r_start_str, "bracket": "WB"
                        })
                assigned = create_round(tournament_id, target_round, pairings, phase=phase)
                for i, mid in enumerate(assigned):
                    br = pairings[i]["bracket"]
                    a = label(pairings[i]["team_a_role_id"]);
                    b = label(pairings[i]["team_b_role_id"])
                    lines.append(f"☆ match #{mid} ({br}):\n{a} vs {b} ━ score: -")

            else:
                # Later rounds: placeholders follow the template
                counts = de_template_counts(n, target_round)
                if not counts:
                    created_blocks.append(f"》round {target_round}\n(no matches in template)\n")
                    latest = target_round
                    continue
                pairings = []
                for br, cnt in counts:
                    for _ in range(cnt):
                        pairings.append({
                            "match_id": None,
                            "team_a_role_id": None,
                            "team_b_role_id": None,
                            "start_time_local": r_start_str,
                            "bracket": br
                        })
                assigned = create_round(tournament_id, target_round, pairings, phase=phase)
                cursor = 0
                for br, cnt in counts:
                    for _ in range(cnt):
                        lines.append(f"☆ match #{assigned[cursor]} ({br}): TBD vs TBD ━ score: -")
                        cursor += 1

        # one date line per round
        lines.append(f"[{round_date_pretty}]")
        created_blocks.append("\n".join(lines))
        latest = target_round  # advance

    await inter.response.send_message(
        embed=discord.Embed(
            title=f"{kind.replace('_',' ').title()} rounds created - {tournament_id}",
            description="\n\n".join(created_blocks) if created_blocks else "No rounds created.",
            color=0xB54882
        ),
        ephemeral=True
    )



bot.tree.add_command(match)

# ------------------------ /tournament ------------------------

tournament = app_commands.Group(name="tournament", description="tournament utilities")


# /tournament schedule
@tournament.command(
    name="schedule",
    description="list all matches by phase and round with scores and dates"
)
@app_commands.describe(slug="tournament slug")
async def tournament_list(inter: discord.Interaction, slug: str):
    if not get_settings(slug):
        return await inter.response.send_message(f"`{slug}` not found; run `/setup new` first.", ephemeral=True)

    rows = list_all_matches_full(slug)  # expects phase, round_no, etc.
    if not rows:
        return await inter.response.send_message(f"no matches found for `{slug}` yet.", ephemeral=True)

    plain_map = team_label_map(slug, inter.guild, plain=True)
    mention_map = team_label_map(slug, inter.guild, plain=False)

    def rr_bye_label(phase: str | None, a_id: int | None, b_id: int | None, *, mention: bool = False) -> tuple[
        str, str]:
        ph = (phase or "").lower()
        is_rr = ph == "roundrobin"

        def lbl(role_id: int | None) -> str:
            if role_id is None:
                return "BYE" if is_rr else "TBD"
            return (mention_map if mention else plain_map).get(role_id, f"<@&{role_id}>")

        return lbl(a_id), lbl(b_id)

    def phase_title(p: str | None) -> str:
        if not p:
            return "unspecified"
        return {"swiss": "swiss", "double_elim": "double elimination", "roundrobin": "round robin"}.get(p, p.title())

    def ordinal(n: int) -> str:
        if 10 <= (n % 100) <= 20:
            suf = "th"
        else:
            suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suf}"

    def fmt_when(s: str | None) -> str | None:
        if not s:
            return None
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
            month = dt.strftime("%B")
            day = ordinal(dt.day)
            year = dt.year
            time12 = dt.strftime("%I:%M%p").lstrip("0")
            return f"[{month} {day}, {year} at {time12}]"
        except Exception:
            return f"[{s}]"

    # --- group by phase -> round_no; preserve phase order as seen in rows ---
    from collections import defaultdict, OrderedDict
    by_phase_round: dict[str | None, dict[int | None, list[dict]]] = OrderedDict()
    for r in rows:
        ph = r.get("phase")
        if ph not in by_phase_round:
            by_phase_round[ph] = defaultdict(list)
        by_phase_round[ph][r.get("round_no")].append(r)

    # --- build embeds ---
    embeds: list[discord.Embed] = []
    current = discord.Embed(title=f"matches - {slug}", color=0xB54882)
    fields_in_current = 0

    def flush_embed():
        nonlocal current, fields_in_current
        if fields_in_current > 0:
            embeds.append(current)
            current = discord.Embed(title=f"matches - {slug} (cont.)", color=0xB54882)
            fields_in_current = 0

    # for each phase, then each round within that phase (by round_no asc, None last)
    for phase, rounds_map in by_phase_round.items():
        # Phase header field (empty value just as a divider)
        current.add_field(name=f"**{phase_title(phase)}**", value="\u200b", inline=False)
        fields_in_current += 1
        if fields_in_current >= 24:
            flush_embed()

        for rn in sorted(rounds_map.keys(), key=lambda x: (x is None, x if x is not None else 0)):
            round_matches = rounds_map[rn]

            # compose blocks for matches in this round
            blocks: list[str] = []
            for r in sorted(round_matches, key=lambda x: x["match_id"]):
                a_id = r.get("team_a_role_id")
                b_id = r.get("team_b_role_id")
                a, b = rr_bye_label(r.get("phase"), a_id, b_id, mention=False)
                sa, sb = r.get("score_a"), r.get("score_b")
                score_text = f"{sa}-{sb}" if r.get("reported") and sa is not None and sb is not None else "_ - _"

                # show bracket tag if present (helps double_elim)
                br = (r.get("bracket") or "").upper() if "bracket" in r else ""
                br_prefix = f"({br}) " if br else ""

                top_line = f"☆ match #{r['match_id']}:\n{br_prefix}{a} vs {b} ━ score: {score_text}"
                when_line = fmt_when(r.get("start_time_local"))
                block = top_line + (f"\n{when_line}" if when_line else "")
                blocks.append(block)

            # chunk large rounds to respect embed field limits
            chunk_size = 8
            for i in range(0, len(blocks), chunk_size):
                chunk = blocks[i:i + chunk_size]
                header = f"》round {rn if rn is not None else '-'}"
                if len(blocks) > chunk_size:
                    header += f" (part {i // chunk_size + 1})"
                current.add_field(name=header, value="\n\n".join(chunk), inline=False)
                fields_in_current += 1
                if fields_in_current >= 24:
                    flush_embed()

    flush_embed()
    if not embeds:
        embeds = [current]

    await inter.response.send_message(embed=embeds[0], ephemeral=True)
    for e in embeds[1:]:
        await inter.followup.send(embed=e, ephemeral=True)


# /tournament standings TODO

# /tournament announcement <tournament_id> <post:true|false>
@tournament.command(
    name="announcement",
    description="post/preview last round results and next round games"
)
@app_commands.describe(slug="tournament slug", post="post it?")
async def tournament_announcement(inter: discord.Interaction, slug: str, post: bool):
    if not get_settings(slug):
        return await inter.response.send_message(f"`{slug}` not found; run `/setup new` first.", ephemeral=True)

    s = get_settings(slug)
    tz = s["tz"] if s and s.get("tz") else "America/Toronto"
    ann_ch_id = s.get("announcements_ch")

    # helpers
    plain_map = team_label_map(slug, inter.guild, plain=True)
    mention_map = team_label_map(slug, inter.guild, plain=False)

    def rr_bye_label(phase: str | None, a_id: int | None, b_id: int | None, *, mention: bool = False) -> tuple[
        str, str]:
        ph = (phase or "").lower()
        is_rr = ph == "roundrobin"

        def lbl(role_id: int | None) -> str:
            if role_id is None:
                return "BYE" if is_rr else "TBD"
            return (mention_map if mention else plain_map).get(role_id, f"<@&{role_id}>")

        return lbl(a_id), lbl(b_id)

    def label(role_id: int | None) -> str:
        if role_id is None:
            return "TBD"
        return plain_map.get(role_id, f"<@&{role_id}>")

    def phase_title(p: str) -> str:
        return {"swiss": "swiss", "double_elim": "double elimination", "roundrobin": "round robin"}.get(p, p.title())

    def ordinal(n: int) -> str:
        if 10 <= (n % 100) <= 20:
            suf = "th"
        else:
            suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suf}"

    def fmt_when_local(s_local: str | None) -> str | None:
        if not s_local:
            return None
        try:
            dt = datetime.strptime(s_local, "%Y-%m-%d %H:%M").replace(tzinfo=safe_zoneinfo(tz))
            dow = dt.strftime("%A").lower()
            mon = dt.strftime("%b").lower()
            day = ordinal(dt.day)
            t12 = dt.strftime("%I:%M%p").lstrip("0")
            z = dt.tzname() or ""
            return f"[{dow} ({mon} {day}) at {t12} {z}]".strip()
        except Exception:
            return f"[{s_local}]"

    # ---- Determine which phases exist in DB (so we don't hardcode)
    all_rows = list_all_matches_full(slug)  # has phase/round_no/etc.
    phases_present = sorted({r.get("phase") for r in all_rows if r.get("phase") is not None})

    # ---- "LAST WEEK" per phase (latest fully-reported round in that phase)
    last_sections: list[str] = []
    for phase in phases_present:
        latest_full = get_latest_fully_reported_round(slug, phase)
        if not latest_full:
            continue
        ms = list_round_matches(slug, latest_full, phase)
        lines: list[str] = []
        for m in ms:
            if m.get("reported") and m.get("score_a") is not None and m.get("score_b") is not None:
                a, b = rr_bye_label(phase, m.get("team_a_role_id"), m.get("team_b_role_id"), mention=False)
                sa, sb = m["score_a"], m["score_b"]
                winner = a if sa > sb else (b if sb > sa else "Draw")
                prefix = ""
                if phase == "double_elim":
                    br = (m.get("bracket") or "").upper()
                    prefix = f"({br}) " if br else ""

                lines.append(f"• {prefix}{a} vs. {b}: **{winner}** win ({sa}–{sb})")
        if lines:
            last_sections.append(f"**{phase_title(phase)} - round {latest_full}**\n" + "\n".join(lines))

    include_last = len(last_sections) > 0

    # ---- "THIS WEEK" per phase (next round after latest fully reported)
    next_sections: list[str] = []
    for phase in phases_present:
        latest_full = get_latest_fully_reported_round(slug, phase)
        next_round = (latest_full or 0) + 1
        if round_exists(slug, next_round, phase):
            ms_next = list_round_matches(slug, next_round, phase)
            if ms_next:
                lines: list[str] = []
                for m in ms_next:
                    a, b = rr_bye_label(phase, m.get("team_a_role_id"), m.get("team_b_role_id"), mention=True)

                    when_line = fmt_when_local(m.get("start_time_local"))
                    prefix = ""
                    if phase == "double_elim":
                        br = (m.get("bracket") or "").upper()
                        prefix = f"({br}) " if br else ""
                    top = f"{prefix}{a} vs. {b}"
                    lines.append(top + (f"\n{when_line}" if when_line else ""))
                next_sections.append(f"**{phase_title(phase)} - round {next_round}**\n" + "\n".join(lines))
        else:
            next_sections.append(f"**{phase_title(phase)} - round {next_round}**\n• next round not created yet.")

    # ---- "TOURNAMENT STATUS" (compact, grouped by phase → round)
    from collections import defaultdict
    by_phase_round: dict[str, dict[int | None, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in all_rows:
        ph = (r.get("phase") or "unspecified")
        by_phase_round[ph][r.get("round_no")].append(r)

    status_blocks: list[str] = []
    for phase in sorted(by_phase_round.keys()):
        status_lines: list[str] = [f"**{phase_title(phase)}**"]
        for rn in sorted(by_phase_round[phase].keys(), key=lambda x: (x is None, x if x is not None else 0)):
            status_lines.append(f"》Round {rn if rn is not None else '-'}")
            round_matches = sorted(by_phase_round[phase][rn], key=lambda x: x["match_id"])
            for r in round_matches:
                a, b = rr_bye_label(phase, r.get("team_a_role_id"), r.get("team_b_role_id"), mention=False)
                sa, sb = r.get("score_a"), r.get("score_b")
                score_text = f"{sa}-{sb}" if r.get("reported") and sa is not None and sb is not None else "_ - _"
                prefix = ""
                if phase == "double_elim":
                    br = (r.get("bracket") or "").upper()
                    if br in ("WB", "LB", "GF"):
                        prefix = f"({br}) "
                status_lines.append(f"☆ match #{r['match_id']}:    {prefix}{a} vs {b} ━ score: {score_text}")
        status_blocks.append("\n".join(status_lines))

    list_text = "\n\n".join(status_blocks) if status_blocks else "No matches yet."

    # ---- compose message
    divider = "-" * 64
    header_last = ":feet: **MATCHES LAST WEEK!**"
    header_next = ":feet: **MATCHES THIS WEEK!**"
    header_status = ":cat: **TOURNAMENT STATUS**"

    intro = (
        "hi everyone! here is the weekly announcement.\n"
        "please discuss rescheduling asap! for any questions, please refer to the rulebook or contact staff.\n"
        "good luck! <3"
    )

    chunks: list[str] = [intro, divider]
    if include_last:
        chunks.append(header_last)
        chunks.append("\n\n".join(last_sections))
        chunks.append(divider)
    chunks.append(header_next)
    chunks.append("\n\n".join(next_sections))
    chunks.append(divider)
    chunks.append(header_status)
    chunks.append(list_text)

    msg = "\n\n".join(chunks)

    # post or preview
    if post:
        if not ann_ch_id:
            return await inter.response.send_message(
                "announcements channel not set. use `/setup channels` first.",
                ephemeral=True
            )
        if not inter.guild:
            return await inter.response.send_message("run this in a server.", ephemeral=True)
        ch = inter.guild.get_channel(ann_ch_id)
        if not isinstance(ch, discord.TextChannel):
            return await inter.response.send_message("configured announcements channel is invalid.", ephemeral=True)

        # chunk to avoid 2k limit
        out_chunks: list[str] = []
        cur, cur_len = [], 0
        for line in msg.split("\n"):
            if cur_len + len(line) + 1 > 1900:
                out_chunks.append("\n".join(cur)); cur, cur_len = [], 0
            cur.append(line); cur_len += len(line) + 1
        if cur:
            out_chunks.append("\n".join(cur))

        allow = discord.AllowedMentions(roles=True, users=False, everyone=False)
        for chunk in out_chunks:
            await ch.send(chunk, allowed_mentions=allow)
        return await inter.response.send_message("announcement posted ✅", ephemeral=False)
    else:
        return await inter.response.send_message(msg, ephemeral=True)


# /tournament refresh
@tournament.command(
    name="refresh",
    description="Fill the next round's placeholders for Swiss or Double Elim (or both with auto)."
)
@app_commands.describe(
    tournament_id="tournament ID",
    kind="Which to refresh: auto/swiss/double_elim"
)
async def tournament_refresh(
    inter: discord.Interaction,
    tournament_id: str,
    kind: Literal["auto", "swiss", "double_elim"] = "auto"
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first", ephemeral=True)

    results: list[str] = []

    async def do_swiss() -> None:
        phase = "swiss"
        latest = get_latest_fully_reported_round(tournament_id, phase)
        if not latest:
            results.append("Swiss: no fully-reported round yet.")
            return

        next_round = latest + 1
        if not round_exists(tournament_id, next_round, phase):
            results.append(f"Swiss: round {next_round} doesn't exist. Create placeholders with `/match add kind:swiss`.")
            return
        if not round_has_placeholders(tournament_id, next_round, phase):
            results.append(f"Swiss: round {next_round} has no empty placeholders.")
            return

        prev_ms = list_round_matches(tournament_id, latest, phase)
        team_ids: list[int] = []
        for m in prev_ms:
            for t in (m.get("team_a_role_id"), m.get("team_b_role_id")):
                if t and t not in team_ids:
                    team_ids.append(t)

        # Uses your swiss helper & history (still reads swiss only)
        hist = swiss_history(tournament_id)
        pairs = pair_next_round(team_ids, hist)  # -> list[(a,b)]

        placeholders = list_round_placeholders(tournament_id, next_round, phase)
        if len(pairs) != len(placeholders):
            results.append(
                f"Swiss: can't fill round {next_round}: {len(placeholders)} placeholders vs {len(pairs)} pairs."
            )
            return

        try:
            assign_pairs_into_round(tournament_id, next_round, pairs, phase)
        except ValueError as e:
            results.append(f"Swiss: {e}")
            return

        lines = [f"• <@&{a}> vs <@&{b}>  (#{mid})" for mid, (a, b) in zip(placeholders, pairs)]
        results.append(f"Swiss → Round {next_round} filled:\n" + "\n".join(lines))

    async def do_de() -> None:
        phase = "double_elim"

        latest = get_latest_round(tournament_id, phase)
        if latest is None or latest == 0:
            results.append("DE: no rounds exist yet.")
            return

        next_round = latest + 1
        if not round_exists(tournament_id, next_round, phase):
            results.append(
                f"DE: round {next_round} doesn't exist. Create placeholders with `/match add kind:double_elim`.")
            return

        # Pull placeholders (both teams NULL) and group by bracket tag
        nr_matches = list_round_matches(tournament_id, next_round, phase)
        placeholders = [r for r in nr_matches if r.get("team_a_role_id") is None and r.get("team_b_role_id") is None]
        if not placeholders:
            results.append(f"DE: round {next_round} has no empty placeholders.")
            return

        from collections import defaultdict
        mids_by_br: dict[str, list[int]] = defaultdict(list)
        for r in sorted(placeholders, key=lambda x: x["match_id"]):
            br = (r.get("bracket") or "").upper()
            mids_by_br[br].append(int(r["match_id"]))

        latest_matches = list_round_matches(tournament_id, latest, phase)
        if not latest_matches:
            results.append(f"DE: no matches found for round {latest}.")
            return

        # Helpers
        def to_pairs(seq: list[int]) -> list[tuple[int, int]]:
            buf = seq[:]
            out: list[tuple[int, int]] = []
            while len(buf) >= 2:
                out.append((buf.pop(0), buf.pop(0)))
            return out

        # Gather winners/losers from the latest round, per bracket
        wb_winners: list[int] = []
        wb_losers: list[int] = []
        lb_winners: list[int] = []
        lb_losers: list[int] = []
        lcq_winners: list[int] = []

        for m in sorted(latest_matches, key=lambda x: x["match_id"]):
            sa, sb = m.get("score_a"), m.get("score_b")
            if not (m.get("reported") and sa is not None and sb is not None and sa != sb):
                continue
            a, b = m.get("team_a_role_id"), m.get("team_b_role_id")
            if a is None or b is None:
                continue

            winner = a if sa > sb else b
            loser = b if sa > sb else a
            br = (m.get("bracket") or "").upper()

            if br == "LB":
                lb_winners.append(winner)
                lb_losers.append(loser)
            elif br == "LCQ":
                lcq_winners.append(winner)
                # LCQ losers are eliminated in our 6-team template, don't feed LB here
            else:  # default WB
                wb_winners.append(winner)
                wb_losers.append(loser)

        # Number of mapped teams drives a few special rules
        n = len(list_teams(tournament_id))

        # Build concrete assignments: list of (mid, a, b)
        updates: list[tuple[int, int, int]] = []

        # ---- 6-team Round 2 special seeding: LCQ winners vs seeds #1/#2
        if n == 6 and latest == 1 and "WB" in mids_by_br and len(mids_by_br["WB"]) >= 2:
            rows = list_teams(tournament_id)
            rows.sort(key=lambda r: int(r["team_id"]))  # seed order 1..N by team_id
            seed1 = int(rows[0]["team_role_id"])
            seed2 = int(rows[1]["team_role_id"])
            if len(lcq_winners) == 2:
                wb_order = mids_by_br["WB"]
                pairs = [(lcq_winners[0], seed1), (lcq_winners[1], seed2)]
                for mid, (a, b) in zip(wb_order[:2], pairs):
                    updates.append((mid, a, b))
                # consume the two WB mids we just filled
                mids_by_br["WB"] = wb_order[2:]

        # ---- Generic WB for other cases: winners of latest WB pair among themselves
        if "WB" in mids_by_br and mids_by_br["WB"]:
            wb_pairs = to_pairs(wb_winners)
            for mid, (a, b) in zip(mids_by_br["WB"], wb_pairs):
                updates.append((mid, a, b))

        # ---- LB next round: losers from latest WB + winners from latest LB
        if "LB" in mids_by_br and mids_by_br["LB"]:
            lb_feed = wb_losers + lb_winners
            lb_pairs = to_pairs(lb_feed)
            for mid, (a, b) in zip(mids_by_br["LB"], lb_pairs):
                updates.append((mid, a, b))

        # ---- 4P (8-team, Round 3): losers of the two WB semis
        if "4P" in mids_by_br and mids_by_br["4P"]:
            fourp_pairs = to_pairs(wb_losers)
            for mid, (a, b) in zip(mids_by_br["4P"], fourp_pairs):
                updates.append((mid, a, b))

        # ---- 3P (6-team, Round 4): loser(WB final) vs loser(LB final) from latest
        if "3P" in mids_by_br and mids_by_br["3P"]:
            threep_pairs = to_pairs(wb_losers + lb_losers)
            for mid, (a, b) in zip(mids_by_br["3P"], threep_pairs):
                updates.append((mid, a, b))

        # ---- GF:
        # 4-team R4 and 6-team R4 → winner(WB final) vs winner(LB final) are both from latest
        if "GF" in mids_by_br and mids_by_br["GF"]:
            can_fill_gf = False
            gf_pair: tuple[int, int] | None = None

            if n in (4, 6):
                if wb_winners and lb_winners:
                    gf_pair = (wb_winners[0], lb_winners[0])
                    can_fill_gf = True
            elif n == 8:
                # In our template GF is in R4 and depends on the R4 LB winner.
                # We can’t fill it when latest=R3; leave TBD to avoid partials.
                can_fill_gf = False

            if can_fill_gf and gf_pair is not None:
                updates.append((mids_by_br["GF"][0], gf_pair[0], gf_pair[1]))

        if not updates:
            results.append("DE: nothing to fill yet (waiting on more results).")
            return

        # Persist: write only fully-known pairs so placeholders remain intact otherwise
        for mid, a, b in updates:
            set_match_teams(tournament_id, mid, team_a_role_id=a, team_b_role_id=b)

        # Pretty output
        plain_map = team_label_map(tournament_id, inter.guild, plain=True)

        def lab(x: int | None) -> str:
            return plain_map.get(x, f"<@&{x}>") if x else "TBD"

        out_lines = [f"• #{mid}: {lab(a)} vs {lab(b)}" for (mid, a, b) in updates]
        results.append(f"double elim → round {next_round} updates:\n" + "\n".join(out_lines))

    phases = (
        ["swiss", "double_elim"] if kind == "auto"
        else [kind]
    )

    if "swiss" in phases:
        await do_swiss()
    if "double_elim" in phases:
        await do_de()

    # Compose output
    # If nothing happened, make that clear.
    body = "\n\n".join(results) if results else "No action taken."
    await inter.response.send_message(
        embed=discord.Embed(
            title=f"Refresh - {tournament_id} ({kind})",
            description=body,
            color=0xB54882
        ),
        ephemeral=True
    )


bot.tree.add_command(tournament)


# ------------------------ misc. ------------------------

# /help TODO

async def _post_reminder_to_thread(bot: commands.Bot, payload: dict) -> tuple[bool, bool]:
    slug = payload["tournament_name"]
    mid = int(payload["match_id"])
    kind = payload["kind"]
    thread_id = payload.get("thread_id")

    # load settings + match
    s = get_settings(slug)
    if not s:
        log.warning(f"[reminders] settings missing for {slug}")
        return False, True
    tz = s.get("tz") or "America/Toronto"

    m = get_match(slug, mid)
    if not m:
        log.warning(f"[reminders] match #{mid} missing for {slug}")
        return False, True

    # MUST have an existing thread id; otherwise bail (no auto-create)
    if not thread_id:
        log.info(f"[reminders] match #{mid} has no thread_id; skipping.")
        return False, True

    # resolve thread from API/cache
    thread: discord.Thread | None = None
    try:
        ch = bot.get_channel(thread_id) or await bot.fetch_channel(thread_id)
        if isinstance(ch, discord.Thread):
            thread = ch
        else:
            log.info(f"[reminders] channel {thread_id} is not a Thread; skipping.")
            return False, True
    except discord.NotFound:
        log.info(f"[reminders] thread_id {thread_id} not found (maybe deleted); skipping.")
        return False, True
    except discord.Forbidden:
        log.warning(f"[reminders] forbidden fetching thread {thread_id}; skipping.")
        return False, True
    except Exception as e:
        log.exception(f"[reminders] error fetching thread {thread_id}: {e}")
        return False, False

    # join if needed
    try:
        if isinstance(thread, discord.Thread):
            await thread.join()
    except Exception:
        pass

    # pretty time (local)
    pretty = ""
    if m.get("start_time_local"):
        try:
            dt = datetime.strptime(m["start_time_local"], "%Y-%m-%d %H:%M").replace(tzinfo=safe_zoneinfo(tz))
            pretty = f"{dt.strftime('%B')} {dt.day}, {dt.strftime('%A')} at {dt.strftime('%I:%M%p').lstrip('0')}"
        except Exception:
            pretty = m["start_time_local"]

    a_id = m.get("team_a_role_id")
    b_id = m.get("team_b_role_id")
    mention = " ".join([f"<@&{a_id}>" if a_id else "", f"<@&{b_id}>" if b_id else ""]).strip()

    prefix = "🕑  reminder"
    if kind == "noon":
        body = f"{prefix}: day-of reminder for **match #{mid}** - starts **{pretty}**."
    elif kind == "pre2h":
        body = f"{prefix}: **2 hours** until **match #{mid}** - starts **{pretty}**."
    else:
        body = f"{prefix}: **1 hour** until **match #{mid}** - starts **{pretty}**."

    try:
        await thread.send(
            f"{mention}\n{body}",
            allowed_mentions=discord.AllowedMentions(roles=True, users=False, everyone=False)
        )
        log.info(f"[reminders] posted {kind} for {slug} match #{mid} in thread {thread.id}")
        return True, True
    except discord.Forbidden:
        log.warning(f"[reminders] forbidden sending to thread {thread.id}")
        return False, True
    except Exception as e:
        log.exception(f"[reminders] error sending to thread {thread.id}: {e}")
        return False, False


async def reminder_worker(bot: commands.Bot):
    await bot.wait_until_ready()
    log.info("[reminders] worker started")
    while not bot.is_closed():
        try:
            now_utc = datetime.utcnow().replace(tzinfo=None)
            due = fetch_due_reminders(now_utc, limit=100)
            if due:
                log.info(f"[reminders] {len(due)} reminder(s) due at <= {now_utc.strftime('%Y-%m-%d %H:%M')}")
            for r in due:
                ok, final = await _post_reminder_to_thread(bot, r)
                if ok or final:
                    mark_reminder_sent(int(r["id"]))
                if not ok:
                    log.info(f"[reminders] failed to deliver id={r['id']} ({r['kind']}) for match #{r['match_id']}")
        except Exception as e:
            log.exception(f"[reminders] worker loop error: {e}")
        await asyncio.sleep(60)


def team_label_map(
    slug: str,
    guild: discord.Guild | None,
    *,
    plain: bool,            # true -> plain role.name; false -> role mention text
) -> dict[int, str]:
    rows = list_teams(slug)
    out: dict[int, str] = {}
    for r in rows:
        rid = int(r["team_role_id"])
        if plain and guild:
            role = guild.get_role(rid)
            out[rid] = role.name if role else f"role:{rid}"
        else:
            out[rid] = f"<@&{rid}>"
    return out


def run():
    logging.basicConfig(level=logging.INFO)
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    run()
