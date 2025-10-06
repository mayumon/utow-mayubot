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

    asyncio.create_task(reminder_worker(bot))


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


# /setup team <@role>
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

    name = role.name
    try:
        assigned_id = link_team(tournament_id, team_role_id=role.id, team_id=None, display_name=name)
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
                f"**display name:** `{name}`",
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
    dn = mapping.get("display_name") or f"<@&{rid}>"
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
                f"**display name:** `{dn}`",
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

    # sort by team_id
    rows.sort(key=lambda r: (int(r["team_id"]), (r["display_name"] or "").lower()))

    lines = []
    for r in rows:
        rid = int(r["team_role_id"])
        tid = int(r["team_id"])
        dn = (r["display_name"] or "").strip()
        label = dn if dn else f"<@&{rid}>"
        lines.append(f"• **{label}** — team_id `{tid}` — role <@&{rid}>")

    embed = discord.Embed(
        title=f"Teams — {tournament_id}",
        color=0xB54882
    )

    chunk = []
    count = 0
    for i, line in enumerate(lines, 1):
        chunk.append(line)
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
        suffix = " (no thread — reminders will not post)" if not has_thread else ""
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
        title=f"Reminders — {tournament_id}",
        color=0xB54882
    )
    embed.set_footer(text=f"All times shown in {tz}")

    def fmt_row(r):
        status = "✅ sent" if int(r["sent"]) else "⏳ pending"
        # convert stored UTC (string) -> local display
        dt_utc = datetime.strptime(r["when_utc"], "%Y-%m-%d %H:%M").replace(tzinfo=safe_zoneinfo("UTC"))
        dt_loc = dt_utc.astimezone(tzinfo)
        local_txt = dt_loc.strftime("%Y-%m-%d %H:%M")
        return f"- `{r['kind']}` at `{local_txt}` — {status}", dt_utc

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


# /match thread <match_id>
@match.command(name="thread", description="create a private match thread for the two teams in a match.")
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

    # title
    phase = (m.get("phase") or "").lower()
    round_no = m.get("round_no")
    title_prefix = "Playoffs Round" if phase == "playoff" else "Round"
    round_label = round_no if round_no is not None else "?"
    title = f"{title_prefix} {round_label}: {a_role.name} vs {b_role.name}"

    # create a PRIVATE thread (only invited users & mods)
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
        # non-fatal; continue
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

    # ensure role pings are allowed in this message
    allowed = discord.AllowedMentions(roles=True, users=False, everyone=False)

    try:
        await thread.send("\n".join(body_lines), allowed_mentions=allowed)
    except Exception:
        # if the first message fails
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
                    # missing perms to add a particular user
                    continue
                except Exception:
                    continue
        except Exception:
            continue

    # pointer message to parent
    pointer_lines = [
        f"**▶ match thread created:** {thread.mention}",
    ]

    # don't ping roles in the public pointer
    no_pings = discord.AllowedMentions(everyone=False, users=False, roles=False)

    try:
        pointer_msg = await channel.send("\n".join(pointer_lines), allowed_mentions=no_pings)

    except Exception:
        pass  # pointer is nice-to-have; don't fail the command if it can't be posted

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
        ephemeral=True
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
        return await inter.response.send_message(f"match `#{match_id}` not found for `{tournament_id}`.",
                                                 ephemeral=True)

    record_result(tournament_id, match_id, score_a, score_b)

    a = m.get("team_a_role_id")
    b = m.get("team_b_role_id")
    label = f"<@&{a}> vs <@&{b}>" if a and b else f"match #{match_id}"
    await inter.response.send_message(f"recorded result for {label}: **{score_a}–{score_b}**.", ephemeral=True)


# /match add <tournament_id> <swiss|double_elim|round_robin> <round_num>
@match.command(
    name="add",
    description="generate and add N swiss rounds"
)
@app_commands.describe(
    tournament_id="tournament ID",
    kind="swiss/double_elim/roundrobin",
    rounds="how many rounds to add",
    start_time="local start time for round 1 in YYYY-MM-DD HH:MM format"
)
async def match_add(
        inter: discord.Interaction,
        tournament_id: str,
        kind: Literal["swiss", "double_elim"],
        rounds: int,
        start_time: str,
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms.", ephemeral=True)
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.",
                                                 ephemeral=True)
    if kind != "swiss":
        return await inter.response.send_message("double_elim not implemented yet.", ephemeral=True)
    if rounds < 1:
        return await inter.response.send_message("`rounds` must be ≥ 1.", ephemeral=True)

    # parse round 1 baseline time
    try:
        base_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
    except ValueError:
        return await inter.response.send_message(
            "invalid `start_time` format. use `YYYY-MM-DD HH:MM`",
            ephemeral=True
        )

    created_blocks: list[str] = []
    latest = get_latest_round(tournament_id)

    # team pool
    mapped = list_teams(tournament_id)
    team_ids = [row["team_role_id"] for row in mapped]
    if len(team_ids) < 2 or len(team_ids) % 2 != 0:
        return await inter.response.send_message(
            "need an **even** number of mapped teams (≥2). Map teams first with `/setup team`.",
            ephemeral=True
        )

    name_map = get_team_display_map(tournament_id)

    def label(role_id: int | None) -> str:
        if role_id is None:
            return "TBD"
        return name_map.get(role_id, f"role:{role_id}")

    for _ in range(rounds):
        target_round = latest + 1

        if round_exists(tournament_id, target_round):
            # still show a block so user knows it was skipped
            round_date = (base_dt + timedelta(weeks=(target_round - 1))).strftime("%B %d, %Y at %I:%M%p")
            created_blocks.append(f"》Round {target_round}\n(already exists)\n\n[{round_date}]")
            latest = target_round
            continue

        # compute round's start timestamp string
        r_start_dt = base_dt + timedelta(weeks=(target_round - 1))
        r_start_str = r_start_dt.strftime("%Y-%m-%d %H:%M")
        round_date_pretty = r_start_dt.strftime("%B %d, %Y at %I:%M%p")

        lines: list[str] = [f"》Round {target_round}"]

        if latest == 0 and target_round == 1:
            # round 1: assign pairings now
            ids = team_ids[:]
            random.shuffle(ids)
            pairings = []
            for i in range(0, len(ids), 2):
                a, b = ids[i], ids[i + 1]
                pairings.append({
                    "match_id": None,
                    "team_a_role_id": a,
                    "team_b_role_id": b,
                    "start_time_local": r_start_str
                })
            assigned = create_swiss_round(tournament_id, target_round, pairings)

            # output
            for i, mid in enumerate(assigned):
                a = label(pairings[i]["team_a_role_id"])
                b = label(pairings[i]["team_b_role_id"])
                lines.append(f"☆ match #{mid}:\n{a} vs {b} ━ score: -")
        else:
            # later rounds: placeholders with dates set (TBD names)
            match_count = len(team_ids) // 2
            pairings = [{
                "match_id": None,
                "team_a_role_id": None,
                "team_b_role_id": None,
                "start_time_local": r_start_str
            } for _ in range(match_count)]
            assigned = create_swiss_round(tournament_id, target_round, pairings)

            for mid in assigned:
                lines.append(f"☆ match #{mid}: TBD vs TBD ━ score: -")

        # one date line per round
        lines.append(f"[{round_date_pretty}]")

        created_blocks.append("\n".join(lines))
        latest = target_round  # advance

    await inter.response.send_message(
        embed=discord.Embed(
            title=f"Swiss rounds created — {tournament_id}",
            description="\n\n".join(created_blocks) if created_blocks else "No rounds created.",
            color=0xB54882
        ),
        ephemeral=True
    )


bot.tree.add_command(match)

# ------------------------ /tournament ------------------------

tournament = app_commands.Group(name="tournament", description="tournament utilities")


# /tournament list
@tournament.command(name="list", description="list all matches by round with scores and dates")
@app_commands.describe(slug="tournament slug")
async def tournament_list(inter: discord.Interaction, slug: str):
    if not get_settings(slug):
        return await inter.response.send_message(f"`{slug}` not found; run `/setup new` first.", ephemeral=True)

    rows = list_all_matches_full(slug)
    if not rows:
        return await inter.response.send_message(f"No matches found for `{slug}` yet.", ephemeral=True)

    name_map = get_team_display_map(slug)

    def team_label(role_id: int | None) -> str:
        if role_id is None:
            return "TBD"
        return name_map.get(role_id, f"<@&{role_id}>")

    def ordinal(n: int) -> str:
        if 10 <= (n % 100) <= 20:
            suf = "th"
        else:
            suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suf}"

    from datetime import datetime

    def fmt_when(s: str | None) -> str | None:
        """Pretty date like: [October 17th, 2025 at 8:00PM]"""
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

    from collections import defaultdict
    grouped: dict[int | None, list[dict]] = defaultdict(list)
    for r in rows:
        grouped[r["round_no"]].append(r)

    ordered_rounds = sorted(grouped.keys(), key=lambda x: (x is None, x if x is not None else 0))

    embeds: list[discord.Embed] = []
    current = discord.Embed(
        title=f"Matches — {slug}",
        description="",
        color=0xB54882
    )
    fields_in_current = 0

    def flush_embed():
        nonlocal current, fields_in_current
        if fields_in_current > 0:
            embeds.append(current)
            current = discord.Embed(title=f"Matches — {slug} (cont.)", color=0xB54882)
            fields_in_current = 0

    for rn in ordered_rounds:
        round_matches = grouped[rn]

        blocks: list[str] = []
        for r in sorted(round_matches, key=lambda x: x["match_id"]):
            a = team_label(r["team_a_role_id"])
            b = team_label(r["team_b_role_id"])
            sa = r["score_a"]
            sb = r["score_b"]

            score_text = f"{sa}-{sb}" if r["reported"] and sa is not None and sb is not None else "_ - _"

            top_line = f"☆ match #{r['match_id']}:\n{a} vs {b} ━ score: {score_text}"
            when_line = fmt_when(r["start_time_local"])
            block = top_line + (f"\n{when_line}" if when_line else "")
            blocks.append(block)

        chunk_size = 8
        for i in range(0, len(blocks), chunk_size):
            chunk = blocks[i:i + chunk_size]
            header = f"**》Round {rn if rn is not None else '-'}**"
            if len(blocks) > chunk_size:
                header += f" (part {i // chunk_size + 1})"
            value = "\n\n".join(chunk)
            current.add_field(name=header, value=value, inline=False)
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

# /tournament announcement <post:true|false>
@tournament.command(
    name="announcement",
    description="post (or preview) last round results and next round games"
)
@app_commands.describe(slug="tournament slug", post="post it?")
async def tournament_announcement(inter: discord.Interaction, slug: str, post: bool):
    if not get_settings(slug):
        return await inter.response.send_message(f"`{slug}` not found; run `/setup new` first.", ephemeral=True)

    s = get_settings(slug)
    tz = s["tz"] if s and s.get("tz") else "America/Toronto"
    ann_ch_id = s.get("announcements_ch")

    # helpers
    name_map = get_team_display_map(slug)

    def label(role_id: int | None) -> str:
        if role_id is None:
            return "TBD"
        return name_map.get(role_id, f"<@&{role_id}>")

    def mention_label(role_id: int | None) -> str:
        if role_id is None:
            return "@unknown-role"
        return f"<@&{role_id}>"

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

    # ----- build "last week" section -----
    latest = get_latest_fully_reported_round(slug)
    last_lines: list[str] = []
    if latest:
        ms = list_round_matches(slug, latest)
        for m in ms:
            a = label(m["team_a_role_id"])
            b = label(m["team_b_role_id"])
            sa = m.get("score_a")
            sb = m.get("score_b")
            if m.get("reported") and sa is not None and sb is not None:
                if sa > sb:
                    winner = a
                elif sb > sa:
                    winner = b
                else:
                    winner = "Draw"
                # No bold on the pair; :star: before each team; show winner + score
                last_lines.append(f"• :star: {a} vs. :star: {b}: {winner} win ({sa}–{sb})")
        if not last_lines:
            last_lines.append("• No completed matches recorded.")
    else:
        last_lines.append("• No fully reported round yet.")

    # ----- build "this week" section -----
    next_lines: list[str] = []
    next_round = (latest or 0) + 1
    if round_exists(slug, next_round):
        ms_next = list_round_matches(slug, next_round)
        for m in ms_next:
            a = mention_label(m["team_a_role_id"])  # ping roles
            b = mention_label(m["team_b_role_id"])
            when_line = fmt_when_local(m.get("start_time_local"))
            top = f"{a} vs. {b}"
            next_lines.append(top + (f"\n{when_line}" if when_line else ""))
    else:
        next_lines.append("• Next round not created yet.")

    # ----- build compact "TOURNAMENT STATUS" section (no dates, single line per match) -----
    all_rows = list_all_matches_full(slug)
    grouped: dict[int | None, list[dict]] = defaultdict(list)
    for r in all_rows:
        grouped[r["round_no"]].append(r)

    def team_label(role_id: int | None) -> str:
        if role_id is None:
            return "TBD"
        return name_map.get(role_id, f"<@&{role_id}>")

    round_sections: list[str] = []
    for rn in sorted(grouped.keys(), key=lambda x: (x is None, x if x is not None else 0)):
        lines: list[str] = []
        lines.append(f"》Round {rn if rn is not None else '-'}")
        for r in sorted(grouped[rn], key=lambda x: x["match_id"]):
            a = team_label(r["team_a_role_id"])
            b = team_label(r["team_b_role_id"])
            sa, sb = r.get("score_a"), r.get("score_b")
            score_text = f"{sa}-{sb}" if r.get("reported") and sa is not None and sb is not None else "_ - _"
            # Single compact line, with small padding before teams for readability
            lines.append(f"☆ match #{r['match_id']}:    {a} vs {b} ━ score: {score_text}")
        round_sections.append("\n".join(lines))

    list_text = "\n\n".join(round_sections) if round_sections else "No matches yet."

    # ----- compose message (intro + dividers + sections) -----
    divider = "—" * 64
    header_last = ":feet: **MATCHES LAST WEEK!**"
    header_next = ":feet: **MATCHES THIS WEEK!**"
    header_status = ":cat: **TOURNAMENT STATUS**"

    intro = (
        "hi everyone! here is the weekly announcement.\n"
        "please discuss rescheduling asap! for any questions, please refer to the rulebook or contact staff.\n"
        "good luck! <3"
    )

    last_text = "\n".join(last_lines) if last_lines else "• —"
    next_text = "\n\n".join(next_lines) if next_lines else "• —"

    msg = (
        f"{intro}\n\n"
        f"{divider}\n"
        f"{header_last}\n\n"
        f"{last_text}\n\n"
        f"{divider}\n"
        f"{header_next}\n\n"
        f"{next_text}\n\n"
        f"{divider}\n"
        f"{header_status}\n\n"
        f"{list_text}"
    )

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

        chunks: list[str] = []
        cur, cur_len = [], 0
        for line in msg.split("\n"):
            if cur_len + len(line) + 1 > 1900:
                chunks.append("\n".join(cur))
                cur, cur_len = [], 0
            cur.append(line)
            cur_len += len(line) + 1
        if cur:
            chunks.append("\n".join(cur))

        # allow role pings (for "this week" section)
        allow = discord.AllowedMentions(roles=True, users=False, everyone=False)
        for chunk in chunks:
            await ch.send(chunk, allowed_mentions=allow)
        return await inter.response.send_message("Announcement posted ✅", ephemeral=True)
    else:
        return await inter.response.send_message(msg, ephemeral=True)


# /tournament swiss_refresh
@tournament.command(
    name="swiss_refresh",
    description="Fill the next Swiss round's placeholders based on current results (does not create matches)."
)
@app_commands.describe(tournament_id="tournament ID")
async def tournament_swiss_refresh(inter: discord.Interaction, tournament_id: str):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first", ephemeral=True)

    # latest fully reported round
    latest = get_latest_fully_reported_round(tournament_id)
    if not latest:
        return await inter.response.send_message(
            "no fully reported Swiss round found yet. report at least one full round first.",
            ephemeral=True
        )

    next_round = latest + 1
    # next round must already exist (placeholders created earlier via /match add)
    if not round_exists(tournament_id, next_round):
        return await inter.response.send_message(
            f"round {next_round} does not exist yet. create placeholders first: `/match add tournament_id:{tournament_id} kind:swiss rounds:1`.",
            ephemeral=True
        )
    if not round_has_placeholders(tournament_id, next_round):
        return await inter.response.send_message(
            f"round {next_round} has no empty placeholders to fill (maybe already assigned?).",
            ephemeral=True
        )

    prev_ms = list_round_matches(tournament_id, latest)
    team_ids: list[int] = []
    for m in prev_ms:
        for t in (m["team_a_role_id"], m["team_b_role_id"]):
            if t and t not in team_ids:
                team_ids.append(t)

    hist = swiss_history(tournament_id)
    pairs = pair_next_round(team_ids, hist)  # returns list[(a,b)]

    placeholders = list_round_placeholders(tournament_id, next_round)
    if len(pairs) != len(placeholders):
        return await inter.response.send_message(
            f"cannot fill round {next_round}: {len(placeholders)} placeholders vs {len(pairs)} computed pairs.",
            ephemeral=True
        )

    try:
        assign_pairs_into_round(tournament_id, next_round, pairs)
    except ValueError as e:
        return await inter.response.send_message(str(e), ephemeral=True)

    lines = []
    for mid, (a, b) in zip(placeholders, pairs):
        lines.append(f"• <@&{a}> vs <@&{b}>  (#{mid})")

    await inter.response.send_message(
        embed=discord.Embed(
            title=f"Swiss Round {next_round} filled — {tournament_id}",
            description="\n".join(lines),
            color=0xB54882
        ),
        ephemeral=True
    )


bot.tree.add_command(tournament)


# ------------------------ misc. ------------------------

# /help TODO

async def _post_reminder_to_thread(bot: commands.Bot, payload: dict) -> bool:
    slug = payload["tournament_name"]
    mid = int(payload["match_id"])
    kind = payload["kind"]
    thread_id = payload.get("thread_id")

    # load settings + match
    s = get_settings(slug)
    if not s:
        log.warning(f"[reminders] settings missing for {slug}")
        return False
    tz = s.get("tz") or "America/Toronto"

    m = get_match(slug, mid)
    if not m:
        log.warning(f"[reminders] match #{mid} missing for {slug}")
        return False

    # MUST have an existing thread id; otherwise bail (no auto-create)
    if not thread_id:
        log.info(f"[reminders] match #{mid} has no thread_id; skipping.")
        return False

    # resolve thread from API/cache
    thread: discord.Thread | None = None
    try:
        ch = bot.get_channel(thread_id) or await bot.fetch_channel(thread_id)
        if isinstance(ch, discord.Thread):
            thread = ch
        else:
            log.info(f"[reminders] channel {thread_id} is not a Thread; skipping.")
            return False
    except discord.NotFound:
        log.info(f"[reminders] thread_id {thread_id} not found (maybe deleted); skipping.")
        return False
    except discord.Forbidden:
        log.warning(f"[reminders] forbidden fetching thread {thread_id}; skipping.")
        return False
    except Exception as e:
        log.exception(f"[reminders] error fetching thread {thread_id}: {e}")
        return False

    # join if needed
    try:
        if isinstance(thread, discord.Thread) and thread.me is not None and not thread.me.joined:
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
        body = f"{prefix}: day-of reminder for **match #{mid}** — starts **{pretty}**."
    elif kind == "pre2h":
        body = f"{prefix}: **2 hours** until **match #{mid}** — starts **{pretty}**."
    else:
        body = f"{prefix}: **1 hour** until **match #{mid}** — starts **{pretty}**."

    try:
        await thread.send(
            f"{mention}\n{body}",
            allowed_mentions=discord.AllowedMentions(roles=True, users=False, everyone=False)
        )
        log.info(f"[reminders] posted {kind} for {slug} match #{mid} in thread {thread.id}")
        return True
    except discord.Forbidden:
        log.warning(f"[reminders] forbidden sending to thread {thread.id}")
        return False
    except Exception as e:
        log.exception(f"[reminders] error sending to thread {thread.id}: {e}")
        return False



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
                ok = await _post_reminder_to_thread(bot, r)
                if ok:
                    mark_reminder_sent(int(r["id"]))
                else:
                    log.info(f"[reminders] failed to deliver id={r['id']} ({r['kind']}) for match #{r['match_id']}")
        except Exception as e:
            log.exception(f"[reminders] worker loop error: {e}")
        await asyncio.sleep(60)


def run():
    logging.basicConfig(level=logging.INFO)
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    run()
