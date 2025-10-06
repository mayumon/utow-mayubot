import logging
import discord
from discord import app_commands
from discord.ext import commands
from .config import DISCORD_TOKEN
from .storage import *
from .swiss_helpers import *
from collections import defaultdict
import random
from datetime import datetime, timedelta
from typing import Literal

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

# /reminders set [match_id] TODO

# /reminders list TODO

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
                 "_no thread to update_")
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
        if not s:
            return None

        # expects "YYYY-MM-DD HH:MM"
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
            month = dt.strftime("%B")
            day = ordinal(dt.day)
            year = dt.year
            time12 = dt.strftime("%I:%M%p").lstrip("0") if hasattr(dt, "strftime") else dt.strftime("%I:%M%p").lstrip("0")

            if time12.startswith("0"):
                time12 = time12[1:]
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

            if r["reported"] and sa is not None and sb is not None:
                score_text = f"{sa}-{sb}"
            else:
                score_text = "_ - _"

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
        await inter.followup.send(embed=e, ephemeral=False)


# /tournament standings TODO

# /tournament announcement <post:true|false> TODO


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


def run():
    logging.basicConfig(level=logging.INFO)
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    run()
