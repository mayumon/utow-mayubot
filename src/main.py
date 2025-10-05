import logging
import discord
from discord import app_commands
from discord.ext import commands
from .config import DISCORD_TOKEN
from .storage import *
from .swiss_helpers import *
from collections import defaultdict
import random
from datetime import datetime
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
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    upsert_settings(tournament_id)
    await inter.response.send_message(f"linked ID `{tournament_id}` to a new tournament", ephemeral=True)


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
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first", ephemeral=True)

    set_channels(tournament_id, announcements.id, match_chats.id)

    await inter.response.send_message(
        f"saved channels {announcements.mention} (announcements) and {match_chats.mention} (match threads) for `{tournament_id}` tournament",
        ephemeral=True
    )


# /setup team <@role> <challonge-team-id> TODO
@setup.command(name="team", description="Map a Discord team role (auto-assigns team id)")
@app_commands.describe(
    tournament_id="tournament id",
    role="Discord team role"
)
async def setup_team_add(
    inter: discord.Interaction,
    tournament_id: str,
    role: discord.Role
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first.", ephemeral=True)

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
            title="Team mapped",
            description="\n".join([
                f"**Tournament ID:** `{tournament_id}`",
                f"**Role:** {role.mention} (`{role.id}`)",
                f"**Team ID:** `{assigned_id}`",
                f"**Display Name:** `{name}`",
            ]),
            color=0xB54882
        ),
        ephemeral=True
    )


# /setup team remove [@role] [challonge-team-id] TODO
# /setup team list TODO


# /setup status
@setup.command(name="status", description="show current config")
@app_commands.describe(tournament_id="tournament ID")
async def setup_status(inter: discord.Interaction, tournament_id: str):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)

    s = get_settings(tournament_id)

    if not s:
        return await inter.response.send_message(f"tournament `{tournament_id}` not found", ephemeral=True)

    teams = list_teams(tournament_id)

    ann = f"<#{s['announcements_ch']}>" if s['announcements_ch'] else "-"
    mc = f"<#{s['match_chats_ch']}>" if s['match_chats_ch'] else "-"

    desc = "\n".join([
        f"**Tournament ID:** `{tournament_id}`",
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

match = app_commands.Group(name="match", description="match utilities")

# /match start <match_id> TODO

# /match poke <match_id> [time|standard] TODO

# /match settime <match_id> <YYYY-MM-DD HH:MM> TODO


# /match report <tournament_id> <match_id> <score_a> <score_b>
@match.command(name="report", description="Report a match score")
@app_commands.describe(
    tournament_id="tournament ID",
    match_id="match id as shown when the round was created",
    score_a="maps for team A",
    score_b="maps for team B"
)
async def match_report(inter: discord.Interaction, tournament_id: str, match_id: int, score_a: int, score_b: int):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)
    if score_a < 0 or score_b < 0:
        return await inter.response.send_message("scores must be non-negative integers.", ephemeral=True)

    m = get_match(tournament_id, match_id)
    if not m:
        return await inter.response.send_message(f"match `#{match_id}` not found for `{tournament_id}`.", ephemeral=True)

    record_result(tournament_id, match_id, score_a, score_b)

    a = m.get("team_a_role_id"); b = m.get("team_b_role_id")
    label = f"<@&{a}> vs <@&{b}>" if a and b else f"match #{match_id}"
    await inter.response.send_message(f"recorded result for {label}: **{score_a}–{score_b}**", ephemeral=True)


# /match add <tournament_id> <swiss|double_elim|round_robin> <round_num>
@match.command(
    name="add",
    description="generate and add N Swiss rounds"
)
@app_commands.describe(
    tournament_id="tournament ID",
    kind="swiss or double_elim (double_elim not implemented yet)",
    rounds="how many rounds to add from the next round"
)
async def match_add(
    inter: discord.Interaction,
    tournament_id: str,
    kind: Literal["swiss", "double_elim"],
    rounds: int,
):
    if not staff_only(inter):
        return await inter.response.send_message("need manage server perms", ephemeral=True)
    if not get_settings(tournament_id):
        return await inter.response.send_message(f"`{tournament_id}` not found; run `/setup new` first", ephemeral=True)
    if kind != "swiss":
        return await inter.response.send_message("double_elim not implemented yet.", ephemeral=True)
    if rounds < 1:
        return await inter.response.send_message("`rounds` must be ≥ 1.", ephemeral=True)

    created_blocks: list[str] = []
    latest = get_latest_round(tournament_id)  # 0 if none

    # canonical team list from mappings
    mapped = list_teams(tournament_id)
    team_ids = [row["team_role_id"] for row in mapped]
    if len(team_ids) < 2 or len(team_ids) % 2 != 0:
        return await inter.response.send_message(
            "need an **even** number of mapped teams (≥2). Use your planned team mapping commands first.",
            ephemeral=True
        )

    for _ in range(rounds):
        target_round = latest + 1

        if round_exists(tournament_id, target_round):
            created_blocks.append(f"**Round {target_round}** — already exists (skipped)")
            latest = target_round
            continue

        if latest == 0 and target_round == 1:
            # first round
            ids = team_ids[:]
            random.shuffle(ids)
            pairings = []
            for i in range(0, len(ids), 2):
                a, b = ids[i], ids[i + 1]
                pairings.append({"match_id": None, "team_a_role_id": a, "team_b_role_id": b, "start_time_local": None})
            assigned = create_swiss_round(tournament_id, target_round, pairings)
            created_blocks.append(
                f"**Round {target_round}**\n" +
                "\n".join(
                    f"• <@&{pairings[i]['team_a_role_id']}> vs <@&{pairings[i]['team_b_role_id']}>  (#{assigned[i]})"
                    for i in range(len(pairings)))
            )
        else:
            # later rounds
            match_count = len(team_ids) // 2
            pairings = [{"match_id": None, "team_a_role_id": None, "team_b_role_id": None, "start_time_local": None}
                        for _ in range(match_count)]
            assigned = create_swiss_round(tournament_id, target_round, pairings)
            created_blocks.append(
                f"**Round {target_round} (placeholders)**\n" +
                "\n".join(f"• (unassigned)  (#{mid})" for mid in assigned)
            )

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
@tournament.command(name="list", description="List all matches by round with scores & dates.")
@app_commands.describe(slug="tournament slug")
async def tournament_list(inter: discord.Interaction, slug: str):
    if not get_settings(slug):
        return await inter.response.send_message(f"`{slug}` not found; run `/setup new` first.", ephemeral=True)

    rows = list_all_matches_full(slug)
    if not rows:
        return await inter.response.send_message(f"No matches found for `{slug}` yet.", ephemeral=True)

    name_map = get_team_display_map(slug)

    # Helpers
    def team_label(role_id: int | None) -> str:
        if role_id is None:
            return "TBD"
        return name_map.get(role_id, f"<@&{role_id}>")

    def ordinal(n: int) -> str:
        if 10 <= (n % 100) <= 20: suf = "th"
        else: suf = {1:"st",2:"nd",3:"rd"}.get(n % 10, "th")
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
            time12 = dt.strftime("%-I:%M%p") if hasattr(dt, "strftime") else dt.strftime("%I:%M%p").lstrip("0")
            # Windows/Py <= 3.10 may not support %-I; fallback:
            if time12.startswith("0"):
                time12 = time12[1:]
            return f"[{month} {day}, {year} at {time12}]"
        except Exception:
            return f"[{s}]"

    # Group purely by round number (hide phase names)
    from collections import defaultdict
    grouped: dict[int | None, list[dict]] = defaultdict(list)
    for r in rows:
        grouped[r["round_no"]].append(r)

    # Sort by round number (None rounds last)
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

        # Build lines for this round
        blocks: list[str] = []
        for r in sorted(round_matches, key=lambda x: x["match_id"]):
            a = team_label(r["team_a_role_id"])
            b = team_label(r["team_b_role_id"])
            sa = r["score_a"]; sb = r["score_b"]

            if r["reported"] and sa is not None and sb is not None:
                score_text = f"{sa}-{sb}"
            else:
                score_text = "_ - _"

            top_line = f"☆    match #{r['match_id']}: {a} vs {b} ━ score: {score_text}"
            when_line = fmt_when(r["start_time_local"])
            block = top_line + (f"\n{when_line}" if when_line else "")
            blocks.append(block)

        # Chunk matches into fields to stay readable
        chunk_size = 8
        for i in range(0, len(blocks), chunk_size):
            chunk = blocks[i:i+chunk_size]
            header = f"**》Round {rn if rn is not None else '-'}**"
            if len(blocks) > chunk_size:
                header += f" (part {i//chunk_size + 1})"
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
            f"round {next_round} does not exist yet. create placeholders first: `/match add tournament_id:{tournament_id} kind:swiss rounds:1`",
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
