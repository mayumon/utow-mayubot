# storage.py
# -- small SQLite DB to keep intramural settings

import sqlite3
import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import datetime, timedelta

DB_PATH = "utow.db"

SCHEMA = """
PRAGMA journal_mode=WAL;

-- Settings
CREATE TABLE IF NOT EXISTS settings (
    tournament_name      TEXT PRIMARY KEY,
    tz                  TEXT NOT NULL DEFAULT 'America/Toronto',
    announcements_ch    INTEGER,
    match_chats_ch      INTEGER
);

-- Teams
CREATE TABLE IF NOT EXISTS teams (
    team_role_id       INTEGER PRIMARY KEY,
    team_id  INTEGER NOT NULL,
    
    tournament_name     TEXT NOT NULL REFERENCES settings(tournament_name) ON DELETE CASCADE,
    UNIQUE(tournament_name, team_id)
);

-- Matches
CREATE TABLE IF NOT EXISTS matches (
    match_id            INTEGER NOT NULL,
    start_time_local    TEXT,
    thread_id           INTEGER UNIQUE,
    active_poke_json    TEXT,
    confirm_a           INTEGER DEFAULT 0,
    confirm_b           INTEGER DEFAULT 0,
    
    phase               TEXT,       -- swiss, playoff, NULL
    round_no            INTEGER,
    team_a_role_id      INTEGER,
    team_b_role_id      INTEGER,
    score_a             INTEGER,
    score_b             INTEGER,
    reported            INTEGER DEFAULT 0,
    bracket             TEXT, -- WB, LB, NULL
    
    tournament_name     TEXT NOT NULL REFERENCES settings(tournament_name) ON DELETE CASCADE,
    PRIMARY KEY (tournament_name, match_id)
);

CREATE TABLE IF NOT EXISTS swiss_meta (
    tournament_name  TEXT PRIMARY KEY REFERENCES settings(tournament_name),
    rounds          INTEGER NOT NULL
);

-- Reminders
CREATE TABLE IF NOT EXISTS reminders (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    tournament_name  TEXT NOT NULL,
    match_id         INTEGER NOT NULL,
    when_utc         TEXT NOT NULL,
    kind             TEXT NOT NULL,
    sent             INTEGER DEFAULT 0,
    retry_count      INTEGER DEFAULT 0,
    
    FOREIGN KEY (tournament_name, match_id)
      REFERENCES matches(tournament_name, match_id) ON DELETE CASCADE,
    UNIQUE (tournament_name, match_id, kind)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_matches_time
    ON matches(tournament_name, start_time_local);
    
CREATE INDEX IF NOT EXISTS idx_teams_challonge_id
    ON teams(tournament_name, team_id);
    
CREATE INDEX IF NOT EXISTS idx_matches_phase_round
    ON matches(tournament_name, phase, round_no);
    
CREATE INDEX IF NOT EXISTS idx_reminders_due
  ON reminders(when_utc, sent);
  
CREATE INDEX IF NOT EXISTS idx_matches_phase_round_bracket
  ON matches(tournament_name, phase, round_no, bracket);

"""
class TeamIdInUseError(Exception):
    pass

@contextmanager
def connect():
    con = sqlite3.connect(DB_PATH)
    try:
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        yield con
        con.commit()
    except:
        con.rollback()
        raise
    finally:
        con.close()


def init_db():
    with connect() as con:
        con.executescript(SCHEMA)


# ------------ settings ------------

@dataclass
class Settings:
    tournament_name: str
    tz: str
    announcements_ch: Optional[int]
    match_chats_ch: Optional[int]


# create/update a settings row given a tournament slug
def upsert_settings(tournament_name: str,
                    *,
                    tz: Optional[str] = None,
                    announcements_ch: Optional[int] = None,
                    match_chats_ch: Optional[int] = None) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM settings WHERE tournament_name=?", (tournament_name,))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute("INSERT INTO settings(tournament_name, tz, announcements_ch, match_chats_ch) "
                        "VALUES(?, COALESCE(?, 'America/Toronto'), ?, ?)",
                        (tournament_name, tz, announcements_ch, match_chats_ch),
                        )
        else:
            if tz is not None:
                cur.execute("UPDATE settings SET tz=? WHERE tournament_name=?", (tz, tournament_name))
            if announcements_ch is not None:
                cur.execute("UPDATE settings SET announcements_ch=? WHERE tournament_name=?",
                            (announcements_ch, tournament_name))
            if match_chats_ch is not None:
                cur.execute("UPDATE settings SET match_chats_ch=? WHERE tournament_name=?",
                            (match_chats_ch, tournament_name))


def get_settings(tournament_name: str) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT tournament_name, tz, announcements_ch, match_chats_ch FROM settings WHERE tournament_name=? ",
                    (tournament_name,), )
        row = cur.fetchone()
        return dict(row) if row else None


def set_channels(tournament_name: str, announcements_ch: int, match_chats_ch: int) -> None:
    upsert_settings(tournament_name, announcements_ch=announcements_ch, match_chats_ch=match_chats_ch)


# ------------ teams ------------

def link_team(
    tournament_name: str,
    team_role_id: int,
    team_id: Optional[int] = None,
    display_name: Optional[str] = None
) -> int:
    assigned_id = team_id if team_id is not None else _next_team_id(tournament_name)
    try:
        with connect() as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO teams(team_role_id, team_id, display_name, tournament_name) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(team_role_id) DO UPDATE SET "
                "  team_id=excluded.team_id, "
                "  display_name=COALESCE(excluded.display_name, teams.display_name), "
                "  tournament_name=excluded.tournament_name",
                (team_role_id, assigned_id, display_name, tournament_name),
            )
            return assigned_id
    except sqlite3.IntegrityError as e:
        raise TeamIdInUseError(
            f"Team id {assigned_id} is already mapped in tournament {tournament_name}"
        ) from e


def unlink_team(team_role_id: int) -> None:
    with connect() as con:
        con.execute("DELETE FROM teams WHERE team_role_id=? ", (team_role_id,))


def list_teams(tournament_name: str) -> list[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT team_role_id, team_id, display_name FROM teams WHERE tournament_name=? ORDER BY display_name ",
                    (tournament_name,),)
        return [dict(r) for r in cur.fetchall()]


def get_team_by_role(team_role_id: int) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT team_role_id, team_id, display_name, tournament_name FROM teams WHERE team_role_id=? ",
                (team_role_id,),)
        row = cur.fetchone()
        return dict(row) if row else None


def get_team_by_participant(tournament_name: str, team_id: int) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT team_role_id, team_id, display_name FROM teams WHERE tournament_name=? AND team_id=? ",
                    (tournament_name, team_id),)
        row = cur.fetchone()
        return dict(row) if row else None


# ------------ matches ------------

# set clear_poke = True to drop active poke JSON when overwriting times
def upsert_match(tournament_name: str,
                match_id: int,
                *,
                start_time_local: Optional[str] = None,
                thread_id: Optional[int] = None,
                clear_poke: bool = False,) -> None:
    with connect() as con:
        # dynamic update list to avoid overwriting with NULLs
        sets = []
        args: list[Any] = []

        if start_time_local is not None:
            sets.append("start_time_local=?"); args.append(start_time_local)
        if thread_id is not None:
            sets.append("thread_id=?"); args.append(thread_id)
        if clear_poke:
            sets.append("active_poke_json=NULL")

        if sets:
            con.execute("INSERT INTO matches(tournament_name, match_id) VALUES(?, ?) "
            f"ON CONFLICT(tournament_name, match_id) DO UPDATE SET {', '.join(sets)}", (tournament_name, match_id, *args))
        else:
            # ensure row exists
            con.execute("INSERT OR IGNORE INTO matches(tournament_name, match_id) VALUES(?, ?) ",
                        (tournament_name, match_id),)


# set start time and clear active poke
def set_match_time(tournament_name: str, match_id: int, start_time_local: str) -> None:
    upsert_match(tournament_name, match_id, start_time_local=start_time_local, clear_poke=True)


def set_match_teams(slug: str, match_id: int, *, team_a_role_id: int, team_b_role_id: int) -> None:
    with connect() as con:
        con.execute("""
            UPDATE matches
            SET team_a_role_id=?, team_b_role_id=?
            WHERE tournament_name=? AND match_id=?
        """, (team_a_role_id, team_b_role_id, slug, match_id))


def set_thread(tournament_name: str, match_id: int, thread_id: int) -> None:
    upsert_match(tournament_name, match_id, thread_id=thread_id)


# save or clear the active poke json for a match
def save_active_poke(tournament_name: str, match_id: int, poke_payload: dict[str, Any] | None) -> None:
    with connect() as con:
        if poke_payload is None:
            con.execute("UPDATE matches SET active_poke_json=NULL WHERE tournament_name=? AND match_id=? ",
                        (tournament_name, match_id),)
        else:
            con.execute("UPDATE matches SET active_poke_json=? WHERE tournament_name=? AND match_id=? ",
                    (json.dumps(poke_payload, ensure_ascii=False), tournament_name, match_id),)


def get_match(tournament_name: str, match_id: int) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT tournament_name, match_id, start_time_local, thread_id, active_poke_json, "
            "       confirm_a, confirm_b, team_a_role_id, team_b_role_id, score_a, score_b, reported, phase, round_no "
            "FROM matches WHERE tournament_name=? AND match_id=? ",
            (tournament_name, match_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("active_poke_json"):
            try:
                d["active_poke_json"] = json.loads(d["active_poke_json"])
            except json.JSONDecodeError:
                d["active_poke_json"] = None
        return d


def list_matches(tournament_name: str, with_time_only: bool = False) -> list[dict[str, Any]]:

    with connect() as con:
        cur = con.cursor()
        if with_time_only:
            cur.execute("SELECT tournament_name, match_id, start_time_local, thread_id FROM matches "
                        "WHERE tournament_name=? AND start_time_local IS NOT NULL ORDER BY start_time_local ",
                        (tournament_name,),)
        else:
            cur.execute("SELECT tournament_name, match_id, start_time_local, thread_id FROM matches "
                        "WHERE tournament_name=? ORDER BY match_id ",
                        (tournament_name,),)
        return [dict(r) for r in cur.fetchall()]


# create a swiss round with pairings
# pairings: list[dict]: {"match_id": int, "team_a_role_id": int, "team_b_role_id": int, "start_time_local": Optional[str]}
def create_round(slug: str, round_no: int, pairings: list[dict], phase: str) -> list[int]:
    assigned_ids: list[int] = []
    with connect() as con:
        cur = con.cursor()
        next_id = _next_match_id_in_tx(cur, slug)

        for p in pairings:
            mid = p.get("match_id")
            if mid is None:
                mid = next_id
                next_id += 1

            assigned_ids.append(mid)

            cur.execute(
                """
                INSERT INTO matches(
                    tournament_name, match_id, phase, round_no,
                    team_a_role_id, team_b_role_id, start_time_local, bracket
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tournament_name, match_id) DO UPDATE SET
                  phase=excluded.phase,
                  round_no=excluded.round_no,
                  team_a_role_id=excluded.team_a_role_id,
                  team_b_role_id=excluded.team_b_role_id,
                  start_time_local=COALESCE(excluded.start_time_local, matches.start_time_local),
                  bracket=COALESCE(excluded.bracket, matches.bracket)
                """,
                (
                    slug, mid, phase, round_no,
                    p.get("team_a_role_id"), p.get("team_b_role_id"),
                    p.get("start_time_local"),
                    p.get("bracket"),
                ),
            )
    return assigned_ids


def list_round_matches(slug: str, round_no: int, phase: str) -> list[dict]:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT match_id, team_a_role_id, team_b_role_id, start_time_local, score_a, score_b, reported, bracket "
            "FROM matches WHERE tournament_name=? AND phase=? AND round_no=? "
            "ORDER BY match_id",
            (slug, phase, round_no),
        )
        return [dict(r) for r in cur.fetchall()]


def swiss_history(slug: str) -> list[dict]:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT team_a_role_id, team_b_role_id, score_a, score_b, reported, round_no "
            "FROM matches WHERE tournament_name=? AND phase='swiss' ORDER BY round_no, match_id",
            (slug,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_latest_round(slug: str, phase: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(round_no), 0) AS r FROM matches WHERE tournament_name=? AND phase=?",
            (slug, phase,)
        )
        row = cur.fetchone()
        return int(row["r"] or 0)


def is_round_fully_reported(slug: str, round_no: int, phase: str) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT COUNT(*) AS c, SUM(CASE WHEN reported=1 THEN 1 ELSE 0 END) AS rep "
            "FROM matches WHERE tournament_name=? AND phase=? AND round_no=?",
            (slug, phase, round_no)
        )
        row = cur.fetchone()
        total = int(row["c"] or 0)
        rep = int(row["rep"] or 0)
        return total > 0 and rep == total


def round_exists(slug: str, round_no: int, phase: str) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT 1 FROM matches WHERE tournament_name=? AND phase=? AND round_no=? LIMIT 1",
            (slug, phase, round_no)
        )
        return cur.fetchone() is not None


def delete_unreported_round(slug: str, round_no: int, phase: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "DELETE FROM matches WHERE tournament_name=? AND phase=? AND round_no=? AND reported=0",
            (slug, phase, round_no)
        )
        return cur.rowcount


def get_latest_fully_reported_round(slug: str, phase: str) -> Optional[int]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT round_no
            FROM matches
            WHERE tournament_name=? AND phase=?
            GROUP BY round_no
            HAVING COUNT(*) = SUM(CASE WHEN reported=1 THEN 1 ELSE 0 END)
            ORDER BY round_no DESC
            LIMIT 1
        """, (slug, phase))
        row = cur.fetchone()
        return int(row["round_no"]) if row else None


def _next_team_id(tournament_name: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COALESCE(MAX(team_id), 0) + 1 AS n FROM teams WHERE tournament_name=?", (tournament_name,))
        row = cur.fetchone()
        return int(row["n"])


def _next_match_id(tournament_name: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COALESCE(MAX(match_id), 0) + 1 AS n FROM matches WHERE tournament_name=?", (tournament_name,))
        row = cur.fetchone()
        return int(row["n"])


def _next_match_id_in_tx(cur, tournament_name: str) -> int:
    cur.execute("SELECT COALESCE(MAX(match_id), 0) AS n FROM matches WHERE tournament_name=?", (tournament_name,))
    row = cur.fetchone()
    return int(row["n"]) + 1


def round_has_placeholders(slug: str, round_no: int, phase: str) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT 1
            FROM matches
            WHERE tournament_name=? AND phase=? AND round_no=?
              AND team_a_role_id IS NULL AND team_b_role_id IS NULL
            LIMIT 1
        """, (slug, phase, round_no))
        return cur.fetchone() is not None


def list_round_placeholders(slug: str, round_no: int, phase: str) -> list[int]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT match_id
            FROM matches
            WHERE tournament_name=? AND phase=? AND round_no=?
              AND team_a_role_id IS NULL AND team_b_role_id IS NULL
            ORDER BY match_id
        """, (slug, phase, round_no))
        return [int(r["match_id"]) for r in cur.fetchall()]


def assign_pairs_into_round(slug: str, round_no: int, pairs: list[tuple[int, int]], phase: str) -> None:

    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT match_id
            FROM matches
            WHERE tournament_name=? AND phase=? AND round_no=?
              AND team_a_role_id IS NULL AND team_b_role_id IS NULL
            ORDER BY match_id
        """, (slug, phase, round_no))
        mids = [int(r["match_id"]) for r in cur.fetchall()]
        if len(mids) != len(pairs):
            raise ValueError(f"pair-count {len(pairs)} != placeholders {len(mids)} in round {round_no}")
        for mid, (a, b) in zip(mids, pairs):
            cur.execute("""
                UPDATE matches
                SET team_a_role_id=?, team_b_role_id=?
                WHERE tournament_name=? AND match_id=? AND phase=? AND round_no=?
            """, (a, b, slug, mid, phase, round_no))


def get_team_display_map(slug: str) -> dict[int, str]:
    """role_id -> display label (display_name if present, else @role mention placeholder)."""
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT team_role_id, COALESCE(NULLIF(TRIM(display_name), ''), NULL) AS dn
            FROM teams
            WHERE tournament_name=?
        """, (slug,))
        out = {}
        for r in cur.fetchall():
            rid = int(r["team_role_id"])
            dn = r["dn"]
            out[rid] = dn if dn else f"<@&{rid}>"
        return out


def list_all_matches_full(slug: str) -> list[dict]:
    """All matches for a tournament with phase/round and core fields."""
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT match_id, phase, round_no, start_time_local,
                   team_a_role_id, team_b_role_id,
                   score_a, score_b, reported, bracket
            FROM matches
            WHERE tournament_name=?
            ORDER BY
                CASE phase
                    WHEN 'swiss' THEN 1
                    WHEN 'roundrobin' THEN 2
                    WHEN 'double_elim' THEN 3
                    ELSE 9
                  END,
                  COALESCE(round_no, 0),
                  match_id
        """, (slug,))
        return [dict(r) for r in cur.fetchall()]


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")

def _parse_local(start_time_local: str, tz_str: str) -> datetime:
    # start_time_local: "YYYY-MM-DD HH:MM" (naive, stored as local)
    naive = datetime.strptime(start_time_local, "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=ZoneInfo(tz_str))


def schedule_match_reminders(slug: str, match_id: int) -> int:

    # load match + tz
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT start_time_local
            FROM matches
            WHERE tournament_name=? AND match_id=?
        """, (slug, match_id))
        m = cur.fetchone()
        if not m or not m["start_time_local"]:
            return 0

        cur.execute("SELECT tz FROM settings WHERE tournament_name=?", (slug,))
        s = cur.fetchone()
        tz = s["tz"] if s and s["tz"] else "America/Toronto"

    tzinfo = safe_zoneinfo(tz)

    # parse local start
    start_local = datetime.strptime(m["start_time_local"], "%Y-%m-%d %H:%M").replace(tzinfo=tzinfo)
    start_utc = start_local.astimezone(safe_zoneinfo("UTC"))
    now_utc = _now_utc_naive()
    now_local = datetime.utcnow().replace(tzinfo=safe_zoneinfo("UTC")).astimezone(tzinfo)

    desired: list[tuple[str, datetime]] = []

    pre1h_local = start_local - timedelta(hours=1)
    pre1h_utc = pre1h_local.astimezone(safe_zoneinfo("UTC"))

    early_kind = None
    early_dt_local = None

    if start_local.hour < 14:
        early_kind = "pre2h"
        early_dt_local = start_local - timedelta(hours=2)
    else:
        early_kind = "noon"
        early_dt_local = start_local.replace(hour=12, minute=0)

        # if noon already passed, try pre2h instead
        if early_dt_local <= now_local:
            alt = start_local - timedelta(hours=2)
            if alt > now_local:
                early_kind = "pre2h"
                early_dt_local = alt
            else:
                early_kind = None  # drop early reminder entirely

    if early_kind is not None:
        desired.append((early_kind, early_dt_local.astimezone(safe_zoneinfo("UTC"))))
    desired.append(("pre1h", pre1h_utc))

    # if pre1h is past, schedule asap
    fixed: list[tuple[str, datetime]] = []
    for kind, when_dt_utc in desired:
        # store minute precision
        when_dt_utc = when_dt_utc.replace(second=0, microsecond=0)
        if when_dt_utc.replace(tzinfo=None) <= now_utc:
            if kind == "pre1h":
                asap = (datetime.utcnow() + timedelta(minutes=1)).replace(second=0, microsecond=0)
                fixed.append((kind, asap.replace(tzinfo=None)))
            else:
                # drop early reminder if its already past
                continue
        else:
            fixed.append((kind, when_dt_utc.replace(tzinfo=None)))

    # upsert + reset sent, delete obsolete kinds
    desired_kinds = {k for k, _ in fixed}
    with connect() as con:
        # delete kinds we no longer want
        if desired_kinds:
            con.execute(f"""
                DELETE FROM reminders
                 WHERE tournament_name=? AND match_id=? AND kind NOT IN ({",".join("?"*len(desired_kinds))})
            """, (slug, match_id, *desired_kinds))
        else:
            con.execute("""
                DELETE FROM reminders
                 WHERE tournament_name=? AND match_id=?
            """, (slug, match_id))

        # upsert wanted kinds, reset sent=0
        for kind, when_utc in fixed:
            con.execute("""
                INSERT INTO reminders(tournament_name, match_id, when_utc, kind, sent)
                VALUES(?, ?, ?, ?, 0)
                ON CONFLICT(tournament_name, match_id, kind)
                DO UPDATE SET when_utc=excluded.when_utc, sent=0
            """, (slug, match_id, when_utc.strftime("%Y-%m-%d %H:%M"), kind))

    return len(fixed)


def list_reminders(slug: str, match_id: int | None = None) -> list[dict]:
    with connect() as con:
        cur = con.cursor()
        if match_id is None:
            cur.execute("SELECT id, match_id, when_utc, kind, sent FROM reminders "
                        "WHERE tournament_name=? ORDER BY when_utc", (slug,))
        else:
            cur.execute("SELECT id, match_id, when_utc, kind, sent FROM reminders "
                        "WHERE tournament_name=? AND match_id=? ORDER BY when_utc", (slug, match_id))
        return [dict(r) for r in cur.fetchall()]


def fetch_due_reminders(now_utc: datetime, limit: int = 50) -> list[dict]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT r.id, r.tournament_name, r.match_id, r.when_utc, r.kind,
                   m.thread_id, m.team_a_role_id, m.team_b_role_id
            FROM reminders r
            JOIN matches m
              ON m.tournament_name=r.tournament_name AND m.match_id=r.match_id
            WHERE r.sent=0 AND r.when_utc <= ?
            ORDER BY r.when_utc ASC
            LIMIT ?
        """, (_iso(now_utc), limit))
        return [dict(r) for r in cur.fetchall()]


def mark_reminder_sent(reminder_id: int) -> None:
    with connect() as con:
        con.execute("UPDATE reminders SET sent=1 WHERE id=?", (reminder_id,))


def _now_utc_naive() -> datetime:
    return datetime.utcnow().replace(tzinfo=None)


def safe_zoneinfo(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        try:
            from dateutil.tz import gettz
            z = gettz(tz_name)
            if z:
                return z
        except Exception:
            pass
        return ZoneInfo("UTC")


def schedule_all_match_reminders(slug: str) -> tuple[int, int]:
    rows = list_matches(slug, with_time_only=True)
    matches_updated = 0
    reminders_total = 0
    for r in rows:
        mid = int(r["match_id"])
        n = schedule_match_reminders(slug, mid)
        if n > 0:
            matches_updated += 1
            reminders_total += n
    return matches_updated, reminders_total


class MatchUpdateError(Exception):
    pass


# storage.py
class MatchUpdateError(Exception):
    pass


def record_result(slug: str, match_id: int, score_a: int, score_b: int) -> None:
    """
    Write scores and mark reported. Standings are computed from matches,
    so we don't maintain per-team counters anymore.
    """
    with connect() as con:
        cur = con.cursor()

        # fetch match + teams
        cur.execute("""
            SELECT team_a_role_id AS a_id, team_b_role_id AS b_id
            FROM matches
            WHERE tournament_name=? AND match_id=?
        """, (slug, match_id))
        row = cur.fetchone()
        if not row:
            raise MatchUpdateError(f"match #{match_id} not found")

        a_id, b_id = row["a_id"], row["b_id"]
        if a_id is None or b_id is None:
            raise MatchUpdateError("both teams must be assigned before reporting")

        # ensure both teams are mapped to this tournament
        cur.execute("""
            SELECT COUNT(*) AS c
            FROM teams
            WHERE tournament_name=? AND team_role_id IN (?,?)
        """, (slug, a_id, b_id))
        if int(cur.fetchone()["c"]) != 2:
            raise MatchUpdateError("one or both teams are not mapped to this tournament")

        # write new result (overwrite if re-reported)
        cur.execute("""
            UPDATE matches
            SET score_a=?, score_b=?, reported=1
            WHERE tournament_name=? AND match_id=?
        """, (score_a, score_b, slug, match_id))


def compute_standings(slug: str, *, phase: str | None = None) -> list[dict]:
    """
    Primary: match wins
    Tiebreaks: map differential, map wins, team_id
    """
    with connect() as con:
        cur = con.cursor()
        sql = f"""
            SELECT
              t.team_role_id AS team_role_id,

              -- match wins / draws / losses from reported matches
              COALESCE(SUM(CASE
                WHEN (m.team_a_role_id=t.team_role_id AND m.score_a>m.score_b) OR
                     (m.team_b_role_id=t.team_role_id AND m.score_b>m.score_a)
                THEN 1 ELSE 0 END), 0) AS wins,

              COALESCE(SUM(CASE
                WHEN m.score_a = m.score_b THEN 1 ELSE 0 END), 0) AS draws,

              COALESCE(SUM(CASE
                WHEN (m.team_a_role_id=t.team_role_id AND m.score_a<m.score_b) OR
                     (m.team_b_role_id=t.team_role_id AND m.score_b<m.score_a)
                THEN 1 ELSE 0 END), 0) AS losses,

              -- map totals for tiebreaks
              COALESCE(SUM(CASE
                WHEN m.team_a_role_id=t.team_role_id THEN m.score_a
                WHEN m.team_b_role_id=t.team_role_id THEN m.score_b
                ELSE 0 END), 0) AS map_wins,

              COALESCE(SUM(CASE
                WHEN m.team_a_role_id=t.team_role_id THEN m.score_b
                WHEN m.team_b_role_id=t.team_role_id THEN m.score_a
                ELSE 0 END), 0) AS map_losses,

              MIN(t.team_id) AS team_id_for_tiebreak

            FROM teams t
            LEFT JOIN matches m
              ON m.tournament_name=t.tournament_name
             AND (m.team_a_role_id=t.team_role_id OR m.team_b_role_id=t.team_role_id)
             AND m.reported=1
             {"AND m.phase=?" if phase else ""}

            WHERE t.tournament_name=?
            GROUP BY t.team_role_id
        """
        args = ([phase] if phase else []) + [slug]
        cur.execute(sql, args)
        rows = [dict(r) for r in cur.fetchall()]

    for r in rows:
        r["team_role_id"] = int(r["team_role_id"])
        for k in ("wins","draws","losses","map_wins","map_losses"):
            r[k] = int(r[k])
        r["md"] = r["map_wins"] - r["map_losses"]
        r["team_id_for_tiebreak"] = (
            int(r["team_id_for_tiebreak"]) if r["team_id_for_tiebreak"] is not None else 10**9
        )

    # sort: wins DESC, map diff DESC, map wins DESC, team_id ASC
    rows.sort(key=lambda r: (-r["wins"], -r["md"], -r["map_wins"], r["team_id_for_tiebreak"]))
    return rows

def ranked_team_ids(slug: str, *, phase: str | None = None) -> list[int]:
    rows = compute_standings(slug, phase=phase)
    if rows:
        return [r["team_role_id"] for r in rows]

    # fallback
    teams = list_teams(slug)
    teams.sort(key=lambda r: int(r["team_id"]))
    return [int(r["team_role_id"]) for r in teams]
