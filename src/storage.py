# storage.py
# -- small SQLite DB to keep intramural settings

import sqlite3
import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Optional

DB_PATH = "utow.db"

SCHEMA = """
PRAGMA journal_mode=WAL;

-- Settings
CREATE TABLE IF NOT EXISTS settings (
    challonge_slug      TEXT PRIMARY KEY,
    tz                  TEXT NOT NULL DEFAULT 'America/Toronto',
    announcements_ch    INTEGER,
    match_chats_ch      INTEGER
);

-- Teams
CREATE TABLE IF NOT EXISTS teams (
    team_role_id       INTEGER PRIMARY KEY,
    challonge_team_id  INTEGER NOT NULL,
    display_name       TEXT,
    
    challonge_slug     TEXT NOT NULL REFERENCES settings(challonge_slug) ON DELETE CASCADE,
    UNIQUE(challonge_slug, challonge_team_id)
);

-- Matches
CREATE TABLE IF NOT EXISTS matches (
    match_id            INTEGER NOT NULL,
    start_time_local    TEXT,
    thread_id           INTEGER UNIQUE,
    active_poke_json    TEXT,
    confirm_a           INTEGER DEFAULT 0,
    confirm_b           INTEGER DEFAULT 0,
    
    challonge_slug     TEXT NOT NULL REFERENCES settings(challonge_slug) ON DELETE CASCADE,
    PRIMARY KEY (challonge_slug, match_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_matches_time
    ON matches(challonge_slug, start_time_local);
    
CREATE INDEX IF NOT EXISTS idx_teams_challonge_id
    ON teams(challonge_slug, challonge_team_id);

"""


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
    challonge_slug: str
    tz: str
    announcements_ch: Optional[int]
    match_chats_ch: Optional[int]


# create/update a settings row given a tournament slug
def upsert_settings(challonge_slug: str,
                    *,
                    tz: Optional[str] = None,
                    announcements_ch: Optional[int] = None,
                    match_chats_ch: Optional[int] = None) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM settings WHERE challonge_slug=?", (challonge_slug,))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute("INSERT INTO settings(challonge_slug, tz, announcements_ch, match_chats_ch) "
                        "VALUES(?, COALESCE(?, 'America/Toronto'), ?, ?)",
                        (challonge_slug, tz, announcements_ch, match_chats_ch),
                        )
        else:
            if tz is not None:
                cur.execute("UPDATE settings SET tz=? WHERE challonge_slug=?", (tz, challonge_slug))
            if announcements_ch is not None:
                cur.execute("UPDATE settings SET announcements_ch=? WHERE challonge_slug=?",
                            (announcements_ch, challonge_slug))
            if match_chats_ch is not None:
                cur.execute("UPDATE settings SET match_chats_ch=? WHERE challonge_slug=?",
                            (match_chats_ch, challonge_slug))


def get_settings(challonge_slug: str) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT challonge_slug, tz, announcements_ch, match_chats_ch FROM settings WHERE challonge_slug=? ",
                    (challonge_slug,), )
        row = cur.fetchone()
        return dict(row) if row else None


def set_channels(challonge_slug: str, announcements_ch: int, match_chats_ch: int) -> None:
    upsert_settings(challonge_slug, announcements_ch=announcements_ch, match_chats_ch=match_chats_ch)


# ------------ teams ------------

def link_team(challonge_slug: str, team_role_id: int, challonge_team_id: int, display_name: Optional[str] = None) -> None:
    with connect() as con:
        con.execute(
            "INSERT INTO teams(team_role_id, challonge_team_id, display_name, challonge_slug) "
            "VALUES(?, ?, ?, ?) "
            "ON CONFLICT(team_role_id) DO UPDATE SET challonge_team_id=excluded.challonge_team_id, display_name=excluded.display_name, challonge_slug=excluded.challonge_slug",
            (team_role_id, challonge_team_id, display_name, challonge_slug),
        )


def unlink_team(team_role_id: int) -> None:
    with connect() as con:
        con.execute("DELETE FROM teams WHERE team_role_id=? ", (team_role_id,))


def list_teams(challonge_slug: str) -> list[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT team_role_id, challonge_team_id, display_name FROM teams WHERE challonge_slug=? ORDER BY display_name ",
                    (challonge_slug,),)
        return [dict(r) for r in cur.fetchall()]


def get_team_by_role(team_role_id: int) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT team_role_id, challonge_team_id, display_name, challonge_slug FROM teams WHERE team_role_id=? ",
                (team_role_id,),)
        row = cur.fetchone()
        return dict(row) if row else None


def get_team_by_participant(challonge_slug: str, challonge_team_id: int) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT team_role_id, challonge_team_id, display_name FROM teams WHERE challonge_slug=? AND challonge_team_id=? ",
                    (challonge_slug, challonge_team_id),)
        row = cur.fetchone()
        return dict(row) if row else None


# ------------ matches ------------

# set clear_poke = True to drop active poke JSON when overwriting times
def upsert_match(challonge_slug: str,
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
            con.execute("INSERT INTO matches(challonge_slug, match_id) VALUES(?, ?) "
            f"ON CONFLICT(challonge_slug, match_id) DO UPDATE SET {', '.join(sets)}", (challonge_slug, match_id, *args))
        else:
            # ensure row exists
            con.execute("INSERT OR IGNORE INTO matches(challonge_slug, match_id) VALUES(?, ?) ",
                        (challonge_slug, match_id),)


# set start time and clear active poke
def set_match_time(challonge_slug: str, match_id: int, start_time_local: str) -> None:
    upsert_match(challonge_slug, match_id, start_time_local=start_time_local, clear_poke=True)


def set_thread(challonge_slug: str, match_id: int, thread_id: int) -> None:
    upsert_match(challonge_slug, match_id, thread_id=thread_id)


# save or clear the active poke json for a match
def save_active_poke(challonge_slug: str, match_id: int, poke_payload: dict[str, Any] | None) -> None:
    with connect() as con:
        if poke_payload is None:
            con.execute("UPDATE matches SET active_poke_json=NULL WHERE challonge_slug=? AND match_id=? ",
                        (challonge_slug, match_id),)
        else:
            con.execute("UPDATE matches SET active_poke_json=? WHERE challonge_slug=? AND match_id=? ",
                    (json.dumps(poke_payload, ensure_ascii=False), challonge_slug, match_id),)

def get_match(challonge_slug: str, match_id: int) -> Optional[dict[str, Any]]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT challonge_slug, match_id, start_time_local, thread_id, active_poke_json, confirm_a, confirm_b "
                    "FROM matches WHERE challonge_slug=? AND match_id=? ",
                    (challonge_slug, match_id),)

        row = cur.fetchone()

        if not row:
            return None

        d = dict(row)
        if d.get("active_poke_json"):
            try:
                 d["active_poke_json"] = json.loads(d["active_poke_json"])  # type: ignore[assignment]
            except json.JSONDecodeError:
                d["active_poke_json"] = None

        return d


def list_matches(challonge_slug: str, with_time_only: bool = False) -> list[dict[str, Any]]:

    with connect() as con:
        cur = con.cursor()
        if with_time_only:
            cur.execute("SELECT challonge_slug, match_id, start_time_local, thread_id FROM matches "
                        "WHERE challonge_slug=? AND start_time_local IS NOT NULL ORDER BY start_time_local ",
                        (challonge_slug,),)
        else:
            cur.execute("SELECT challonge_slug, match_id, start_time_local, thread_id FROM matches "
                        "WHERE challonge_slug=? ORDER BY match_id ",
                        (challonge_slug,),)
        return [dict(r) for r in cur.fetchall()]