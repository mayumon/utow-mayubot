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
    tournament_name      TEXT PRIMARY KEY,
    tz                  TEXT NOT NULL DEFAULT 'America/Toronto',
    announcements_ch    INTEGER,
    match_chats_ch      INTEGER
);

-- Teams
CREATE TABLE IF NOT EXISTS teams (
    team_role_id       INTEGER PRIMARY KEY,
    team_id  INTEGER NOT NULL,
    display_name       TEXT,
    
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
    
    tournament_name     TEXT NOT NULL REFERENCES settings(tournament_name) ON DELETE CASCADE,
    PRIMARY KEY (tournament_name, match_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_matches_time
    ON matches(tournament_name, start_time_local);
    
CREATE INDEX IF NOT EXISTS idx_teams_challonge_id
    ON teams(tournament_name, team_id);
    
CREATE INDEX IF NOT EXISTS idx_matches_phase_round
    ON matches(tournament_name, phase, round_no);
    
CREATE TABLE IF NOT EXISTS swiss_meta (
    tournament_name  TEXT PRIMARY KEY REFERENCES settings(tournament_name),
    rounds          INTEGER NOT NULL
);

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
    """
    Upsert a role -> team mapping. If team_id is None, auto-assign the next team_id for this tournament.
    Returns the team_id actually stored.
    """
    try:
        with connect() as con:
            cur = con.cursor()  # <-- you had cur = con.cursor (missing parentheses)
            # Decide the id
            assigned_id = team_id if team_id is not None else _next_team_id(tournament_name)

            cur.execute(
                "INSERT INTO teams(team_role_id, team_id, display_name, tournament_name) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(team_role_id) DO UPDATE SET "
                "  team_id=excluded.team_id, "
                "  display_name=excluded.display_name, "
                "  tournament_name=excluded.tournament_name",
                (team_role_id, assigned_id, display_name, tournament_name),
            )
            return assigned_id
    except sqlite3.IntegrityError as e:
        # UNIQUE(tournament_name, team_id) collision
        raise TeamIdInUseError(
            f"Team id {team_id} is already mapped in tournament {tournament_name}"
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


# swiss
def set_swiss_rounds(slug: str, rounds: int) -> None:
    with connect() as con:
        con.execute(
            "INSERT INTO swiss_meta(tournament_name, rounds) VALUES(?, ?) "
            "ON CONFLICT(tournament_name) DO UPDATE SET rounds=excluded.rounds",
            (slug, rounds),
        )


def get_swiss_rounds(slug: str) -> Optional[int]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("SELECT rounds FROM swiss_meta WHERE tournament_name=?", (slug,))
        row = cur.fetchone()
        return int(row["rounds"]) if row else None


# create a swiss round with pairings
# pairings: list[dict]: {"match_id": int, "team_a_role_id": int, "team_b_role_id": int, "start_time_local": Optional[str]}
def create_swiss_round(slug: str, round_no: int, pairings: list[dict]) -> list[int]:
    assigned_ids: list[int] = []
    with connect() as con:
        cur = con.cursor()
        # reserve a starting point once, within this transaction
        next_id = _next_match_id_in_tx(cur, slug)

        for p in pairings:
            mid = p.get("match_id")
            if mid is None:
                mid = next_id
                next_id += 1   # increment locally

            assigned_ids.append(mid)
            cur.execute(
                "INSERT INTO matches(tournament_name, match_id, phase, round_no, "
                " team_a_role_id, team_b_role_id, start_time_local) "
                "VALUES(?, ?, 'swiss', ?, ?, ?, ?) "
                "ON CONFLICT(tournament_name, match_id) DO UPDATE SET "
                " phase='swiss', round_no=excluded.round_no, "
                " team_a_role_id=excluded.team_a_role_id, "
                " team_b_role_id=excluded.team_b_role_id, "
                " start_time_local=COALESCE(excluded.start_time_local, matches.start_time_local)",
                (slug, mid, round_no, p.get("team_a_role_id"), p.get("team_b_role_id"), p.get("start_time_local")),
            )
    return assigned_ids



def list_round_matches(slug: str, round_no: int) -> list[dict]:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT match_id, team_a_role_id, team_b_role_id, start_time_local, score_a, score_b, reported "
            "FROM matches WHERE tournament_name=? AND phase='swiss' AND round_no=? "
            "ORDER BY match_id",
            (slug, round_no),
        )
        return [dict(r) for r in cur.fetchall()]


def record_result(slug: str, match_id: int, score_a: int, score_b: int) -> None:
    with connect() as con:
        con.execute(
            "UPDATE matches SET score_a=?, score_b=?, reported=1 WHERE tournament_name=? AND match_id=?",
            (score_a, score_b, slug, match_id),
        )


def swiss_history(slug: str) -> list[dict]:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT team_a_role_id, team_b_role_id, score_a, score_b, reported, round_no "
            "FROM matches WHERE tournament_name=? AND phase='swiss' ORDER BY round_no, match_id",
            (slug,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_latest_round(slug: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(round_no), 0) AS r FROM matches WHERE tournament_name=? AND phase='swiss'",
            (slug,)
        )
        row = cur.fetchone()
        return int(row["r"] or 0)


def is_round_fully_reported(slug: str, round_no: int) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT COUNT(*) AS c, SUM(CASE WHEN reported=1 THEN 1 ELSE 0 END) AS rep "
            "FROM matches WHERE tournament_name=? AND phase='swiss' AND round_no=?",
            (slug, round_no)
        )
        row = cur.fetchone()
        total = int(row["c"] or 0)
        rep = int(row["rep"] or 0)
        return total > 0 and rep == total


def round_exists(slug: str, round_no: int) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT 1 FROM matches WHERE tournament_name=? AND phase='swiss' AND round_no=? LIMIT 1",
            (slug, round_no)
        )
        return cur.fetchone() is not None


def delete_unreported_round(slug: str, round_no: int) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "DELETE FROM matches WHERE tournament_name=? AND phase='swiss' AND round_no=? AND reported=0",
            (slug, round_no)
        )
        return cur.rowcount


def get_latest_fully_reported_round(slug: str) -> Optional[int]:
    """Return the highest round_no that is fully reported, or None if none."""
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT round_no
            FROM matches
            WHERE tournament_name=? AND phase='swiss'
            GROUP BY round_no
            HAVING COUNT(*) = SUM(CASE WHEN reported=1 THEN 1 ELSE 0 END)
            ORDER BY round_no DESC
            LIMIT 1
        """, (slug,))
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


def round_has_placeholders(slug: str, round_no: int) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT 1
            FROM matches
            WHERE tournament_name=? AND phase='swiss' AND round_no=?
              AND team_a_role_id IS NULL AND team_b_role_id IS NULL
            LIMIT 1
        """, (slug, round_no))
        return cur.fetchone() is not None


def list_round_placeholders(slug: str, round_no: int) -> list[int]:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT match_id
            FROM matches
            WHERE tournament_name=? AND phase='swiss' AND round_no=?
              AND team_a_role_id IS NULL AND team_b_role_id IS NULL
            ORDER BY match_id
        """, (slug, round_no))
        return [int(r["match_id"]) for r in cur.fetchall()]


def assign_pairs_into_round(slug: str, round_no: int, pairs: list[tuple[int, int]]) -> None:

    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT match_id
            FROM matches
            WHERE tournament_name=? AND phase='swiss' AND round_no=?
              AND team_a_role_id IS NULL AND team_b_role_id IS NULL
            ORDER BY match_id
        """, (slug, round_no))
        mids = [int(r["match_id"]) for r in cur.fetchall()]
        if len(mids) != len(pairs):
            raise ValueError(f"pair-count {len(pairs)} != placeholders {len(mids)} in round {round_no}")
        for mid, (a, b) in zip(mids, pairs):
            cur.execute("""
                UPDATE matches
                SET team_a_role_id=?, team_b_role_id=?
                WHERE tournament_name=? AND match_id=? AND phase='swiss' AND round_no=?
            """, (a, b, slug, mid, round_no))


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
                   score_a, score_b, reported
            FROM matches
            WHERE tournament_name=?
            ORDER BY
              CASE phase WHEN 'swiss' THEN 1 WHEN 'playoff' THEN 2 ELSE 3 END,
              COALESCE(round_no, 0),
              match_id
        """, (slug,))
        return [dict(r) for r in cur.fetchall()]
