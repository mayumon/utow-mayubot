# dev_smoke_storage.py
# -- integration test for storage.py

from src.storage import *

SLUG = "utow-season-YYYY"

def main():
    init_db()

    print("--- settings cases ---")

    # case 1: settings - insert new settings row and update channels
    upsert_settings(SLUG, tz="America/Toronto")
    set_channels(SLUG, announcements_ch=12345, match_chats_ch=67890)
    print("case 1: settings - insert new settings row and update channels:", get_settings(SLUG))

    print("\n--- teams cases ---")

    # case 1: insert two team-role mappings
    link_team(SLUG, team_role_id=111, challonge_team_id=333, display_name="UTOW A")
    link_team(SLUG, team_role_id=222, challonge_team_id=444, display_name="UTOW B")
    print("case 1: insert two team-role mappings:\n", list_teams(SLUG))

    # case 2: update a team’s display name
    link_team(SLUG, team_role_id=111, challonge_team_id=333, display_name="UTOW C")
    print("case 2: update a team:\n", list_teams(SLUG))

    # case 3: unlink a team
    unlink_team(222)
    print("case 3: unlink a team\n", list_teams(SLUG))

    print("\n--- matches cases ---")

    # case 1: insert and get match
    upsert_match(SLUG, 1010)
    set_match_time(SLUG, 1010, "2025-10-5 21:00")
    set_thread(SLUG, 1010, 999888777)
    save_active_poke(SLUG, 1010, {"proposed": "2025-10-5 21:00"})
    print("case 1: insert and get match:\n", get_match(SLUG, 1010))

    # case 2: listing and filtering
    upsert_match(SLUG, 1011, start_time_local="2025-10-04 20:00")
    print("case 2: listing and filtering (with_time_only):\n", list_matches(SLUG, with_time_only=True))
    print("case 2: listing and filtering (all matches):\n", list_matches(SLUG, with_time_only=False))

if __name__ == "__main__":
    main()
