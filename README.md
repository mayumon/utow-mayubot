set up for any future users

set env vars locally
run py -m src.main on terminal OR Start-Job { py -m src.main } on windows terminal

## ▶ commands

### setup 🛠️ (staff only)
- `/setup new <tournament_id>` - create new tournament.
- `/setup channels <tournament_id> <#announcements> <#match-chats>` – save channels.
- `/setup team add <tournament_id> <@role>` - map a team.
- `/setup team remove <tournament_id> [@role|team_id]` - unmap.
- `/setup team list <tournament_id>` - show mapped teams.
- `/setup status <tournament_id>` - show setup info.


### reminders ⏰ (staff only)
- `/reminders set <tournament_id> [match_id]` - schedule 2 reminders for each game.
- `/reminders list <tournament_id> [match_id]` - see pending/sent reminders.


### matches 🎮
- `/match thread create <tournament_id> <match_id>` - make a private thread + invite both teams.
- `/match settime <when> [tournament_id] [match_id]` - re-set a match's time. also reschedules reminders.
    - **players**: run inside your match thread.
    - **staff**: run in match thread or another channel with IDs.
- `/match setteam <tournament_id> <match_id> <@team_a> <@team_b>` - assign teams to matches.
- `/match report <score_a> <score_b> [tournament_id] [match_id]` - saves match score.
  - **players**: run inside your match thread.
  - **staff**: run in match or another channel with IDs.
- `/match add <tournament_id> <swiss|roundrobin|double_elim> [rounds] <start_time>` - create one or more rounds of matches starting atthe given start time.


### tournament 📣
- `/tournament schedule <tournament_id>` - list all matches by phase/round.
- `/tournament standings <tournament_id> [all|swiss|roundrobin|double_elim]` – show current rankings.
- `/tournament announcement <tournament_id> <post:true|false>` - weekly recap/preview (post to channel or preview).
- `/tournament refresh <tournament_id> [auto|swiss|double_elim]` - fill next round placeholders from results (for swiss and double elimination only).





### coming soon:

- fix /tournament standings (error somewhere)
- hosting
- add a team list or smth 
- bye teams bug???
- add some sort of log idk? if i can like of people using commands and making changes
- faciltiate weekly announcement + thread w one command