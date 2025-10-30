# swiss_helpers.py

from collections import defaultdict
from typing import List, Dict, Tuple, Set, Optional


def compute_standings(teams: List[int], history: List[dict]) -> Dict[int, dict]:
    # teams: list of role_ids
    # history: from swiss_history(slug)
    W = defaultdict(int); L = defaultdict(int); MAP = defaultdict(int)
    for h in history:
        if not h["reported"]:
            continue
        a = h["team_a_role_id"]; b = h["team_b_role_id"]
        sa = int(h["score_a"] or 0); sb = int(h["score_b"] or 0)
        if sa > sb:
            W[a]+=1; L[b]+=1
        elif sb > sa:
            W[b]+=1; L[a]+=1
        # map diff optional:
        MAP[a]+= (sa - sb)
        MAP[b]+= (sb - sa)
    # ensure all teams present
    st = {}
    for t in teams:
        st[t] = {"team": t, "wins": W[t], "losses": L[t], "map_diff": MAP[t]}
    return st


def previous_opponents(history: List[dict]) -> Dict[int, Set[int]]:
    opp = defaultdict(set)
    for h in history:
        a = h["team_a_role_id"]; b = h["team_b_role_id"]
        if a and b:
            opp[a].add(b); opp[b].add(a)
    return opp


# swiss pairing
def pair_next_round(teams: List[int], history: List[dict]) -> List[Tuple[int,int]]:
    st = compute_standings(teams, history)
    groups = defaultdict(list)
    for t, row in st.items():
        groups[(row["wins"], row["losses"])].append(t)

    # sort groups best→worst; inside group deterministic by (wins desc, losses asc, map_diff desc, id)
    keys = sorted(groups.keys(), key=lambda k: (-k[0], k[1]))
    for k in keys:
        groups[k].sort(key=lambda t: (-st[t]["wins"], st[t]["losses"], -st[t]["map_diff"], t))

    opp = previous_opponents(history)

    # turn into a list of buckets from top bracket to bottom bracket
    buckets: List[List[int]] = [groups[k][:] for k in keys]

    pairs: List[Tuple[int,int]] = []
    i = 0
    carry: Optional[int] = None

    while i < len(buckets):
        cur = buckets[i][:]
        if carry is not None:
            cur.insert(0, carry)

        # Case A: even size → try perfect no-repeat pairing within bucket
        if len(cur) % 2 == 0:
            paired = _pair_bucket_no_repeats(cur, opp)
            if paired is not None:
                pairs.extend(paired)
                carry = None
                i += 1
                continue
            # if impossible, fall through to leave one as carry (choose best leftover)

        # Case B: odd size OR Case A failed → choose a “leftover” that can be cleanly paired in next bucket
        # Try each candidate as leftover (stable order), and see if it has a non-repeat partner in next bucket.
        next_bucket = buckets[i+1] if i+1 < len(buckets) else []
        chosen_leftover_index: Optional[int] = None

        for idx, cand in enumerate(cur):
            # try pairing the rest of 'cur' (without cand) internally with no repeats
            rest = cur[:idx] + cur[idx+1:]
            if len(rest) % 2 == 1:
                continue  # must be even to pair internally
            if _pair_bucket_no_repeats(rest, opp) is None:
                continue  # cannot pair the rest cleanly → skip

            # can 'cand' be matched to someone in next_bucket without a repeat?
            ok = any( (nb not in opp[cand]) for nb in next_bucket )
            if ok:
                chosen_leftover_index = idx
                # Build the internal pairs for 'rest' deterministically and stash; we’ll add cross pair later
                internal = _pair_bucket_no_repeats(rest, opp)
                assert internal is not None
                pairs.extend(internal)
                carry = cand
                # Remove from actual bucket; next loop will see carry injected into next bucket
                buckets[i] = []  # we've consumed this bucket into pairs
                break

        if chosen_leftover_index is not None:
            i += 1
            continue

        # Case C: no way to avoid a repeat even with a floater → minimize repeats.
        # Strategy: pair greedily but prefer partners that DO NOT repeat; if forced, allow exactly the minimal repeats.
        # We’ll try internal best-effort pairing and leave one as carry (first), then match carry in next bucket preferring non-repeat.
        if len(cur) >= 2:
            # simple greedy: always pick first 'a', find first non-repeat partner; else take first partner
            used = [False]*len(cur)
            made_any = False
            for x in range(len(cur)):
                if used[x]:
                    continue
                a = cur[x]; used[x] = True
                j = None
                # prefer non-repeat
                for y in range(x+1, len(cur)):
                    if not used[y] and (cur[y] not in opp[a]):
                        j = y; break
                # fallback: allow one repeat if needed
                if j is None:
                    for y in range(x+1, len(cur)):
                        if not used[y]:
                            j = y; break
                if j is None:
                    # leftover becomes carry
                    carry = a
                    made_any = True
                    break
                used[j] = True
                pairs.append((a, cur[j]))
                made_any = True
            if made_any and carry is None:
                # all paired inside; advance
                i += 1
                continue
            else:
                # we have a carry; advance to try to match it with next bucket
                i += 1
                continue

        # nothing left in this bucket; advance
        i += 1

    # If a carry still remains with no next bucket, there were an odd number of teams (shouldn’t happen).
    # Just ignore silently (or raise) — your storage/create_round expects even count already.
    return pairs


def _pair_bucket_no_repeats(bucket: List[int], opp: Dict[int, Set[int]]) -> Optional[List[Tuple[int,int]]]:
    n = len(bucket)
    used = [False]*n
    out: List[Tuple[int,int]] = []

    # precompute adjacency: who is allowed (no prior meeting)
    allow = [[True]*n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i==j:
                allow[i][j] = False
            else:
                ai, aj = bucket[i], bucket[j]
                if aj in opp[ai]:
                    allow[i][j] = False

    def dfs() -> bool:
        # find first unused
        try:
            i = next(k for k in range(n) if not used[k])
        except StopIteration:
            return True
        used[i] = True
        for j in range(i+1, n):
            if not used[j] and allow[i][j]:
                used[j] = True
                out.append((bucket[i], bucket[j]))
                if dfs():
                    return True
                out.pop()
                used[j] = False
        used[i] = False
        return False

    if dfs():
        return out
    return None
