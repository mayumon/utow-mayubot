# swiss_helpers.py

from collections import defaultdict
from typing import List, Dict, Tuple, Set


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
    # sort groups by best record first
    keys = sorted(groups.keys(), key=lambda k: (-k[0], k[1]))
    opp = previous_opponents(history)

    # ensure deterministic order
    for k in keys:
        groups[k].sort(key=lambda t: ( -st[t]["wins"], st[t]["losses"], -st[t]["map_diff"], t ))

    # floaters between neighbor groups
    ordered = []
    for k in keys:
        ordered.append(groups[k])

    # flatten with float logic:
    pairs: List[Tuple[int,int]] = []
    carry: int | None = None

    def greedy_pair(bucket: List[int], carry_in: int | None) -> Tuple[List[Tuple[int,int]], int | None]:
        b = bucket[:]
        if carry_in is not None:
            b.insert(0, carry_in)
        out_pairs = []
        used = set()
        i = 0
        while i < len(b):
            if i in used:
                i += 1; continue
            a = b[i]
            # find partner j
            j = None
            for k in range(i+1, len(b)):
                if k in used: continue
                cand = b[k]
                if cand not in opp[a]:  # avoid repeat
                    j = k; break
            if j is None:
                # no clean partner, pick first available to avoid leaving unpaired
                for k in range(i+1, len(b)):
                    if k not in used:
                        j = k; break
            if j is None:
                # unpaired leftover becomes carry_out
                return (out_pairs, a)
            out_pairs.append((a, b[j]))
            used.add(i); used.add(j)
            i += 1
        return (out_pairs, None)

    carry_in = carry
    for bucket in ordered:
        ps, carry_in = greedy_pair(bucket, carry_in)
        pairs.extend(ps)
    # if any carry remains, it means odd total team count (shouldn't happen if even)
    return pairs
