"""
SolTrace - Módulo de coleta de dados on-chain da Solana.
Rastreia fundos até pararem de se mover — sem limite rígido de hops.
Detecta: CEX, DEX swap, Bridge, carteiras estacionadas (PARKED), splits (1→N).
"""

import httpx
import os
from typing import Optional
from datetime import datetime

from cex_database import (
    CEX_ADDRESSES, BRIDGE_PROGRAMS, DEFI_PROGRAMS, CEX_NAME_PATTERNS,
    ALL_DEX_PROGRAMS,
    is_cex_address, is_dex_program, is_bridge_program,
    detect_cex_from_label, get_entity_info, classify_address,
)

HELIUS_API_KEY   = os.getenv("HELIUS_API_KEY", "")
HELIUS_BASE_URL  = "https://api.helius.xyz/v0"
SOLSCAN_API_KEY  = os.getenv("SOLSCAN_API_KEY", "")
SOLSCAN_BASE_URL = "https://public-api.solscan.io"
SOLSCAN_PRO_URL  = "https://pro-api.solscan.io/v2.0"
SOLANA_RPC_URL   = "https://api.mainnet-beta.solana.com"

SOL_MINT    = "So11111111111111111111111111111111111111112"
SOL_SYMBOLS = {"sol", "solana", SOL_MINT.lower()}

# Limites de segurança para evitar explosão de BFS
MAX_BFS_WALLETS   = 80   # máximo de carteiras únicas rastreadas
MAX_OUTGOING_NODE = 15   # máximo de saídas por carteira (splits)
MAX_BFS_DEPTH     = 20   # profundidade máxima absoluta de segurança

HELIUS_SOURCE_TO_DEX: dict[str, tuple[str, str]] = {
    "JUPITER":   ("Jupiter Aggregator v6",  "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
    "RAYDIUM":   ("Raydium AMM v4",          "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"),
    "ORCA":      ("Orca Whirlpools",          "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),
    "METEORA":   ("Meteora Dynamic Pools",    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkVnGE9n"),
    "LIFINITY":  ("Lifinity AMM v1",          "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S"),
    "SABER":     ("Saber Stable Swap",        "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ"),
    "ALDRIN":    ("Aldrin AMM v2",            "AMM55ShdkoioZB5LzcqgGYBCpiHnSnbQMqrha1zfaHgR"),
    "OPENBOOK":  ("OpenBook v2",              "opnb2LAfJYbRMAHHvqjCwQxanZn7n7QM7qHbeFkurtm"),
    "SERUM":     ("Serum DEX v3",             "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"),
    "INVARIANT": ("Invariant Protocol",       "HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJuyz"),
    "CREMA":     ("Crema Finance CLMM",       "CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR"),
    "SANCTUM":   ("Sanctum Router",           "5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx"),
}


def _classify_dest(address: str) -> dict:
    info = classify_address(address)
    if info["is_cex"]:      nt = "CEX"
    elif info["is_bridge"]: nt = "BRIDGE"
    elif info["is_dex"]:    nt = "DEX_SWAP"
    elif info["is_defi"]:   nt = "DEFI"
    else:                   nt = "WALLET"
    return {
        "is_cex": info["is_cex"], "is_dex": info["is_dex"],
        "is_bridge": info["is_bridge"], "is_defi": info["is_defi"],
        "label": info.get("name") or "Wallet Destino",
        "node_type": nt, "risk": info.get("risk", "UNKNOWN"),
    }


class SolanaFetcher:
    def __init__(self):
        self.helius_key      = HELIUS_API_KEY
        self.use_helius      = bool(HELIUS_API_KEY)
        self.solscan_key     = SOLSCAN_API_KEY
        self.use_solscan_pro = bool(SOLSCAN_API_KEY)

    def _solscan_headers(self) -> dict:
        return {"token": self.solscan_key} if self.use_solscan_pro else {}

    # ─────────────────────────────────────────────────────────────────────────
    # DEX Detection
    # ─────────────────────────────────────────────────────────────────────────

    def _is_dex_swap_tx(self, tx: dict) -> tuple[bool, str, str]:
        src = (tx.get("source") or "").upper().strip()
        typ = (tx.get("type")   or "").upper().strip()
        if src in HELIUS_SOURCE_TO_DEX:
            name, pid = HELIUS_SOURCE_TO_DEX[src]
            return True, name, pid
        if typ == "SWAP":
            return True, "DEX Swap", ""
        for k in (tx.get("accountKeys") or []):
            addr = k if isinstance(k, str) else (k.get("pubkey") or "")
            found, dn = is_dex_program(addr)
            if found: return True, dn, addr
        return False, "", ""

    def _get_swap_output_token(self, tx: dict, wallet: str) -> tuple[Optional[str], Optional[float]]:
        wl = wallet.lower()
        sw = (tx.get("events") or {}).get("swap") or {}
        for out in (sw.get("tokenOutputs") or []):
            u = (out.get("userAccount") or out.get("account") or "").lower()
            if u == wl or not u:
                m = out.get("mint") or ""
                a = out.get("tokenAmount") or out.get("amount")
                if m: return m, float(a) if a else None
        no = sw.get("nativeOutput")
        if no:
            u = (no.get("account") or "").lower()
            if u == wl or not u:
                ra = no.get("amount", 0)
                return "SOL", ra / 1e9 if ra else None
        for t in tx.get("tokenTransfers", []):
            to = (t.get("toUserAccount") or "").lower()
            fr = (t.get("fromUserAccount") or "").lower()
            if to == wl and fr != wl:
                m = t.get("mint") or ""
                a = t.get("tokenAmount")
                if m: return m, float(a) if a else None
        for n in tx.get("nativeTransfers", []):
            to = (n.get("toUserAccount") or "").lower()
            fr = (n.get("fromUserAccount") or "").lower()
            if to == wl and fr != wl and n.get("amount", 0) > 5000:
                return "SOL", n["amount"] / 1e9
        return None, None

    def _sum_outgoing(self, wl: str, tx: dict, is_sol: bool) -> Optional[float]:
        total, found = 0.0, False
        for t in tx.get("nativeTransfers" if is_sol else "tokenTransfers", []):
            if (t.get("fromUserAccount") or "").lower() == wl:
                try:
                    v = float(t.get("amount") or 0) / 1e9 if is_sol else float(t.get("tokenAmount") or 0)
                    total += v; found = True
                except: pass
        return total if found else None

    # ─────────────────────────────────────────────────────────────────────────
    # Busca de transações
    # ─────────────────────────────────────────────────────────────────────────

    async def get_wallet_transactions(self, wallet: str, limit: int = 50) -> list[dict]:
        if self.use_helius:
            txs = await self._helius_get_transactions(wallet, limit)
            if txs: return txs
        txs = await self._solscan_get_wallet_transactions(wallet, limit)
        if txs: return txs
        return await self._rpc_get_transactions(wallet, limit)

    async def _helius_get_transactions(self, wallet: str, limit: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=30) as c:
            try:
                r = await c.get(f"{HELIUS_BASE_URL}/addresses/{wallet}/transactions",
                                params={"api-key": self.helius_key, "limit": min(limit, 100)})
                if r.status_code == 200: return r.json()
            except Exception as e: print(f"[Helius] {e}")
        return []

    async def _solscan_get_wallet_transactions(self, wallet: str, limit: int) -> list[dict]:
        if self.use_solscan_pro:
            url = f"{SOLSCAN_PRO_URL}/account/transactions"
            params = {"address": wallet, "page": 1, "page_size": min(limit, 100)}
        else:
            url = f"{SOLSCAN_BASE_URL}/account/transactions"
            params = {"account": wallet, "limit": min(limit, 50)}
        async with httpx.AsyncClient(timeout=30) as c:
            try:
                r = await c.get(url, params=params, headers=self._solscan_headers())
                if r.status_code == 200:
                    data = r.json()
                    items = data.get("data", data) if isinstance(data, dict) else data
                    if isinstance(items, list): return [self._parse_solscan_tx(t) for t in items if t]
            except Exception as e: print(f"[Solscan] {e}")
        return []

    async def _solscan_get_transaction(self, tx_hash: str) -> Optional[dict]:
        if self.use_solscan_pro:
            url = f"{SOLSCAN_PRO_URL}/transaction/detail"; params = {"tx": tx_hash}
        else:
            url = f"{SOLSCAN_BASE_URL}/transaction/{tx_hash}"; params = {}
        async with httpx.AsyncClient(timeout=30) as c:
            try:
                r = await c.get(url, params=params, headers=self._solscan_headers())
                if r.status_code == 200:
                    data = r.json()
                    raw = data.get("data", data) if isinstance(data, dict) and "data" in data else data
                    if raw and isinstance(raw, dict): return self._parse_solscan_tx(raw, tx_hash)
            except Exception as e: print(f"[Solscan TX] {e}")
        return None

    def _parse_solscan_tx(self, tx: dict, sig: str = "") -> dict:
        sig   = sig or tx.get("txHash") or tx.get("signature") or tx.get("tx") or ""
        ts    = tx.get("blockTime") or tx.get("block_time") or 0
        tts, nts = [], []
        for t in tx.get("tokenTransfers", []):
            src = t.get("sourceOwner") or t.get("source") or ""
            dst = t.get("destinationOwner") or t.get("destination") or ""
            ti  = t.get("token") or {}
            dec = int(ti.get("decimals") or t.get("decimals") or 0)
            raw = t.get("amount") or t.get("tokenAmount") or 0
            try: ui = float(raw) / (10 ** dec) if dec > 0 else float(raw)
            except: ui = 0.0
            mint = ti.get("tokenAddress") or ti.get("address") or t.get("mint") or ""
            if src and dst and ui > 0:
                tts.append({"fromUserAccount": src, "toUserAccount": dst, "mint": mint, "tokenAmount": ui})
        for s in tx.get("solTransfers", []):
            if s.get("source") and s.get("destination") and (s.get("amount") or 0) > 5000:
                nts.append({"fromUserAccount": s["source"], "toUserAccount": s["destination"], "amount": s["amount"]})
        parsed = {"signature": sig, "timestamp": ts, "type": tx.get("txType") or "TRANSFER",
                  "source": "SOLSCAN", "description": tx.get("memo") or "",
                  "tokenTransfers": tts, "nativeTransfers": nts,
                  "feePayer": (tx.get("signer") or [""])[0], "accountKeys": []}
        for instr in (tx.get("parsedInstruction") or tx.get("instructions") or []):
            pid = instr.get("programId") or instr.get("program") or ""
            found, dn = is_dex_program(pid)
            if found:
                parsed["type"] = "SWAP"; parsed["source"] = dn.split()[0].upper()
                parsed["accountKeys"] = [pid]; break
        return parsed

    async def _rpc_get_transactions(self, wallet: str, limit: int) -> list[dict]:
        sigs = await self._rpc_get_signatures(wallet, limit)
        txs = []
        async with httpx.AsyncClient(timeout=30) as c:
            for si in sigs[:20]:
                sig = si.get("signature")
                if not sig: continue
                try:
                    r = await c.post(SOLANA_RPC_URL, json={"jsonrpc":"2.0","id":1,"method":"getTransaction",
                        "params":[sig,{"encoding":"jsonParsed","maxSupportedTransactionVersion":0}]})
                    if r.status_code == 200:
                        res = r.json().get("result")
                        if res: txs.append(self._parse_rpc_transaction(res, sig))
                except: pass
        return txs

    async def _rpc_get_signatures(self, wallet: str, limit: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=20) as c:
            try:
                r = await c.post(SOLANA_RPC_URL, json={"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress","params":[wallet,{"limit":limit}]})
                if r.status_code == 200: return r.json().get("result", [])
            except: pass
        return []

    def _parse_rpc_transaction(self, tx_data: dict, signature: str) -> dict:
        meta = tx_data.get("meta", {}) or {}
        msg  = tx_data.get("transaction", {}).get("message", {})
        blt  = tx_data.get("blockTime", 0)
        aks  = [a.get("pubkey", "") for a in msg.get("accountKeys", [])]
        dex_name, dex_pid = "", ""
        for addr in aks:
            f, dn = is_dex_program(addr)
            if f: dex_name = dn; dex_pid = addr; break
        tts, nts = [], []
        from collections import defaultdict
        ptb = {b["accountIndex"]: b for b in (meta.get("preTokenBalances") or [])}
        pptb= {b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])}
        mo: dict = defaultdict(list); mi: dict = defaultdict(list)
        for idx in set(ptb) | set(pptb):
            pre = ptb.get(idx, {}); post = pptb.get(idx, {})
            pa  = float((pre.get("uiTokenAmount")  or {}).get("uiAmount") or 0)
            ppa = float((post.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            d   = ppa - pa
            if abs(d) < 1e-9: continue
            own  = post.get("owner") or pre.get("owner") or (aks[idx] if idx < len(aks) else "unknown")
            mint = post.get("mint") or pre.get("mint") or ""
            (mo if d < 0 else mi)[mint].append({"owner": own, "amount": abs(d)})
        for mint in set(mo) | set(mi):
            outs = mo.get(mint, []); ins = mi.get(mint, []); used: set = set()
            for out in outs:
                bi, bd = None, float("inf")
                for i, inc in enumerate(ins):
                    if i in used: continue
                    dv = abs(inc["amount"] - out["amount"])
                    if dv < bd: bd = dv; bi = i
                tow = ins[bi]["owner"] if bi is not None else ""
                if bi is not None: used.add(bi)
                tts.append({"fromUserAccount": out["owner"], "toUserAccount": tow, "mint": mint, "tokenAmount": out["amount"]})
            for i, inc in enumerate(ins):
                if i not in used: tts.append({"fromUserAccount": "", "toUserAccount": inc["owner"], "mint": mint, "tokenAmount": inc["amount"]})
        pb = meta.get("preBalances", []); ppb = meta.get("postBalances", [])
        ss, sr = [], []
        for i, (a, b) in enumerate(zip(pb, ppb)):
            d = b - a
            if abs(d) <= 5000 or i >= len(aks): continue
            (ss if d < 0 else sr).append({"acct": aks[i], "amount": abs(d)})
        ur: set = set()
        for s in ss:
            bi, bd = None, float("inf")
            for i, r in enumerate(sr):
                if i in ur: continue
                d = abs(r["amount"] - s["amount"])
                if d < bd: bd = d; bi = i
            ta = sr[bi]["acct"] if bi is not None else ""
            if bi is not None: ur.add(bi)
            nts.append({"fromUserAccount": s["acct"], "toUserAccount": ta, "amount": s["amount"]})
        return {
            "signature": signature, "timestamp": blt,
            "type":   "SWAP"       if dex_name else "TRANSFER",
            "source": dex_name.split()[0].upper() if dex_name else "SYSTEM_PROGRAM",
            "description": "", "tokenTransfers": tts, "nativeTransfers": nts,
            "feePayer": aks[0] if aks else "", "accountKeys": aks,
            "_dex_name": dex_name, "_dex_pid": dex_pid,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Rastreamento — segue até os fundos pararem
    # ─────────────────────────────────────────────────────────────────────────

    async def trace_token_flow(self, wallet: str, token: str, amount: float,
                               transactions: list[dict], max_hops: int = 20,
                               tx_hash: Optional[str] = None) -> dict:
        """
        Rastreia fundos até pararem de se mover.
        Para em: CEX, Bridge, DEX (depois continua com token de saída),
                 carteira PARKED (sem saídas), ou limites de segurança.
        Detecta: splits (1→N), fundos estacionados, padrões de lavagem.
        """
        graph = {
            "nodes": [{"id": wallet, "label": "Carteira Hackeada (Vítima)", "type": "VICTIM",
                       "is_cex": False, "is_dex": False, "is_bridge": False,
                       "is_defi": False, "is_parked": False, "depth": 0}],
            "edges": [], "cex_detected": [], "bridge_detected": [],
            "dex_detected": [], "parked_wallets": [], "summary": {},
        }

        token_lower = token.lower().strip()
        # visited: "wallet:mint" — permite re-visitar com token diferente pós-swap
        visited: set[str] = {f"{wallet}:{token_lower}"}

        # ── HOP 0→1 ──────────────────────────────────────────────────────────
        if tx_hash:
            print(f"[Fetcher] TX Hash: {tx_hash}")
            tx_transfers = await self._transfers_from_tx_hash(wallet, token_lower, tx_hash)
            if not tx_transfers:
                print(f"[Fetcher] ERRO: TX não encontrada.")
                outgoing = []
            elif len(tx_transfers) == 1:
                outgoing = tx_transfers
            else:
                outgoing = self._pick_best_from_list(tx_transfers, amount)
        else:
            outgoing = self._best_match_transfers(wallet, token_lower, transactions, amount)

        actual_mint = next(
            (t["mint"].lower() for t in outgoing if t.get("mint") and t["mint"].lower() != "sol"),
            token_lower,
        )
        print(f"[Fetcher] Mint inicial: {actual_mint}")

        # BFS: (wallet, received_amount, received_ts, depth, current_mint)
        bfs_queue: list[tuple[str, object, int, int, str]] = []

        for tx in outgoing:
            dest = tx.get("to")
            if not dest or dest == wallet: continue
            cls = await self._classify_and_enrich(dest)
            self._update_node(graph, dest, cls, 1)
            self._add_edge(graph, wallet, dest, tx)
            self._update_detections(graph, cls)
            vkey = f"{dest}:{actual_mint}"
            if not self._is_terminal(cls) and vkey not in visited:
                visited.add(vkey)
                bfs_queue.append((dest, tx.get("amount"), tx.get("timestamp") or 0, 1, actual_mint))

        # ── BFS principal ─────────────────────────────────────────────────────
        total_processed = 0
        while bfs_queue:
            hop_wallet, recv_amount, recv_ts, depth, current_mint = bfs_queue.pop(0)

            # Limites de segurança
            if total_processed >= MAX_BFS_WALLETS:
                print(f"[Fetcher] Limite de segurança: {MAX_BFS_WALLETS} carteiras processadas.")
                self._mark_node_truncated(graph, hop_wallet)
                continue
            if depth >= MAX_BFS_DEPTH:
                print(f"[Fetcher] Profundidade máxima atingida: {hop_wallet[:20]}...")
                continue

            total_processed += 1
            print(f"[Fetcher] BFS {depth}→{depth+1} [{total_processed}/{MAX_BFS_WALLETS}]: {hop_wallet[:20]}... (mint={current_mint[:15]}...)")

            hop_txs = await self.get_wallet_transactions(hop_wallet, limit=30)
            hop_out = self._extract_outgoing_transfers(
                hop_wallet, current_mint, hop_txs,
                min_timestamp=recv_ts, received_amount=recv_amount,
            )

            # ── CARTEIRA PARKED: recebeu mas não moveu ────────────────────────
            if not hop_out:
                self._mark_parked(graph, hop_wallet, recv_ts)
                if hop_wallet not in graph["parked_wallets"]:
                    graph["parked_wallets"].append(hop_wallet)
                print(f"[Fetcher] 🅿️ PARKED: {hop_wallet[:20]}... (sem saídas após receber)")
                continue

            # ── SPLIT: 1 carteira → N destinos ──────────────────────────────
            if len(hop_out) > 1:
                split_count = len([t for t in hop_out if t.get("transfer_type") != "DEX_SWAP"])
                if split_count > 1:
                    self._mark_split(graph, hop_wallet, split_count)
                    print(f"[Fetcher] ⚡ SPLIT: {hop_wallet[:20]}... → {split_count} destinos")

            # ── Processa saídas (até MAX_OUTGOING_NODE) ──────────────────────
            for tx in hop_out[:MAX_OUTGOING_NODE]:
                dest   = tx.get("to")
                t_type = tx.get("transfer_type", "TRANSFER")

                # DEX SWAP
                if t_type == "DEX_SWAP":
                    dex_name = tx.get("dex_name", "DEX Swap")
                    node_id  = tx.get("dex_pid") or dex_name
                    dex_cls  = {
                        "is_cex": False, "is_dex": True, "is_bridge": False,
                        "is_defi": False, "is_parked": False,
                        "label": dex_name, "node_type": "DEX_SWAP", "risk": "LOW RISK",
                    }
                    self._update_node(graph, node_id, dex_cls, depth + 1)
                    self._add_edge(graph, hop_wallet, node_id, tx)
                    if dex_name not in graph["dex_detected"]:
                        graph["dex_detected"].append(dex_name)
                    print(f"[Fetcher] 🔄 SWAP: {hop_wallet[:20]}... → {dex_name}")

                    # Continua com token de saída
                    output_mint, output_amount = tx.get("output_mint"), tx.get("output_amount")
                    if output_mint:
                        out_mint_lower = output_mint.lower()
                        vkey = f"{hop_wallet}:{out_mint_lower}"
                        if vkey not in visited and depth + 1 < MAX_BFS_DEPTH:
                            visited.add(vkey)
                            print(f"[Fetcher] 🔄 Pós-swap: {hop_wallet[:20]}... → {out_mint_lower[:20]}...")
                            bfs_queue.insert(0, (hop_wallet, output_amount, tx.get("timestamp") or 0, depth + 1, out_mint_lower))
                    continue

                # Transfer normal
                if not dest: continue
                vkey = f"{dest}:{current_mint}"
                if vkey in visited: continue

                cls = await self._classify_and_enrich(dest)
                self._update_node(graph, dest, cls, depth + 1)
                self._add_edge(graph, hop_wallet, dest, tx)
                self._update_detections(graph, cls)

                if not self._is_terminal(cls):
                    visited.add(vkey)
                    bfs_queue.append((dest, tx.get("amount"), tx.get("timestamp") or 0, depth + 1, current_mint))

        # ── Summary ───────────────────────────────────────────────────────────
        parked_nodes = [n for n in graph["nodes"] if n.get("is_parked")]
        split_nodes  = [n for n in graph["nodes"] if n.get("is_split")]

        graph["summary"] = {
            "total_nodes":        len(graph["nodes"]),
            "total_edges":        len(graph["edges"]),
            "max_depth":          max((n["depth"] for n in graph["nodes"]), default=0),
            "cex_found":          len(graph["cex_detected"]) > 0,
            "bridge_used":        len(graph["bridge_detected"]) > 0,
            "dex_used":           len(graph["dex_detected"]) > 0,
            "parked_count":       len(parked_nodes),
            "split_count":        len(split_nodes),
            "outgoing_transfers": len(outgoing),
            "wallets_processed":  total_processed,
            "truncated":          total_processed >= MAX_BFS_WALLETS,
        }

        print(f"[Fetcher] Rastreamento concluído: {len(graph['nodes'])} nós | "
              f"{len(parked_nodes)} parked | {len(split_nodes)} splits | "
              f"CEX={graph['cex_detected']} DEX={graph['dex_detected']}")

        return graph

    # ─────────────────────────────────────────────────────────────────────────
    # Node state helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _mark_parked(self, graph: dict, wallet: str, recv_ts: int):
        for n in graph["nodes"]:
            if n["id"] == wallet:
                n["type"]      = "PARKED"
                n["is_parked"] = True
                n["label"]     = f"Carteira Estacionada"
                n["recv_ts"]   = recv_ts
                break

    def _mark_split(self, graph: dict, wallet: str, split_count: int):
        for n in graph["nodes"]:
            if n["id"] == wallet:
                n["is_split"]    = True
                n["split_count"] = split_count
                if n.get("type") == "WALLET":
                    n["type"]  = "SPLIT"
                    n["label"] = f"Carteira Split ({split_count} destinos)"
                break

    def _mark_node_truncated(self, graph: dict, wallet: str):
        for n in graph["nodes"]:
            if n["id"] == wallet:
                n["truncated"] = True
                n["label"]    += " [rastreamento interrompido]"
                break

    # ─────────────────────────────────────────────────────────────────────────
    # Classification helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _classify_and_enrich(self, dest: str) -> dict:
        cls = _classify_dest(dest)
        if not self._is_terminal(cls) and self.use_helius:
            lbl = await self._helius_get_entity_label(dest)
            if lbl:
                ok, name = detect_cex_from_label(lbl)
                if ok: cls.update({"is_cex": True, "label": name, "node_type": "CEX"})
        return cls

    def _is_terminal(self, cls: dict) -> bool:
        return cls["is_cex"] or cls["is_bridge"] or cls["is_dex"] or cls["is_defi"]

    def _update_node(self, graph: dict, node_id: str, cls: dict, depth: int):
        if node_id not in [n["id"] for n in graph["nodes"]]:
            graph["nodes"].append({
                "id": node_id, "label": cls["label"], "type": cls["node_type"],
                "is_cex": cls["is_cex"], "is_dex": cls["is_dex"],
                "is_bridge": cls["is_bridge"], "is_defi": cls["is_defi"],
                "is_parked": False, "is_split": False, "depth": depth,
            })

    def _add_edge(self, graph: dict, frm: str, to: str, tx: dict):
        graph["edges"].append({
            "from": frm, "to": to,
            "amount": tx.get("amount"), "mint": tx.get("mint"),
            "timestamp": tx.get("timestamp"),
            "timestamp_human": self._ts_to_human(tx.get("timestamp")),
            "signature": tx.get("signature"),
            "transfer_type": tx.get("transfer_type", "TRANSFER"),
            "dex_name": tx.get("dex_name"),
            "output_mint": tx.get("output_mint"),
            "output_amount": tx.get("output_amount"),
        })

    def _update_detections(self, graph: dict, cls: dict):
        lbl = cls["label"]
        if cls["is_cex"]    and lbl not in graph["cex_detected"]:    graph["cex_detected"].append(lbl)
        if cls["is_bridge"] and lbl not in graph["bridge_detected"]: graph["bridge_detected"].append(lbl)
        if cls["is_dex"]    and lbl not in graph["dex_detected"]:    graph["dex_detected"].append(lbl)

    # ─────────────────────────────────────────────────────────────────────────
    # Helius entity label
    # ─────────────────────────────────────────────────────────────────────────

    async def _helius_get_entity_label(self, address: str) -> str:
        if not self.use_helius: return ""
        async with httpx.AsyncClient(timeout=15) as c:
            try:
                r = await c.get(f"{HELIUS_BASE_URL}/addresses/{address}/transactions",
                                params={"api-key": self.helius_key, "limit": 5})
                if r.status_code != 200: return ""
                txs = r.json()
                if not txs or not isinstance(txs, list): return ""
                for tx in txs:
                    src = (tx.get("source") or "").strip()
                    if src and src not in ("SYSTEM_PROGRAM", "UNKNOWN", ""):
                        ok, name = detect_cex_from_label(src)
                        if ok: return name
                    desc = (tx.get("description") or "").lower()
                    for pat, name in CEX_NAME_PATTERNS.items():
                        if pat in desc: return name
                    for ad in (tx.get("accountData") or []):
                        if (ad.get("account") or "").lower() == address.lower():
                            lbl = ad.get("label") or ad.get("entity") or ""
                            if lbl: return lbl
            except Exception as e: print(f"[Helius Entity] {e}")
        return ""

    # ─────────────────────────────────────────────────────────────────────────
    # TX fetching
    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch_tx_data(self, tx_hash: str) -> Optional[dict]:
        if self.use_helius:
            async with httpx.AsyncClient(timeout=30) as c:
                try:
                    r = await c.post(f"{HELIUS_BASE_URL}/transactions",
                                     params={"api-key": self.helius_key},
                                     json={"transactions": [tx_hash]})
                    if r.status_code == 200:
                        res = r.json()
                        if res and isinstance(res, list) and res[0]: return res[0]
                except Exception as e: print(f"[Helius TX] {e}")
        tx = await self._solscan_get_transaction(tx_hash)
        if tx: return tx
        async with httpx.AsyncClient(timeout=30) as c:
            try:
                r = await c.post(SOLANA_RPC_URL, json={"jsonrpc":"2.0","id":1,"method":"getTransaction",
                    "params":[tx_hash,{"encoding":"jsonParsed","maxSupportedTransactionVersion":0}]})
                if r.status_code == 200:
                    res = r.json().get("result")
                    if res: return self._parse_rpc_transaction(res, tx_hash)
            except Exception as e: print(f"[RPC TX] {e}")
        return None

    async def _transfers_from_tx_hash(self, wallet: str, token_lower: str, tx_hash: str) -> list[dict]:
        tx = await self._fetch_tx_data(tx_hash)
        if not tx: return []
        is_sol = token_lower in SOL_SYMBOLS
        ts = tx.get("timestamp", 0)
        wl = wallet.lower()
        strict: list[dict] = []
        if not is_sol:
            for t in tx.get("tokenTransfers", []):
                if (t.get("fromUserAccount") or "").lower() == wl and t.get("toUserAccount"):
                    strict.append({"type":"token","from":wallet,"to":t["toUserAccount"],
                                   "amount":t.get("tokenAmount"),"mint":t.get("mint"),"signature":tx_hash,"timestamp":ts})
        else:
            for n in tx.get("nativeTransfers", []):
                if (n.get("fromUserAccount") or "").lower() == wl and n.get("toUserAccount") and n.get("amount",0)>0:
                    strict.append({"type":"native","from":wallet,"to":n["toUserAccount"],
                                   "amount":n["amount"]/1e9,"mint":"SOL","signature":tx_hash,"timestamp":ts})
        if strict: return strict
        broad: list[dict] = []
        if not is_sol:
            for t in tx.get("tokenTransfers", []):
                if t.get("toUserAccount") and t.get("tokenAmount"):
                    broad.append({"type":"token","from":t.get("fromUserAccount") or wallet,"to":t["toUserAccount"],
                                  "amount":t["tokenAmount"],"mint":t.get("mint"),"signature":tx_hash,"timestamp":ts})
        else:
            for n in tx.get("nativeTransfers", []):
                if n.get("toUserAccount") and n.get("amount",0)>0:
                    broad.append({"type":"native","from":n.get("fromUserAccount") or wallet,"to":n["toUserAccount"],
                                  "amount":n["amount"]/1e9,"mint":"SOL","signature":tx_hash,"timestamp":ts})
        return broad

    def _pick_best_from_list(self, transfers: list[dict], target: float) -> list[dict]:
        def pd(t):
            try: return abs(float(t.get("amount") or 0) - target) / target
            except: return float("inf")
        ranked = sorted(transfers, key=pd)
        tight  = [t for t in ranked if pd(t) <= 0.30]
        return tight if tight else ([ranked[0]] if ranked else [])

    def _best_match_transfers(self, wallet: str, token_lower: str,
                              transactions: list[dict], target: float) -> list[dict]:
        all_t = self._extract_outgoing_transfers(wallet, token_lower, transactions)
        return self._pick_best_from_list(all_t, target) if all_t else []

    def _extract_outgoing_transfers(self, wallet: str, token_lower: str,
                                    transactions: list[dict], min_timestamp: int = 0,
                                    received_amount: Optional[float] = None,
                                    max_amount_ratio: float = 10.0) -> list[dict]:
        transfers = []
        wl     = wallet.lower()
        is_sol = token_lower in SOL_SYMBOLS

        for tx in transactions:
            sig = tx.get("signature", "")
            ts  = tx.get("timestamp", 0) or 0
            if min_timestamp and ts and ts < min_timestamp: continue

            # DEX swap detection
            is_swap, dex_name, dex_pid = self._is_dex_swap_tx(tx)
            if is_swap:
                is_sender = (
                    any((t.get("fromUserAccount") or "").lower() == wl for t in tx.get("tokenTransfers", []))
                    or any((n.get("fromUserAccount") or "").lower() == wl for n in tx.get("nativeTransfers", []))
                )
                if is_sender:
                    swap_amt = self._sum_outgoing(wl, tx, is_sol)
                    if received_amount and swap_amt and swap_amt > float(received_amount) * max_amount_ratio: continue
                    out_mint, out_amt = self._get_swap_output_token(tx, wallet)
                    transfers.append({
                        "transfer_type": "DEX_SWAP", "type": "dex_swap",
                        "from": wallet, "to": dex_pid or dex_name,
                        "dex_name": dex_name, "dex_pid": dex_pid,
                        "amount": swap_amt, "mint": token_lower,
                        "output_mint": out_mint, "output_amount": out_amt,
                        "signature": sig, "timestamp": ts,
                    })
                    continue

            if not is_sol:
                for t in tx.get("tokenTransfers", []):
                    fr = (t.get("fromUserAccount") or "").lower()
                    mn = (t.get("mint") or "").lower()
                    to = t.get("toUserAccount") or ""
                    mm = (mn == token_lower) or (not self._looks_like_address(token_lower))
                    if not (fr == wl and to and mm): continue
                    try: af = float(t.get("tokenAmount") or 0)
                    except: af = None
                    if received_amount and af and af > float(received_amount) * max_amount_ratio: continue
                    transfers.append({"transfer_type":"TRANSFER","type":"token","from":wallet,"to":to,
                                      "amount":t.get("tokenAmount"),"mint":t.get("mint"),"signature":sig,"timestamp":ts})

            if is_sol:
                for n in tx.get("nativeTransfers", []):
                    fr = (n.get("fromUserAccount") or "").lower()
                    to = n.get("toUserAccount") or ""
                    lm = n.get("amount", 0)
                    if not (fr == wl and to and lm > 0): continue
                    sol = lm / 1e9
                    if received_amount and sol > float(received_amount) * max_amount_ratio: continue
                    transfers.append({"transfer_type":"TRANSFER","type":"native","from":wallet,"to":to,
                                      "amount":sol,"mint":"SOL","signature":sig,"timestamp":ts})

        return transfers

    @staticmethod
    def _looks_like_address(s: str) -> bool: return 32 <= len(s) <= 50

    @staticmethod
    def _ts_to_human(ts: Optional[int]) -> str:
        if not ts: return "desconhecido"
        try: return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
        except: return str(ts)
