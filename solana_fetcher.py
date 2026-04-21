"""
SolTrace - Módulo de coleta de dados on-chain da Solana.
Fontes de dados em ordem de prioridade:
  1. Helius Enhanced Transactions API (dados mais ricos, requer API key)
  2. Solscan Public API (dados parseados, gratuito, sem key necessária)
  3. Solana RPC público (dados brutos, sempre disponível)
"""

import httpx
import os
from typing import Optional
from datetime import datetime

from cex_database import (
    CEX_ADDRESSES, BRIDGE_PROGRAMS, DEFI_PROGRAMS, CEX_NAME_PATTERNS,
    ALL_DEX_PROGRAMS, ALL_KNOWN_PROGRAMS,
    is_cex_address, is_dex_program, is_bridge_program,
    detect_cex_from_label, get_entity_info, classify_address,
)

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
HELIUS_BASE_URL = "https://api.helius.xyz/v0"

SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY", "")
SOLSCAN_BASE_URL = "https://public-api.solscan.io"
SOLSCAN_PRO_URL = "https://pro-api.solscan.io/v2.0"

SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"

SOL_MINT = "So11111111111111111111111111111111111111112"
SOL_SYMBOLS = {"sol", "solana", SOL_MINT.lower()}

# Helius `source` field -> (DEX display name, program ID)
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
    if info["is_cex"]:    node_type = "CEX"
    elif info["is_bridge"]: node_type = "BRIDGE"
    elif info["is_dex"]:  node_type = "DEX_SWAP"
    elif info["is_defi"]: node_type = "DEFI"
    else:                 node_type = "WALLET"
    return {
        "is_cex":    info["is_cex"],
        "is_dex":    info["is_dex"],
        "is_bridge": info["is_bridge"],
        "is_defi":   info["is_defi"],
        "label":     info.get("name") or "Wallet Destino",
        "node_type": node_type,
        "risk":      info.get("risk", "UNKNOWN"),
    }


class SolanaFetcher:
    def __init__(self):
        self.helius_key    = HELIUS_API_KEY
        self.use_helius    = bool(HELIUS_API_KEY)
        self.solscan_key   = SOLSCAN_API_KEY
        self.use_solscan_pro = bool(SOLSCAN_API_KEY)

    def _solscan_headers(self) -> dict:
        return {"token": self.solscan_key} if self.use_solscan_pro else {}

    # ─────────────────────────────────────────────────────────────────────────
    # DEX Swap Detection
    # ─────────────────────────────────────────────────────────────────────────

    def _is_dex_swap_tx(self, tx: dict) -> tuple[bool, str, str]:
        """
        Detecta se uma transação é um swap em DEX.
        Retorna (is_swap, dex_name, dex_program_id).

        Camada 1: Helius type="SWAP" + source field
        Camada 2: source field sozinho (ex: "JUPITER")
        Camada 3: accountKeys contém program ID de DEX (fallback RPC/Solscan)
        """
        tx_type   = (tx.get("type")   or "").upper().strip()
        tx_source = (tx.get("source") or "").upper().strip()

        if tx_source in HELIUS_SOURCE_TO_DEX:
            name, pid = HELIUS_SOURCE_TO_DEX[tx_source]
            return True, name, pid

        if tx_type == "SWAP":
            return True, "DEX Swap", ""

        # Camada 3: accountKeys
        for key_entry in (tx.get("accountKeys") or []):
            addr = key_entry if isinstance(key_entry, str) else (key_entry.get("pubkey") or "")
            found, dex_name = is_dex_program(addr)
            if found:
                return True, dex_name, addr

        return False, "", ""

    def _sum_outgoing(self, wallet_lower: str, tx: dict, is_sol: bool) -> Optional[float]:
        """Soma o valor total saindo do wallet em uma TX (para swaps)."""
        total = 0.0
        found = False
        if not is_sol:
            for t in tx.get("tokenTransfers", []):
                if (t.get("fromUserAccount") or "").lower() == wallet_lower:
                    try:
                        total += float(t.get("tokenAmount") or 0)
                        found = True
                    except (TypeError, ValueError):
                        pass
        else:
            for n in tx.get("nativeTransfers", []):
                if (n.get("fromUserAccount") or "").lower() == wallet_lower:
                    try:
                        total += float(n.get("amount") or 0) / 1e9
                        found = True
                    except (TypeError, ValueError):
                        pass
        return total if found else None

    # ─────────────────────────────────────────────────────────────────────────
    # Busca de transações (Helius -> Solscan -> RPC)
    # ─────────────────────────────────────────────────────────────────────────

    async def get_wallet_transactions(self, wallet: str, limit: int = 50) -> list[dict]:
        if self.use_helius:
            txs = await self._helius_get_transactions(wallet, limit)
            if txs:
                return txs
            print("[Fetcher] Helius falhou, tentando Solscan...")
        txs = await self._solscan_get_wallet_transactions(wallet, limit)
        if txs:
            return txs
        print("[Fetcher] Solscan falhou, usando RPC público...")
        return await self._rpc_get_transactions(wallet, limit)

    async def _helius_get_transactions(self, wallet: str, limit: int) -> list[dict]:
        url = f"{HELIUS_BASE_URL}/addresses/{wallet}/transactions"
        params = {"api-key": self.helius_key, "limit": min(limit, 100)}
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    return resp.json()
                print(f"[Helius] HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                print(f"[Helius] Erro: {e}")
        return []

    async def _solscan_get_wallet_transactions(self, wallet: str, limit: int) -> list[dict]:
        if self.use_solscan_pro:
            url = f"{SOLSCAN_PRO_URL}/account/transactions"
            params = {"address": wallet, "page": 1, "page_size": min(limit, 100)}
        else:
            url = f"{SOLSCAN_BASE_URL}/account/transactions"
            params = {"account": wallet, "limit": min(limit, 50)}

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, params=params, headers=self._solscan_headers())
                if resp.status_code == 200:
                    data  = resp.json()
                    items = data.get("data", data) if isinstance(data, dict) else data
                    if isinstance(items, list):
                        return [self._parse_solscan_tx(tx) for tx in items if tx]
                print(f"[Solscan] HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                print(f"[Solscan] Erro wallet TXs: {e}")
        return []

    async def _solscan_get_transaction(self, tx_hash: str) -> Optional[dict]:
        if self.use_solscan_pro:
            url    = f"{SOLSCAN_PRO_URL}/transaction/detail"
            params = {"tx": tx_hash}
        else:
            url    = f"{SOLSCAN_BASE_URL}/transaction/{tx_hash}"
            params = {}

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, params=params, headers=self._solscan_headers())
                if resp.status_code == 200:
                    data = resp.json()
                    raw  = data.get("data", data) if isinstance(data, dict) and "data" in data else data
                    if raw and isinstance(raw, dict):
                        return self._parse_solscan_tx(raw, tx_hash)
                print(f"[Solscan] TX HTTP {resp.status_code}")
            except Exception as e:
                print(f"[Solscan] Erro TX {tx_hash[:20]}: {e}")
        return None

    def _parse_solscan_tx(self, tx: dict, override_sig: str = "") -> dict:
        sig = override_sig or tx.get("txHash") or tx.get("signature") or tx.get("tx") or ""
        ts  = tx.get("blockTime") or tx.get("block_time") or 0
        token_transfers  = []
        native_transfers = []

        for t in tx.get("tokenTransfers", []):
            source  = t.get("sourceOwner") or t.get("source") or ""
            dest    = t.get("destinationOwner") or t.get("destination") or ""
            ti      = t.get("token") or {}
            decimals = int(ti.get("decimals") or t.get("decimals") or 0)
            raw_amt  = t.get("amount") or t.get("tokenAmount") or 0
            try:
                ui_amt = float(raw_amt) / (10 ** decimals) if decimals > 0 else float(raw_amt)
            except:
                ui_amt = 0.0
            mint = ti.get("tokenAddress") or ti.get("address") or t.get("mint") or ""
            if source and dest and ui_amt > 0:
                token_transfers.append({"fromUserAccount": source, "toUserAccount": dest, "mint": mint, "tokenAmount": ui_amt})

        for s in tx.get("solTransfers", []):
            source   = s.get("source") or ""
            dest     = s.get("destination") or ""
            lamports = s.get("amount") or 0
            if source and dest and lamports > 5000:
                native_transfers.append({"fromUserAccount": source, "toUserAccount": dest, "amount": lamports})

        signers   = tx.get("signer") or []
        fee_payer = signers[0] if signers else ""

        parsed = {
            "signature": sig, "timestamp": ts,
            "type": tx.get("txType") or "TRANSFER",
            "source": "SOLSCAN", "description": tx.get("memo") or "",
            "tokenTransfers": token_transfers, "nativeTransfers": native_transfers,
            "feePayer": fee_payer, "accountKeys": [],
        }

        # Detecta DEX via programId nas instruções do Solscan
        for instr in (tx.get("parsedInstruction") or tx.get("instructions") or []):
            pid = instr.get("programId") or instr.get("program") or ""
            found, dex_name = is_dex_program(pid)
            if found:
                parsed["type"]   = "SWAP"
                parsed["source"] = dex_name.split()[0].upper()
                parsed["accountKeys"] = [pid]
                break

        return parsed

    # ─────────────────────────────────────────────────────────────────────────
    # RPC público (fallback)
    # ─────────────────────────────────────────────────────────────────────────

    async def _rpc_get_transactions(self, wallet: str, limit: int) -> list[dict]:
        signatures   = await self._rpc_get_signatures(wallet, limit)
        transactions = []
        async with httpx.AsyncClient(timeout=30) as client:
            for sig_info in signatures[:20]:
                sig = sig_info.get("signature")
                if not sig:
                    continue
                try:
                    payload = {"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                               "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]}
                    resp = await client.post(SOLANA_RPC_URL, json=payload)
                    if resp.status_code == 200:
                        result = resp.json().get("result")
                        if result:
                            transactions.append(self._parse_rpc_transaction(result, sig))
                except Exception as e:
                    print(f"[RPC] Erro tx {sig[:20]}: {e}")
        return transactions

    async def _rpc_get_signatures(self, wallet: str, limit: int) -> list[dict]:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [wallet, {"limit": limit}]}
        async with httpx.AsyncClient(timeout=20) as client:
            try:
                resp = await client.post(SOLANA_RPC_URL, json=payload)
                if resp.status_code == 200:
                    return resp.json().get("result", [])
            except Exception as e:
                print(f"[RPC] Erro getSignaturesForAddress: {e}")
        return []

    def _parse_rpc_transaction(self, tx_data: dict, signature: str) -> dict:
        meta        = tx_data.get("meta", {}) or {}
        message     = tx_data.get("transaction", {}).get("message", {})
        block_time  = tx_data.get("blockTime", 0)
        account_keys = [a.get("pubkey", "") for a in message.get("accountKeys", [])]

        # Detecta DEX via accountKeys
        detected_dex_name = ""
        detected_dex_pid  = ""
        for addr in account_keys:
            found, dex_name = is_dex_program(addr)
            if found:
                detected_dex_name = dex_name
                detected_dex_pid  = addr
                print(f"[RPC] DEX via accountKeys: {dex_name}")
                break

        token_transfers  = []
        native_transfers = []
        from collections import defaultdict
        pre_tb  = {b["accountIndex"]: b for b in (meta.get("preTokenBalances")  or [])}
        post_tb = {b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])}
        mint_out: dict = defaultdict(list)
        mint_in:  dict = defaultdict(list)

        for idx in set(pre_tb) | set(post_tb):
            pre  = pre_tb.get(idx, {})
            post = post_tb.get(idx, {})
            pre_a  = float((pre.get("uiTokenAmount")  or {}).get("uiAmount") or 0)
            post_a = float((post.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            diff   = post_a - pre_a
            if abs(diff) < 1e-9:
                continue
            owner = post.get("owner") or pre.get("owner") or (account_keys[idx] if idx < len(account_keys) else "unknown")
            mint  = post.get("mint") or pre.get("mint") or ""
            (mint_out if diff < 0 else mint_in)[mint].append({"owner": owner, "amount": abs(diff)})

        for mint in set(mint_out) | set(mint_in):
            outs = mint_out.get(mint, [])
            ins  = mint_in.get(mint,  [])
            used: set = set()
            for out in outs:
                best_i, best_d = None, float("inf")
                for i, inc in enumerate(ins):
                    if i in used: continue
                    d = abs(inc["amount"] - out["amount"])
                    if d < best_d: best_d = d; best_i = i
                to_owner = ins[best_i]["owner"] if best_i is not None else ""
                if best_i is not None: used.add(best_i)
                token_transfers.append({"fromUserAccount": out["owner"], "toUserAccount": to_owner, "mint": mint, "tokenAmount": out["amount"]})
            for i, inc in enumerate(ins):
                if i not in used:
                    token_transfers.append({"fromUserAccount": "", "toUserAccount": inc["owner"], "mint": mint, "tokenAmount": inc["amount"]})

        pre_b  = meta.get("preBalances",  [])
        post_b = meta.get("postBalances", [])
        sol_s, sol_r = [], []
        for i, (pb, ppb) in enumerate(zip(pre_b, post_b)):
            diff = ppb - pb
            if abs(diff) <= 5000 or i >= len(account_keys): continue
            (sol_s if diff < 0 else sol_r).append({"acct": account_keys[i], "amount": abs(diff)})
        used_r: set = set()
        for sender in sol_s:
            best_i, best_d = None, float("inf")
            for i, recv in enumerate(sol_r):
                if i in used_r: continue
                d = abs(recv["amount"] - sender["amount"])
                if d < best_d: best_d = d; best_i = i
            to_acct = sol_r[best_i]["acct"] if best_i is not None else ""
            if best_i is not None: used_r.add(best_i)
            native_transfers.append({"fromUserAccount": sender["acct"], "toUserAccount": to_acct, "amount": sender["amount"]})

        return {
            "signature": signature, "timestamp": block_time,
            "type":   "SWAP"         if detected_dex_name else "TRANSFER",
            "source": detected_dex_name.split()[0].upper() if detected_dex_name else "SYSTEM_PROGRAM",
            "description": "", "tokenTransfers": token_transfers, "nativeTransfers": native_transfers,
            "feePayer": account_keys[0] if account_keys else "",
            "accountKeys": account_keys,
            "_dex_name": detected_dex_name, "_dex_pid": detected_dex_pid,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Rastreamento de fluxo de fundos
    # ─────────────────────────────────────────────────────────────────────────

    async def trace_token_flow(self, wallet: str, token: str, amount: float,
                               transactions: list[dict], max_hops: int = 2,
                               tx_hash: Optional[str] = None) -> dict:
        graph = {
            "nodes": [{"id": wallet, "label": "Carteira Hackeada (Vítima)", "type": "VICTIM",
                       "is_cex": False, "is_dex": False, "is_bridge": False, "is_defi": False, "depth": 0}],
            "edges": [], "cex_detected": [], "bridge_detected": [], "dex_detected": [], "summary": {},
        }
        token_lower = token.lower().strip()
        visited: set[str] = {wallet}

        # HOP 0->1
        if tx_hash:
            print(f"[Fetcher] Modo TX Hash: {tx_hash}")
            tx_transfers = await self._transfers_from_tx_hash(wallet, token_lower, tx_hash)
            if not tx_transfers:
                print(f"[Fetcher] ERRO: TX {tx_hash[:30]}... não encontrada.")
                outgoing = []
            elif len(tx_transfers) == 1:
                outgoing = tx_transfers
            else:
                outgoing = self._pick_best_from_list(tx_transfers, amount)
        else:
            outgoing = self._best_match_transfers(wallet, token_lower, transactions, amount)

        actual_mint = next(
            (tx["mint"].lower() for tx in outgoing if tx.get("mint") and tx["mint"].lower() != "sol"),
            token_lower,
        )
        print(f"[Fetcher] Mint HOP 0->1: {actual_mint}")

        bfs_queue: list[tuple[str, object, int, int]] = []

        for tx in outgoing:
            dest = tx.get("to")
            if not dest or dest == wallet:
                continue
            classification = _classify_dest(dest)
            if not classification["is_cex"] and not classification["is_dex"] and not classification["is_bridge"] and self.use_helius:
                lbl = await self._helius_get_entity_label(dest)
                if lbl:
                    ok, name = detect_cex_from_label(lbl)
                    if ok:
                        classification.update({"is_cex": True, "label": name, "node_type": "CEX"})
            self._update_node(graph, dest, classification, 1)
            self._add_edge(graph, wallet, dest, tx)
            self._update_detections(graph, classification)
            if not (classification["is_cex"] or classification["is_bridge"] or classification["is_dex"] or classification["is_defi"]) and dest not in visited:
                bfs_queue.append((dest, tx.get("amount"), tx.get("timestamp") or 0, 1))

        # BFS
        while bfs_queue:
            hop_wallet, recv_amount, recv_ts, depth = bfs_queue.pop(0)
            if depth >= max_hops or hop_wallet in visited:
                continue
            visited.add(hop_wallet)
            print(f"[Fetcher] BFS {depth}→{depth+1}: {hop_wallet[:20]}...")

            hop_txs = await self.get_wallet_transactions(hop_wallet, limit=20)
            hop_out = self._extract_outgoing_transfers(hop_wallet, actual_mint, hop_txs,
                                                       min_timestamp=recv_ts, received_amount=recv_amount)

            for tx in hop_out[:5]:
                dest = tx.get("to")
                if not dest or dest in visited:
                    continue

                # Trata DEX_SWAP sintético
                if tx.get("transfer_type") == "DEX_SWAP":
                    dex_name = tx.get("dex_name", "DEX Swap")
                    node_id  = tx.get("dex_pid") or dex_name
                    cls = {"is_cex": False, "is_dex": True, "is_bridge": False, "is_defi": False,
                           "label": dex_name, "node_type": "DEX_SWAP", "risk": "LOW RISK"}
                    self._update_node(graph, node_id, cls, depth + 1)
                    self._add_edge(graph, hop_wallet, node_id, tx)
                    if dex_name not in graph["dex_detected"]:
                        graph["dex_detected"].append(dex_name)
                    print(f"[Fetcher] 🔄 SWAP: {hop_wallet[:20]}... → {dex_name}")
                    continue

                classification = _classify_dest(dest)
                if not classification["is_cex"] and not classification["is_dex"] and not classification["is_bridge"] and self.use_helius:
                    lbl = await self._helius_get_entity_label(dest)
                    if lbl:
                        ok, name = detect_cex_from_label(lbl)
                        if ok:
                            classification.update({"is_cex": True, "label": name, "node_type": "CEX"})

                self._update_node(graph, dest, classification, depth + 1)
                self._add_edge(graph, hop_wallet, dest, tx)
                self._update_detections(graph, classification)

                if not (classification["is_cex"] or classification["is_bridge"] or classification["is_dex"] or classification["is_defi"]):
                    bfs_queue.append((dest, tx.get("amount"), tx.get("timestamp") or 0, depth + 1))

        graph["summary"] = {
            "total_nodes": len(graph["nodes"]), "total_edges": len(graph["edges"]),
            "max_depth":   max((n["depth"] for n in graph["nodes"]), default=0),
            "cex_found":   len(graph["cex_detected"]) > 0,
            "bridge_used": len(graph["bridge_detected"]) > 0,
            "dex_used":    len(graph["dex_detected"]) > 0,
            "outgoing_transfers": len(outgoing),
        }
        return graph

    # ─────────────────────────────────────────────────────────────────────────
    # Graph helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _update_node(self, graph: dict, node_id: str, cls: dict, depth: int):
        if node_id not in [n["id"] for n in graph["nodes"]]:
            graph["nodes"].append({"id": node_id, "label": cls["label"], "type": cls["node_type"],
                                   "is_cex": cls["is_cex"], "is_dex": cls["is_dex"],
                                   "is_bridge": cls["is_bridge"], "is_defi": cls["is_defi"], "depth": depth})

    def _add_edge(self, graph: dict, frm: str, to: str, tx: dict):
        graph["edges"].append({"from": frm, "to": to, "amount": tx.get("amount"),
                               "mint": tx.get("mint"), "timestamp": tx.get("timestamp"),
                               "timestamp_human": self._ts_to_human(tx.get("timestamp")),
                               "signature": tx.get("signature"),
                               "transfer_type": tx.get("transfer_type", "TRANSFER"),
                               "dex_name": tx.get("dex_name")})

    def _update_detections(self, graph: dict, cls: dict):
        lbl = cls["label"]
        if cls["is_cex"]    and lbl not in graph["cex_detected"]:    graph["cex_detected"].append(lbl)
        if cls["is_bridge"] and lbl not in graph["bridge_detected"]: graph["bridge_detected"].append(lbl)
        if cls["is_dex"]    and lbl not in graph["dex_detected"]:    graph["dex_detected"].append(lbl)

    # ─────────────────────────────────────────────────────────────────────────
    # Helius entity label
    # ─────────────────────────────────────────────────────────────────────────

    async def _helius_get_entity_label(self, address: str) -> str:
        if not self.use_helius:
            return ""
        url    = f"{HELIUS_BASE_URL}/addresses/{address}/transactions"
        params = {"api-key": self.helius_key, "limit": 5}
        async with httpx.AsyncClient(timeout=15) as client:
            try:
                resp = await client.get(url, params=params)
                if resp.status_code != 200:
                    return ""
                txs = resp.json()
                if not txs or not isinstance(txs, list):
                    return ""
                for tx in txs:
                    source = (tx.get("source") or "").strip()
                    if source and source not in ("SYSTEM_PROGRAM", "UNKNOWN", ""):
                        ok, name = detect_cex_from_label(source)
                        if ok: return name
                    desc = (tx.get("description") or "").lower()
                    for pattern, name in CEX_NAME_PATTERNS.items():
                        if pattern in desc: return name
                    for acct_data in (tx.get("accountData") or []):
                        if (acct_data.get("account") or "").lower() == address.lower():
                            lbl = acct_data.get("label") or acct_data.get("entity") or ""
                            if lbl: return lbl
                return ""
            except Exception as e:
                print(f"[Helius Entity] Erro {address[:20]}...: {e}")
                return ""

    # ─────────────────────────────────────────────────────────────────────────
    # TX fetching
    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch_tx_data(self, tx_hash: str) -> Optional[dict]:
        if self.use_helius:
            url = f"{HELIUS_BASE_URL}/transactions"
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.post(url, params={"api-key": self.helius_key}, json={"transactions": [tx_hash]})
                    if resp.status_code == 200:
                        results = resp.json()
                        if results and isinstance(results, list) and results[0]:
                            return results[0]
                    print(f"[Helius TX] HTTP {resp.status_code}")
                except Exception as e:
                    print(f"[Helius TX] Erro: {e}")

        tx_data = await self._solscan_get_transaction(tx_hash)
        if tx_data:
            return tx_data

        payload = {"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                   "params": [tx_hash, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]}
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(SOLANA_RPC_URL, json=payload)
                if resp.status_code == 200:
                    result = resp.json().get("result")
                    if result:
                        return self._parse_rpc_transaction(result, tx_hash)
            except Exception as e:
                print(f"[RPC TX] Erro {tx_hash[:20]}: {e}")
        return None

    async def _transfers_from_tx_hash(self, wallet: str, token_lower: str, tx_hash: str) -> list[dict]:
        tx_data = await self._fetch_tx_data(tx_hash)
        if not tx_data:
            return []
        is_sol = token_lower in SOL_SYMBOLS
        ts = tx_data.get("timestamp", 0)
        wallet_lower = wallet.lower()
        strict: list[dict] = []

        if not is_sol:
            for t in tx_data.get("tokenTransfers", []):
                if (t.get("fromUserAccount") or "").lower() == wallet_lower and t.get("toUserAccount"):
                    strict.append({"type": "token", "from": wallet, "to": t["toUserAccount"],
                                   "amount": t.get("tokenAmount"), "mint": t.get("mint"), "signature": tx_hash, "timestamp": ts})
        else:
            for n in tx_data.get("nativeTransfers", []):
                if (n.get("fromUserAccount") or "").lower() == wallet_lower and n.get("toUserAccount") and n.get("amount", 0) > 0:
                    strict.append({"type": "native", "from": wallet, "to": n["toUserAccount"],
                                   "amount": n["amount"] / 1e9, "mint": "SOL", "signature": tx_hash, "timestamp": ts})

        if strict:
            print(f"[Fetcher] TX {tx_hash[:20]}... -> {len(strict)} saída(s) (match exato)")
            return strict

        broad: list[dict] = []
        if not is_sol:
            for t in tx_data.get("tokenTransfers", []):
                if t.get("toUserAccount") and t.get("tokenAmount"):
                    broad.append({"type": "token", "from": t.get("fromUserAccount") or wallet,
                                  "to": t["toUserAccount"], "amount": t["tokenAmount"], "mint": t.get("mint"), "signature": tx_hash, "timestamp": ts})
        else:
            for n in tx_data.get("nativeTransfers", []):
                if n.get("toUserAccount") and n.get("amount", 0) > 0:
                    broad.append({"type": "native", "from": n.get("fromUserAccount") or wallet,
                                  "to": n["toUserAccount"], "amount": n["amount"] / 1e9, "mint": "SOL", "signature": tx_hash, "timestamp": ts})
        print(f"[Fetcher] TX {tx_hash[:20]}... -> {len(broad)} transfer(s) broad")
        return broad

    def _pick_best_from_list(self, transfers: list[dict], target_amount: float) -> list[dict]:
        def pct_diff(t):
            try: return abs(float(t.get("amount") or 0) - target_amount) / target_amount
            except: return float("inf")
        ranked = sorted(transfers, key=pct_diff)
        tight  = [t for t in ranked if pct_diff(t) <= 0.30]
        return tight if tight else [ranked[0]]

    def _best_match_transfers(self, wallet: str, token_lower: str, transactions: list[dict], target_amount: float) -> list[dict]:
        all_t = self._extract_outgoing_transfers(wallet, token_lower, transactions)
        if not all_t:
            return []
        return self._pick_best_from_list(all_t, target_amount)

    def _extract_outgoing_transfers(self, wallet: str, token_lower: str, transactions: list[dict],
                                     min_timestamp: int = 0, received_amount: Optional[float] = None,
                                     max_amount_ratio: float = 10.0) -> list[dict]:
        """
        Extrai transferências de saída de um wallet.
        Detecta TXs de DEX swap e retorna um transfer sintético DEX_SWAP
        em vez dos endereços internos das pools.
        """
        transfers    = []
        wallet_lower = wallet.lower()
        is_sol       = token_lower in SOL_SYMBOLS

        for tx in transactions:
            sig = tx.get("signature", "")
            ts  = tx.get("timestamp", 0) or 0

            if min_timestamp and ts and ts < min_timestamp:
                continue

            # ── DETECÇÃO DE SWAP ─────────────────────────────────────────────
            is_swap, dex_name, dex_pid = self._is_dex_swap_tx(tx)

            if is_swap:
                wallet_is_sender = (
                    any((t.get("fromUserAccount") or "").lower() == wallet_lower for t in tx.get("tokenTransfers", []))
                    or any((n.get("fromUserAccount") or "").lower() == wallet_lower for n in tx.get("nativeTransfers", []))
                )
                if wallet_is_sender:
                    swap_amt = self._sum_outgoing(wallet_lower, tx, is_sol)
                    if received_amount and swap_amt and swap_amt > float(received_amount) * max_amount_ratio:
                        continue
                    dest = dex_pid or dex_name
                    print(f"[Fetcher] 🔄 SWAP TX {sig[:20]}... → {dex_name}")
                    transfers.append({
                        "transfer_type": "DEX_SWAP", "type": "dex_swap",
                        "from": wallet, "to": dest,
                        "dex_name": dex_name, "dex_pid": dex_pid,
                        "amount": swap_amt, "mint": token_lower,
                        "signature": sig, "timestamp": ts,
                    })
                    continue  # não processa transfers individuais desta TX

            # ── TRANSFERS NORMAIS ────────────────────────────────────────────
            if not is_sol:
                for t in tx.get("tokenTransfers", []):
                    from_acct = (t.get("fromUserAccount") or "").lower()
                    mint      = (t.get("mint") or "").lower()
                    to_acct   = t.get("toUserAccount") or ""
                    mint_match = (mint == token_lower) or (not self._looks_like_address(token_lower))
                    if not (from_acct == wallet_lower and to_acct and mint_match):
                        continue
                    try:
                        amt_f = float(t.get("tokenAmount") or 0)
                    except:
                        amt_f = None
                    if received_amount and amt_f and amt_f > float(received_amount) * max_amount_ratio:
                        continue
                    transfers.append({"transfer_type": "TRANSFER", "type": "token", "from": wallet, "to": to_acct,
                                      "amount": t.get("tokenAmount"), "mint": t.get("mint"), "signature": sig, "timestamp": ts})

            if is_sol:
                for n in tx.get("nativeTransfers", []):
                    from_acct = (n.get("fromUserAccount") or "").lower()
                    to_acct   = n.get("toUserAccount") or ""
                    lamports  = n.get("amount", 0)
                    if not (from_acct == wallet_lower and to_acct and lamports > 0):
                        continue
                    sol_amt = lamports / 1e9
                    if received_amount and sol_amt > float(received_amount) * max_amount_ratio:
                        continue
                    transfers.append({"transfer_type": "TRANSFER", "type": "native", "from": wallet, "to": to_acct,
                                      "amount": sol_amt, "mint": "SOL", "signature": sig, "timestamp": ts})

        return transfers

    @staticmethod
    def _looks_like_address(s: str) -> bool:
        return 32 <= len(s) <= 50

    @staticmethod
    def _ts_to_human(ts: Optional[int]) -> str:
        if not ts:
            return "desconhecido"
        try:
            return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
        except:
            return str(ts)
