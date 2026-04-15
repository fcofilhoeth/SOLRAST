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
    is_cex_address, detect_cex_from_label, get_entity_info
)

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
HELIUS_BASE_URL = "https://api.helius.xyz/v0"

SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY", "")
SOLSCAN_BASE_URL = "https://public-api.solscan.io"
SOLSCAN_PRO_URL = "https://pro-api.solscan.io/v2.0"

SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"

SOL_MINT = "So11111111111111111111111111111111111111112"
SOL_SYMBOLS = {"sol", "solana", SOL_MINT.lower()}


class SolanaFetcher:
    def __init__(self):
        self.helius_key = HELIUS_API_KEY
        self.use_helius = bool(HELIUS_API_KEY)
        self.solscan_key = SOLSCAN_API_KEY
        self.use_solscan_pro = bool(SOLSCAN_API_KEY)

    def _solscan_headers(self) -> dict:
        if self.use_solscan_pro:
            return {"token": self.solscan_key}
        return {}

    # -------------------------------------------------------------------------
    # Busca de transações da carteira  (Helius -> Solscan -> RPC)
    # -------------------------------------------------------------------------

    async def get_wallet_transactions(self, wallet: str, limit: int = 50) -> list[dict]:
        """Busca transações com parsing enriquecido.
        Tenta Helius primeiro, depois Solscan, depois RPC público."""
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
                return []
            except Exception as e:
                print(f"[Helius] Erro na requisição: {e}")
                return []

    # -------------------------------------------------------------------------
    # Solscan API
    # -------------------------------------------------------------------------

    async def _solscan_get_wallet_transactions(self, wallet: str, limit: int) -> list[dict]:
        """Busca transações da carteira via Solscan (Pro ou Public API)."""
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
                    data = resp.json()
                    # Pro API envolve em {"success": true, "data": [...]}
                    items = data.get("data", data) if isinstance(data, dict) else data
                    if isinstance(items, list):
                        return [self._parse_solscan_tx(tx) for tx in items if tx]
                print(f"[Solscan] Wallet TXs HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                print(f"[Solscan] Erro wallet TXs: {e}")
        return []

    async def _solscan_get_transaction(self, tx_hash: str) -> Optional[dict]:
        """Busca uma transação específica pelo hash via Solscan."""
        if self.use_solscan_pro:
            url = f"{SOLSCAN_PRO_URL}/transaction/detail"
            params = {"tx": tx_hash}
        else:
            url = f"{SOLSCAN_BASE_URL}/transaction/{tx_hash}"
            params = {}

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, params=params, headers=self._solscan_headers())
                if resp.status_code == 200:
                    data = resp.json()
                    raw = data.get("data", data) if isinstance(data, dict) and "data" in data else data
                    if raw and isinstance(raw, dict):
                        return self._parse_solscan_tx(raw, tx_hash)
                print(f"[Solscan] TX HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                print(f"[Solscan] Erro TX {tx_hash[:20]}: {e}")
        return None

    def _parse_solscan_tx(self, tx: dict, override_sig: str = "") -> dict:
        """Converte resposta do Solscan para o formato interno (igual ao Helius)."""
        sig = override_sig or tx.get("txHash") or tx.get("signature") or tx.get("tx") or ""
        ts = tx.get("blockTime") or tx.get("block_time") or 0
        token_transfers = []
        native_transfers = []

        # Solscan tokenTransfers: [{source/sourceOwner, destination/destinationOwner, token, amount}]
        # Prefer *Owner fields (wallet address) over plain fields (ATA address)
        for t in tx.get("tokenTransfers", []):
            source = t.get("sourceOwner") or t.get("source") or ""
            dest = t.get("destinationOwner") or t.get("destination") or ""
            token_info = t.get("token") or {}
            decimals = int(token_info.get("decimals") or t.get("decimals") or 0)
            raw_amt = t.get("amount") or t.get("tokenAmount") or 0
            try:
                ui_amt = float(raw_amt) / (10 ** decimals) if decimals > 0 else float(raw_amt)
            except (ValueError, TypeError, ZeroDivisionError):
                ui_amt = 0.0
            mint = token_info.get("tokenAddress") or token_info.get("address") or t.get("mint") or ""
            if source and dest and ui_amt > 0:
                token_transfers.append({
                    "fromUserAccount": source,
                    "toUserAccount": dest,
                    "mint": mint,
                    "tokenAmount": ui_amt,
                })

        # Solscan solTransfers: [{source, destination, amount (lamports)}]
        for s in tx.get("solTransfers", []):
            source = s.get("source") or ""
            dest = s.get("destination") or ""
            lamports = s.get("amount") or 0
            if source and dest and lamports > 5000:
                native_transfers.append({
                    "fromUserAccount": source,
                    "toUserAccount": dest,
                    "amount": lamports,
                })

        signers = tx.get("signer") or []
        fee_payer = signers[0] if signers else ""

        return {
            "signature": sig,
            "timestamp": ts,
            "type": tx.get("txType") or "TRANSFER",
            "source": "SOLSCAN",
            "description": tx.get("memo") or "",
            "tokenTransfers": token_transfers,
            "nativeTransfers": native_transfers,
            "feePayer": fee_payer,
        }

    # -------------------------------------------------------------------------
    # Solana RPC público (fallback final)
    # -------------------------------------------------------------------------

    async def _rpc_get_transactions(self, wallet: str, limit: int) -> list[dict]:
        """Fallback: busca via RPC público da Solana (dados mais brutos)."""
        signatures = await self._rpc_get_signatures(wallet, limit)
        transactions = []

        async with httpx.AsyncClient(timeout=30) as client:
            for sig_info in signatures[:20]:  # Limita para não sobrecarregar
                sig = sig_info.get("signature")
                if not sig:
                    continue
                try:
                    payload = {
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getTransaction",
                        "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                    }
                    resp = await client.post(SOLANA_RPC_URL, json=payload)
                    if resp.status_code == 200:
                        result = resp.json().get("result")
                        if result:
                            tx = self._parse_rpc_transaction(result, sig)
                            transactions.append(tx)
                except Exception as e:
                    print(f"[RPC] Erro ao buscar tx {sig[:20]}: {e}")
                    continue

        return transactions

    async def _rpc_get_signatures(self, wallet: str, limit: int) -> list[dict]:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [wallet, {"limit": limit}]
        }
        async with httpx.AsyncClient(timeout=20) as client:
            try:
                resp = await client.post(SOLANA_RPC_URL, json=payload)
                if resp.status_code == 200:
                    return resp.json().get("result", [])
            except Exception as e:
                print(f"[RPC] Erro getSignaturesForAddress: {e}")
        return []

    def _parse_rpc_transaction(self, tx_data: dict, signature: str) -> dict:
        """Converte tx RPC bruta para formato parecido com Helius."""
        meta = tx_data.get("meta", {}) or {}
        message = tx_data.get("transaction", {}).get("message", {})
        block_time = tx_data.get("blockTime", 0)

        token_transfers = []
        native_transfers = []

        # Token balance changes
        pre_token_balances = {b["accountIndex"]: b for b in (meta.get("preTokenBalances") or [])}
        post_token_balances = {b["accountIndex"]: b for b in (meta.get("postTokenBalances") or [])}
        account_keys = [a.get("pubkey", "") for a in message.get("accountKeys", [])]

        # Collect per-mint outgoing and incoming sides separately, then pair them.
        # The raw RPC gives balance deltas per account — each account only knows its
        # own side of the transfer. We must match sender ↔ receiver by mint+amount
        # to produce Helius-style entries with both fromUserAccount and toUserAccount.
        from collections import defaultdict
        mint_outgoing: dict = defaultdict(list)  # mint -> [{owner, amount}]
        mint_incoming: dict = defaultdict(list)  # mint -> [{owner, amount}]

        all_indices = set(pre_token_balances.keys()) | set(post_token_balances.keys())
        for idx in all_indices:
            pre = pre_token_balances.get(idx, {})
            post = post_token_balances.get(idx, {})
            pre_amount = float((pre.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            post_amount = float((post.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            diff = post_amount - pre_amount

            if abs(diff) < 1e-9:
                continue

            # `owner` is the actual wallet address; fall back to account key (ATA) only
            # if owner is missing (some older RPC responses may omit it).
            owner = (
                post.get("owner") or pre.get("owner")
                or (account_keys[idx] if idx < len(account_keys) else "unknown")
            )
            mint = post.get("mint") or pre.get("mint") or ""

            if diff < 0:
                mint_outgoing[mint].append({"owner": owner, "amount": abs(diff)})
            else:
                mint_incoming[mint].append({"owner": owner, "amount": abs(diff)})

        # Pair each outgoing side with the best-matching incoming side (same mint).
        all_mints = set(mint_outgoing.keys()) | set(mint_incoming.keys())
        for mint in all_mints:
            outs = mint_outgoing.get(mint, [])
            ins = mint_incoming.get(mint, [])
            used_incoming: set = set()

            for out in outs:
                # Find the incoming entry with the closest amount (fees may cause small diff)
                best_idx = None
                best_diff = float("inf")
                for i, inc in enumerate(ins):
                    if i in used_incoming:
                        continue
                    d = abs(inc["amount"] - out["amount"])
                    if d < best_diff:
                        best_diff = d
                        best_idx = i

                to_owner = ins[best_idx]["owner"] if best_idx is not None else ""
                if best_idx is not None:
                    used_incoming.add(best_idx)

                token_transfers.append({
                    "fromUserAccount": out["owner"],
                    "toUserAccount": to_owner,
                    "mint": mint,
                    "tokenAmount": out["amount"],
                })

            # Unmatched incoming (e.g., initial mint/airdrop with no sender)
            for i, inc in enumerate(ins):
                if i in used_incoming:
                    continue
                token_transfers.append({
                    "fromUserAccount": "",
                    "toUserAccount": inc["owner"],
                    "mint": mint,
                    "tokenAmount": inc["amount"],
                })

        # Native SOL changes — pair senders with receivers the same way.
        pre_balances = meta.get("preBalances", [])
        post_balances = meta.get("postBalances", [])
        sol_senders = []
        sol_receivers = []
        for i, (pre_b, post_b) in enumerate(zip(pre_balances, post_balances)):
            diff_lamports = post_b - pre_b
            if abs(diff_lamports) <= 5000 or i >= len(account_keys):
                continue
            acct = account_keys[i]
            if diff_lamports < 0:
                sol_senders.append({"acct": acct, "amount": abs(diff_lamports)})
            else:
                sol_receivers.append({"acct": acct, "amount": abs(diff_lamports)})

        used_sol_recv: set = set()
        for sender in sol_senders:
            best_idx = None
            best_diff = float("inf")
            for i, recv in enumerate(sol_receivers):
                if i in used_sol_recv:
                    continue
                d = abs(recv["amount"] - sender["amount"])
                if d < best_diff:
                    best_diff = d
                    best_idx = i
            to_acct = sol_receivers[best_idx]["acct"] if best_idx is not None else ""
            if best_idx is not None:
                used_sol_recv.add(best_idx)
            native_transfers.append({
                "fromUserAccount": sender["acct"],
                "toUserAccount": to_acct,
                "amount": sender["amount"],
            })

        return {
            "signature": signature,
            "timestamp": block_time,
            "type": "TRANSFER",
            "source": "SYSTEM_PROGRAM",
            "description": "",
            "tokenTransfers": token_transfers,
            "nativeTransfers": native_transfers,
            "feePayer": account_keys[0] if account_keys else "",
        }

    # -------------------------------------------------------------------------
    # Rastreamento de fluxo de fundos
    # -------------------------------------------------------------------------

    async def trace_token_flow(
        self,
        wallet: str,
        token: str,
        amount: float,
        transactions: list[dict],
        max_hops: int = 2,
        tx_hash: Optional[str] = None,
    ) -> dict:
        """
        Constrói grafo de fluxo de fundos a partir da carteira vítima.

        Args:
            tx_hash: Hash da transação do roubo (opcional). Quando fornecido,
                     o HOP 0->1 é construído a partir dessa TX específica, com
                     precisão total. Sem ele, usa best-match por valor.
        """
        graph = {
            "nodes": [{
                "id": wallet,
                "label": "Carteira Hackeada (Vítima)",
                "type": "VICTIM",
                "is_cex": False,
                "is_bridge": False,
                "depth": 0,
            }],
            "edges": [],
            "cex_detected": [],
            "bridge_detected": [],
            "summary": {},
        }

        token_lower = token.lower().strip()
        visited: set[str] = {wallet}

        # HOP 0->1: determina as transferências de saída da carteira vítima.
        if tx_hash:
            # MODO TX HASH: a próxima carteira está DENTRO desta transação.
            # Nunca buscar em outra TX — o usuário garantiu que este é o hash do roubo.
            print(f"[Fetcher] Modo TX Hash: buscando transação {tx_hash}")
            tx_transfers = await self._transfers_from_tx_hash(wallet, token_lower, tx_hash)

            if not tx_transfers:
                # TX não encontrada na rede (inválida, pruned, ou erro de rede).
                # Não fazemos fallback para o histórico — o resultado seria errado.
                print(
                    f"[Fetcher] ERRO: TX hash {tx_hash[:30]}... não encontrada ou sem transfers. "
                    f"Verifique o hash e a conectividade com o RPC."
                )
                outgoing = []
            elif len(tx_transfers) == 1:
                outgoing = tx_transfers
                print(f"[Fetcher] TX -> única saída: amount={tx_transfers[0].get('amount')} destino={tx_transfers[0].get('to', '')[:30]}...")
            else:
                # Múltiplos transfers na TX — seleciona o mais próximo do amount informado.
                outgoing = self._pick_best_from_list(tx_transfers, amount)
                print(
                    f"[Fetcher] TX com {len(tx_transfers)} transfers — "
                    f"selecionado: amount={outgoing[0].get('amount')} destino={outgoing[0].get('to', '')[:30]}..."
                )
        else:
            # MODO HEURÍSTICO: sem hash, usa best-match por proximidade ao valor roubado.
            outgoing = self._best_match_transfers(wallet, token_lower, transactions, amount)

        # Após HOP 0->1, captura o mint address real da transferência encontrada.
        # Isso garante que o HOP 1->2 rastreie o mesmo token, mesmo que o usuário
        # tenha informado apenas um símbolo (ex: "Anon") em vez do mint address.
        actual_mint = next(
            (tx["mint"].lower() for tx in outgoing if tx.get("mint") and tx["mint"].lower() != "sol"),
            token_lower,
        )
        print(f"[Fetcher] Mint real identificado no HOP 0->1: {actual_mint}")

        # HOP 0->1: processa saídas da carteira vítima e monta o grafo inicial.
        # BFS queue: (wallet_addr, received_amount, received_timestamp, depth)
        bfs_queue: list[tuple[str, object, int, int]] = []

        for tx in outgoing:
            dest = tx.get("to")
            if not dest or dest == wallet:
                continue

            is_cex, cex_name = is_cex_address(dest)
            is_bridge = dest in BRIDGE_PROGRAMS

            # Detecção dinâmica via Helius para endereços não catalogados estaticamente
            if not is_cex and not is_bridge and self.use_helius:
                helius_label = await self._helius_get_entity_label(dest)
                if helius_label:
                    is_cex_dyn, cex_dyn_name = detect_cex_from_label(helius_label)
                    if is_cex_dyn:
                        is_cex, cex_name = True, cex_dyn_name
                        print(f"[Helius] CEX detectada dinamicamente (HOP 0→1): {cex_dyn_name} | {dest[:20]}...")

            node_label = cex_name or BRIDGE_PROGRAMS.get(dest) or DEFI_PROGRAMS.get(dest) or "Wallet Destino"
            node_type = "CEX" if is_cex else ("BRIDGE" if is_bridge else "WALLET")

            if dest not in [n["id"] for n in graph["nodes"]]:
                graph["nodes"].append({
                    "id": dest,
                    "label": node_label,
                    "type": node_type,
                    "is_cex": is_cex,
                    "is_bridge": is_bridge,
                    "depth": 1,
                })

            graph["edges"].append({
                "from": wallet,
                "to": dest,
                "amount": tx.get("amount"),
                "mint": tx.get("mint"),
                "timestamp": tx.get("timestamp"),
                "timestamp_human": self._ts_to_human(tx.get("timestamp")),
                "signature": tx.get("signature"),
            })

            if is_cex and cex_name not in graph["cex_detected"]:
                graph["cex_detected"].append(cex_name)
            if is_bridge and node_label not in graph["bridge_detected"]:
                graph["bridge_detected"].append(node_label)

            # Adiciona à fila BFS apenas carteiras intermediárias (não CEX, não bridge)
            if not is_cex and not is_bridge and dest not in visited:
                bfs_queue.append((
                    dest,
                    tx.get("amount"),
                    tx.get("timestamp") or 0,
                    1,
                ))

        # BFS: rastreia hops intermediários até max_hops de profundidade.
        # Para em cada caminho ao detectar CEX ou bridge.
        while bfs_queue:
            hop_wallet, received_amount, received_timestamp, current_depth = bfs_queue.pop(0)

            if current_depth >= max_hops:
                print(f"[Fetcher] BFS: profundidade máxima {max_hops} atingida em {hop_wallet[:20]}...")
                continue
            if hop_wallet in visited:
                continue

            visited.add(hop_wallet)
            print(f"[Fetcher] BFS hop {current_depth}→{current_depth+1}: rastreando {hop_wallet[:20]}...")

            hop_txs = await self.get_wallet_transactions(hop_wallet, limit=20)
            hop_out = self._extract_outgoing_transfers(
                hop_wallet,
                actual_mint,
                hop_txs,
                min_timestamp=received_timestamp,
                received_amount=received_amount,
            )

            for tx in hop_out[:5]:
                dest = tx.get("to")
                if not dest or dest in visited:
                    continue

                is_cex, cex_name = is_cex_address(dest)
                is_bridge = dest in BRIDGE_PROGRAMS

                # Detecção dinâmica via Helius para endereços não catalogados estaticamente
                if not is_cex and not is_bridge and self.use_helius:
                    helius_label = await self._helius_get_entity_label(dest)
                    if helius_label:
                        is_cex_dyn, cex_dyn_name = detect_cex_from_label(helius_label)
                        if is_cex_dyn:
                            is_cex, cex_name = True, cex_dyn_name
                            print(f"[Helius] CEX detectada dinamicamente (HOP {current_depth}→{current_depth+1}): {cex_dyn_name} | {dest[:20]}...")

                node_label = cex_name or BRIDGE_PROGRAMS.get(dest) or DEFI_PROGRAMS.get(dest) or f"Wallet Intermediária"
                node_type = "CEX" if is_cex else ("BRIDGE" if is_bridge else "WALLET")

                if dest not in [n["id"] for n in graph["nodes"]]:
                    graph["nodes"].append({
                        "id": dest,
                        "label": node_label,
                        "type": node_type,
                        "is_cex": is_cex,
                        "is_bridge": is_bridge,
                        "depth": current_depth + 1,
                    })

                graph["edges"].append({
                    "from": hop_wallet,
                    "to": dest,
                    "amount": tx.get("amount"),
                    "mint": tx.get("mint"),
                    "timestamp": tx.get("timestamp"),
                    "timestamp_human": self._ts_to_human(tx.get("timestamp")),
                    "signature": tx.get("signature"),
                })

                if is_cex and cex_name not in graph["cex_detected"]:
                    graph["cex_detected"].append(cex_name)
                if is_bridge and node_label not in graph["bridge_detected"]:
                    graph["bridge_detected"].append(node_label)

                # Encerra este caminho ao detectar CEX ou bridge — não há razão para continuar
                if is_cex or is_bridge:
                    continue

                # Continua rastreando esta carteira no próximo nível do BFS
                bfs_queue.append((
                    dest,
                    tx.get("amount"),
                    tx.get("timestamp") or 0,
                    current_depth + 1,
                ))

        graph["summary"] = {
            "total_nodes": len(graph["nodes"]),
            "total_edges": len(graph["edges"]),
            "max_depth": max((n["depth"] for n in graph["nodes"]), default=0),
            "cex_found": len(graph["cex_detected"]) > 0,
            "bridge_used": len(graph["bridge_detected"]) > 0,
            "outgoing_transfers": len(outgoing),
        }

        return graph

    async def _helius_get_entity_label(self, address: str) -> str:
        """
        Consulta o Helius para identificar dinamicamente o label/entidade de um endereço.

        Estratégia em camadas:
          1. Verifica o campo `source` das transações mais recentes (ex: "BINANCE", "COINBASE")
          2. Analisa o campo `description` por menções textuais de CEX conhecidas
          3. Inspeciona `accountData[].label` no formato Enhanced Transaction do Helius

        Retorna o label detectado (ex: "Binance") ou "" se desconhecido.
        Chamado apenas para endereços não presentes na base estática para minimizar API calls.
        """
        if not self.use_helius:
            return ""

        url = f"{HELIUS_BASE_URL}/addresses/{address}/transactions"
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
                    # Camada 1: source field (Helius mapeia protocolos conhecidos)
                    source = (tx.get("source") or "").strip()
                    if source and source not in ("SYSTEM_PROGRAM", "UNKNOWN", ""):
                        # source pode ser "BINANCE", "COINBASE", "JUPITER", etc.
                        is_cex_src, cex_name_src = detect_cex_from_label(source)
                        if is_cex_src:
                            return cex_name_src

                    # Camada 2: description textual
                    desc = (tx.get("description") or "").lower()
                    if desc:
                        for pattern, name in CEX_NAME_PATTERNS.items():
                            if pattern in desc:
                                return name

                    # Camada 3: accountData[].label (formato Enhanced Transaction Helius)
                    address_lower = address.lower()
                    for acct_data in (tx.get("accountData") or []):
                        if (acct_data.get("account") or "").lower() == address_lower:
                            label = acct_data.get("label") or acct_data.get("entity") or ""
                            if label:
                                return label

                return ""

            except Exception as e:
                print(f"[Helius Entity] Erro ao resolver label para {address[:20]}...: {e}")
                return ""

    async def _fetch_tx_data(self, tx_hash: str) -> Optional[dict]:
        """
        Busca os dados de uma transação pelo hash.
        Prioridade: Helius -> Solscan -> RPC público.
        Retorna o objeto de transação parseado, ou None se não encontrado em nenhuma fonte.
        """
        # ── 1. Helius (mais rico) ─────────────────────────────────────────────
        if self.use_helius:
            url = f"{HELIUS_BASE_URL}/transactions"
            params = {"api-key": self.helius_key}
            payload = {"transactions": [tx_hash]}
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.post(url, params=params, json=payload)
                    if resp.status_code == 200:
                        results = resp.json()
                        if results and isinstance(results, list) and results[0]:
                            print(f"[Fetcher] TX {tx_hash[:20]}... obtida via Helius")
                            return results[0]
                    print(f"[Helius TX] HTTP {resp.status_code} — tentando Solscan...")
                except Exception as e:
                    print(f"[Helius TX] Erro: {e} — tentando Solscan...")

        # ── 2. Solscan (dados parseados, sem necessidade de key) ──────────────
        tx_data = await self._solscan_get_transaction(tx_hash)
        if tx_data:
            print(f"[Fetcher] TX {tx_hash[:20]}... obtida via Solscan")
            return tx_data
        print(f"[Solscan TX] Não encontrada — tentando RPC público...")

        # ── 3. RPC público (dados brutos) ─────────────────────────────────────
        payload_rpc = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTransaction",
            "params": [tx_hash, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
        }
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.post(SOLANA_RPC_URL, json=payload_rpc)
                if resp.status_code == 200:
                    result = resp.json().get("result")
                    if result:
                        print(f"[Fetcher] TX {tx_hash[:20]}... obtida via RPC público")
                        return self._parse_rpc_transaction(result, tx_hash)
                    print(f"[RPC TX] Transação não encontrada em nenhuma fonte: {tx_hash[:30]}...")
            except Exception as e:
                print(f"[RPC TX] Erro ao buscar {tx_hash[:20]}: {e}")

        return None

    async def _transfers_from_tx_hash(
        self,
        wallet: str,
        token_lower: str,
        tx_hash: str,
    ) -> list[dict]:
        """
        Busca uma transação específica pelo hash e extrai as transferências
        de saída relevantes.

        Estratégia em dois passos:
          1. Tenta encontrar transfers onde `fromUserAccount == wallet` (match exato).
          2. Se não encontrar nada (wallet pode aparecer como ATA ou fee payer),
             retorna TODOS os transfers de saída com `toUserAccount` preenchido —
             o chamador escolhe pelo amount. NUNCA busca em outra transação.
        """
        tx_data = await self._fetch_tx_data(tx_hash)
        if not tx_data:
            print(f"[Fetcher] TX {tx_hash[:30]}... não encontrada na rede.")
            return []

        is_sol = token_lower in SOL_SYMBOLS
        ts = tx_data.get("timestamp", 0)

        # ── Passo 1: match exato — wallet é o remetente direto ────────────────
        wallet_lower = wallet.lower()
        strict: list[dict] = []

        if not is_sol:
            for t in tx_data.get("tokenTransfers", []):
                from_acct = (t.get("fromUserAccount") or "").lower()
                to_acct = t.get("toUserAccount") or ""
                if from_acct == wallet_lower and to_acct:
                    strict.append({
                        "type": "token",
                        "from": wallet,
                        "to": to_acct,
                        "amount": t.get("tokenAmount"),
                        "mint": t.get("mint"),
                        "signature": tx_hash,
                        "timestamp": ts,
                    })
        else:
            for n in tx_data.get("nativeTransfers", []):
                from_acct = (n.get("fromUserAccount") or "").lower()
                to_acct = n.get("toUserAccount") or ""
                amt = n.get("amount", 0)
                if from_acct == wallet_lower and to_acct and amt > 0:
                    strict.append({
                        "type": "native",
                        "from": wallet,
                        "to": to_acct,
                        "amount": amt / 1e9,
                        "mint": "SOL",
                        "signature": tx_hash,
                        "timestamp": ts,
                    })

        if strict:
            print(f"[Fetcher] TX {tx_hash[:20]}... -> {len(strict)} saída(s) do wallet vítima (match exato)")
            return strict

        # ── Passo 2: match amplo — coleta TODOS os transfers com destino ──────
        # O wallet pode não aparecer como fromUserAccount (ex: assina via programa,
        # usa ATA de outro owner, ou é o fee payer de uma instrução interna).
        # Como o usuário garantiu que ESTA TX é a do roubo, qualquer transfer com
        # destino preenchido é candidato — o chamador seleciona pelo amount.
        broad: list[dict] = []

        if not is_sol:
            for t in tx_data.get("tokenTransfers", []):
                to_acct = t.get("toUserAccount") or ""
                amt = t.get("tokenAmount")
                if to_acct and amt:
                    broad.append({
                        "type": "token",
                        "from": t.get("fromUserAccount") or wallet,
                        "to": to_acct,
                        "amount": amt,
                        "mint": t.get("mint"),
                        "signature": tx_hash,
                        "timestamp": ts,
                    })
        else:
            for n in tx_data.get("nativeTransfers", []):
                to_acct = n.get("toUserAccount") or ""
                amt = n.get("amount", 0)
                if to_acct and amt > 0:
                    broad.append({
                        "type": "native",
                        "from": n.get("fromUserAccount") or wallet,
                        "to": to_acct,
                        "amount": amt / 1e9,
                        "mint": "SOL",
                        "signature": tx_hash,
                        "timestamp": ts,
                    })

        if broad:
            print(
                f"[Fetcher] TX {tx_hash[:20]}... -> wallet não aparece como remetente direto. "
                f"Retornando {len(broad)} transfer(s) da TX para seleção por amount."
            )
        else:
            print(f"[Fetcher] TX {tx_hash[:20]}... -> nenhum transfer encontrado na transação.")

        return broad

    def _pick_best_from_list(
        self,
        transfers: list[dict],
        target_amount: float,
    ) -> list[dict]:
        """
        Dado uma lista de transfers já coletados, retorna o(s) mais próximo(s)
        do target_amount. Usado quando uma única TX contém múltiplas saídas.
        """
        def pct_diff(t: dict) -> float:
            try:
                return abs(float(t.get("amount") or 0) - target_amount) / target_amount
            except (TypeError, ValueError, ZeroDivisionError):
                return float("inf")

        ranked = sorted(transfers, key=pct_diff)

        for t in ranked:
            print(
                f"[Fetcher]   candidato: {t.get('amount')} -> {t.get('to','')[:20]}... "
                f"({pct_diff(t)*100:.1f}% diff do alvo {target_amount})"
            )

        # Retorna os que estão dentro de ±30%, senão só o mais próximo
        tight = [t for t in ranked if pct_diff(t) <= 0.30]
        return tight if tight else [ranked[0]]

    def _best_match_transfers(
        self,
        wallet: str,
        token_lower: str,
        transactions: list[dict],
        target_amount: float,
    ) -> list[dict]:
        """
        Coleta todas as saídas do wallet e delega a seleção ao _pick_best_from_list.
        Resolve o problema de tokens por símbolo onde qualquer SPL token é aceito.
        """
        all_transfers = self._extract_outgoing_transfers(wallet, token_lower, transactions)
        if not all_transfers:
            return []
        print(f"[Fetcher] Best-match HOP 0->1: {len(all_transfers)} candidatos para target={target_amount}")
        return self._pick_best_from_list(all_transfers, target_amount)

    def _extract_outgoing_transfers(
        self,
        wallet: str,
        token_lower: str,
        transactions: list[dict],
        min_timestamp: int = 0,
        received_amount: Optional[float] = None,
        max_amount_ratio: float = 10.0,
    ) -> list[dict]:
        """
        Extrai transferências de saída de um wallet para um token.
        Usado internamente por _best_match_transfers (HOP 0->1) e diretamente (HOP 1->2+).

        Args:
            min_timestamp:    Ignora TXs anteriores a este timestamp (hops 1->2+).
            received_amount:  Valor recebido no hop anterior. TXs com valor
                              > received_amount * max_amount_ratio são ignoradas
                              (possíveis fundos pré-existentes não relacionados ao roubo).
            max_amount_ratio: Multiplicador de tolerância sobre received_amount (padrão 10x).
        """
        transfers = []
        wallet_lower = wallet.lower()
        is_sol = token_lower in SOL_SYMBOLS

        for tx in transactions:
            sig = tx.get("signature", "")
            ts = tx.get("timestamp", 0) or 0

            # Filtro temporal: ignora TXs anteriores à chegada dos fundos roubados
            if min_timestamp and ts and ts < min_timestamp:
                continue

            # Token transfers (SPL)
            if not is_sol:
                for t in tx.get("tokenTransfers", []):
                    from_acct = (t.get("fromUserAccount") or "").lower()
                    mint = (t.get("mint") or "").lower()
                    to_acct = t.get("toUserAccount") or ""

                    mint_match = (mint == token_lower) or (
                        not self._looks_like_address(token_lower)
                    )

                    if not (from_acct == wallet_lower and to_acct and mint_match):
                        continue

                    outgoing_amt = t.get("tokenAmount")

                    try:
                        amt_float = float(outgoing_amt) if outgoing_amt is not None else None
                    except (TypeError, ValueError):
                        amt_float = None

                    # Filtro de proporção (HOP 1->2+): ignora valores muito maiores que o recebido
                    if received_amount and amt_float is not None:
                        if amt_float > float(received_amount) * max_amount_ratio:
                            print(
                                f"[Fetcher] Ignorando TX de {amt_float} "
                                f"(recebido: {received_amount}, ratio: "
                                f"{amt_float / float(received_amount):.1f}x > "
                                f"{max_amount_ratio}x) — possível fundo pré-existente"
                            )
                            continue

                    transfers.append({
                        "type": "token",
                        "from": wallet,
                        "to": to_acct,
                        "amount": outgoing_amt,
                        "mint": t.get("mint"),
                        "signature": sig,
                        "timestamp": ts,
                    })

            # Native SOL transfers
            if is_sol:
                for n in tx.get("nativeTransfers", []):
                    from_acct = (n.get("fromUserAccount") or "").lower()
                    to_acct = n.get("toUserAccount") or ""
                    amt_lamports = n.get("amount", 0)

                    if not (from_acct == wallet_lower and to_acct and amt_lamports > 0):
                        continue

                    outgoing_sol = amt_lamports / 1e9

                    # Filtro de proporção (HOP 1->2+)
                    if received_amount and outgoing_sol > float(received_amount) * max_amount_ratio:
                        print(
                            f"[Fetcher] Ignorando TX SOL de {outgoing_sol:.4f} "
                            f"(recebido: {received_amount}, ratio: "
                            f"{outgoing_sol / float(received_amount):.1f}x) — possível fundo pré-existente"
                        )
                        continue

                    transfers.append({
                        "type": "native",
                        "from": wallet,
                        "to": to_acct,
                        "amount": outgoing_sol,
                        "mint": "SOL",
                        "signature": sig,
                        "timestamp": ts,
                    })

        return transfers

    @staticmethod
    def _looks_like_address(s: str) -> bool:
        """Verifica se a string parece um endereço Solana (base58, ~44 chars)."""
        return len(s) >= 32 and len(s) <= 50

    @staticmethod
    def _ts_to_human(ts: Optional[int]) -> str:
        if not ts:
            return "desconhecido"
        try:
            return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return str(ts)
