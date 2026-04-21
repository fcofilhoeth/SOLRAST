"""
SolTrace - Bot Analisador Rule-Based.
Handles simple, deterministic investigation cases without AI costs.
"""

import math
from typing import Optional
from datetime import datetime
from collections import defaultdict


class BotAnalyzer:

    # ─────────────────────────────────────────────────────────────────────────
    # Feature computation
    # ─────────────────────────────────────────────────────────────────────────

    def compute_features(self, flow_graph: dict, transactions: list[dict]) -> dict:
        nodes     = flow_graph.get("nodes", [])
        edges     = flow_graph.get("edges", [])
        num_hops  = flow_graph.get("summary", {}).get("max_depth", 0)
        has_bridge = len(flow_graph.get("bridge_detected", [])) > 0

        out_degree: dict[str, int]  = defaultdict(int)
        in_sources: dict[str, set]  = defaultdict(set)

        for edge in edges:
            frm = edge.get("from", "")
            to  = edge.get("to",   "")
            if frm: out_degree[frm] += 1
            if to and frm: in_sources[to].add(frm)

        num_splits = sum(1 for c in out_degree.values() if c > 1)
        num_merges = sum(1 for s in in_sources.values() if len(s) > 1)

        amounts = []
        for edge in edges:
            amt = edge.get("amount")
            if amt is not None:
                try: amounts.append(float(amt))
                except: pass

        value_entropy = 0.0
        if len(amounts) > 1:
            total = sum(amounts)
            if total > 0:
                probs = [a / total for a in amounts]
                value_entropy = -sum(p * math.log2(p) for p in probs if p > 0)

        labeled_nodes = 0
        for n in nodes:
            label = n.get("label", "")
            is_labeled = (
                n.get("is_cex") or n.get("is_bridge") or n.get("is_dex") or n.get("is_defi")
                or n.get("type") in ("CEX", "BRIDGE", "DEFI", "DEX_SWAP", "DEX", "VICTIM")
                or (label and label not in ("Wallet Destino", "Wallet Final", "Unknown", "Desconhecido", ""))
            )
            if is_labeled:
                labeled_nodes += 1

        return {
            "num_hops":            num_hops,
            "num_wallets":         len(nodes),
            "num_splits":          num_splits,
            "num_merges":          num_merges,
            "value_entropy":       round(value_entropy, 4),
            "has_bridge":          has_bridge,
            "label_coverage_ratio": round(labeled_nodes / max(len(nodes), 1), 4),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Main analysis
    # ─────────────────────────────────────────────────────────────────────────

    def analyze(self, wallet: str, token: str, amount: float, token_name: Optional[str],
                transactions: list[dict], flow_graph: dict, features: dict) -> dict:

        cex_detected    = flow_graph.get("cex_detected",    [])
        bridge_detected = flow_graph.get("bridge_detected", [])
        dex_detected    = flow_graph.get("dex_detected",    [])
        nodes           = flow_graph.get("nodes", [])
        edges           = flow_graph.get("edges", [])
        token_display   = token_name or token
        now             = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        exec_summary, confidence = self._executive_summary(
            cex_detected, bridge_detected, dex_detected, nodes, edges, features, token_display
        )
        ascii_map      = self._build_ascii_map(wallet, nodes, edges)
        cex_section    = self._build_cex_section(cex_detected, edges, nodes, wallet, token_display, amount, now)
        dex_section    = self._build_dex_section(dex_detected, edges, nodes)
        entities_table = self._build_entities_table(nodes)
        timeline       = self._build_timeline(edges, nodes)
        metrics_table  = self._build_metrics_table(features, edges, dex_detected)
        hypothesis     = self._build_hypothesis(cex_detected, bridge_detected, dex_detected, nodes, features)
        next_steps     = self._build_next_steps(cex_detected, bridge_detected, dex_detected, features)

        raw_markdown = f"""### 🔍 SUMÁRIO EXECUTIVO
{exec_summary}

**Confiança geral:** {confidence} | **Método:** Análise Rule-Based (BOT) | **Data:** {now}

---

### 🗺️ MAPA DO FLUXO DE FUNDOS

```
{ascii_map}
```

---

### 🏦 ANÁLISE CEX
{cex_section}

---

### 🔄 ANÁLISE DEX / SWAP
{dex_section}

---

### 📋 ENTIDADES IDENTIFICADAS
{entities_table}

---

### ⏱️ TIMELINE DE EVENTOS
{timeline}

---

### 📊 MÉTRICAS DO GRAFO
{metrics_table}

---

### ⚠️ HIPÓTESE PRINCIPAL + ALTERNATIVAS

**HIPÓTESE PRINCIPAL [CONFIANÇA: {confidence}]:**
{hypothesis}

**Hipóteses alternativas:**
- Carteiras intermediárias podem ser contas de passagem (ephemeral accounts)
- Token pode ter sido convertido via DEX e saído por outra rota

---

### 📬 PRÓXIMOS PASSOS RECOMENDADOS
{next_steps}

---
*Relatório gerado pelo **SolTrace BOT** (análise rule-based, sem custo de IA). \
Para casos com alta complexidade (splits, merges, bridges, ofuscação), o sistema escala automaticamente para o Agente de IA.*
"""

        return {
            "raw_markdown": raw_markdown,
            "has_cex": len(cex_detected) > 0,
            "graph": flow_graph,
            "metadata": {
                "wallet": wallet, "token": token_display, "token_mint": token, "amount": amount,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "model": "rule-based-bot", "analysis_method": "bot", "features": features,
                "transactions_analyzed": len(transactions),
                "graph_nodes": len(nodes), "graph_edges": len(edges),
                "cex_detected": cex_detected, "bridge_detected": bridge_detected,
                "dex_detected": dex_detected,
            },
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Section builders
    # ─────────────────────────────────────────────────────────────────────────

    def _executive_summary(self, cex_detected, bridge_detected, dex_detected,
                           nodes, edges, features, token_display) -> tuple[str, str]:
        if cex_detected:
            return (
                f"Fundos rastreados com **ALTA CONFIANÇA**. "
                f"Detectado depósito em CEX: **{', '.join(cex_detected)}**. "
                f"Fluxo de {features['num_hops']} hop(s). Recomenda-se contato imediato para bloqueio preventivo.",
                "HIGH"
            )

        if dex_detected:
            return (
                f"Fundos rastreados por {features['num_hops']} hop(s). "
                f"**Swap detectado via DEX: {', '.join(dex_detected)}**. "
                f"O token original pode ter sido convertido em outro ativo — rastreamento do token de saída é necessário. "
                f"Técnica comum de ofuscação de trail on-chain.",
                "MEDIUM"
            )

        if bridge_detected:
            return (
                f"Fundos encaminhados para bridge cross-chain: **{', '.join(bridge_detected)}**. "
                f"Rastreamento em {features['num_hops']} hop(s). Destino final pode estar em outra blockchain.",
                "MEDIUM"
            )

        dest_nodes = [n for n in nodes if n.get("depth", 0) > 0]
        if dest_nodes:
            labels = list(dict.fromkeys(
                n.get("label", "") for n in dest_nodes
                if n.get("label") not in ("Wallet Destino", "Wallet Final", None, "", "Desconhecido")
            ))
            label_str = f"Entidades: {', '.join(labels[:3])}. " if labels else ""
            return (
                f"Fundos rastreados por {features['num_hops']} hop(s) para "
                f"{len(dest_nodes)} carteira(s). {label_str}Fluxo sem padrões complexos detectados pelo BOT.",
                "MEDIUM"
            )

        return (
            f"Nenhuma transferência de saída identificada para **{token_display}** nas transações analisadas.",
            "LOW"
        )

    def _build_ascii_map(self, wallet: str, nodes: list, edges: list) -> str:
        lines = [f"[Vítima: {wallet}]"]

        depth_nodes: dict[int, list] = defaultdict(list)
        for n in nodes:
            if n["id"] != wallet:
                depth_nodes[n.get("depth", 1)].append(n)

        edge_map: dict[str, dict] = {}
        for edge in edges:
            to = edge.get("to", "")
            if to and to not in edge_map:
                edge_map[to] = edge

        for depth in sorted(depth_nodes.keys()):
            for node in depth_nodes[depth]:
                node_id = node["id"]
                label   = node.get("label", "Desconhecido")
                indent  = "    " * (depth - 1)

                # Ícones por tipo
                if node.get("is_cex"):
                    icon = " ⚠️ [CEX]"
                elif node.get("is_bridge"):
                    icon = " 🌉 [BRIDGE]"
                elif node.get("is_dex") or node.get("type") == "DEX_SWAP":
                    icon = " 🔄 [DEX SWAP]"
                elif node.get("is_defi"):
                    icon = " 🏦 [DEFI]"
                else:
                    icon = ""

                edge = edge_map.get(node_id, {})
                amt  = edge.get("amount", "?")
                sig  = edge.get("signature") or "?"
                t_type = edge.get("transfer_type", "TRANSFER")
                dex_n  = edge.get("dex_name", "")

                if t_type == "DEX_SWAP" and dex_n:
                    lines.append(f"{indent}        ↓ SWAP via {dex_n} | entrada: {amt}")
                else:
                    lines.append(f"{indent}        ↓ {amt}")
                lines.append(f"{indent}        TX: {sig}")
                lines.append(f"{indent}[{label}: {node_id}]{icon}")

        return "\n".join(lines) if len(lines) > 1 else f"[Vítima: {wallet}]\n    (sem transferências identificadas)"

    def _build_cex_section(self, cex_detected, edges, nodes, wallet, token_display, amount, now) -> str:
        if not cex_detected:
            dest_ids   = {e.get("to") for e in edges}
            source_ids = {e.get("from") for e in edges}
            final_ids  = dest_ids - source_ids - {wallet}
            final_nodes = [n for n in nodes if n["id"] in final_ids and not n.get("is_dex") and not n.get("is_bridge")]

            if final_nodes:
                addrs = "\n".join(f"  - `{n['id']}` — {n.get('label', 'desconhecido')}" for n in final_nodes[:5])
                return f"Nenhuma CEX identificada nas transações analisadas.\n\n**Endereços finais detectados:**\n{addrs}"
            return "Nenhuma CEX identificada."

        sections = []
        for cex_name in cex_detected:
            cex_nodes = [n for n in nodes if n.get("is_cex") and cex_name.lower() in n.get("label", "").lower()]
            for cex_node in cex_nodes:
                deposit_edges = [e for e in edges if e.get("to") == cex_node["id"]]
                for dep_edge in deposit_edges[:1]:
                    sig = dep_edge.get("signature", "N/A")
                    ts  = dep_edge.get("timestamp_human", "desconhecido")
                    amt = dep_edge.get("amount", amount)
                    sections.append(f"""**Exchange detectada: {cex_name}**
- Endereço de depósito: `{cex_node['id']}`
- TX de depósito: `{sig}`
- Timestamp: {ts}
- Valor: {amt} {token_display}
- **STATUS: FUNDOS POTENCIALMENTE RASTREÁVEIS — SOLICITE BLOQUEIO IMEDIATAMENTE**

**Template de Contato Formal:**
```
Assunto: Solicitação Urgente de Bloqueio de Fundos — Roubo Confirmado em Blockchain

Para: Equipe de Compliance / {cex_name}
Data: {now}

Prezados,

Identificamos que fundos roubados foram depositados em endereço associado à vossa exchange.

Detalhes:
- Carteira da vítima: {wallet}
- Token roubado: {token_display}
- Valor: {amount}
- TX de depósito: {sig}
- Endereço de depósito: {cex_node['id']}
- Timestamp: {ts}

Solicitamos urgentemente:
1. Bloqueio preventivo dos fundos
2. Preservação de registros KYC
3. Cooperação com as autoridades

Evidências on-chain disponíveis mediante solicitação.
```""")

        return "\n\n".join(sections) if sections else f"CEX detectada: {', '.join(cex_detected)} (detalhes não encontrados no grafo)"

    def _build_dex_section(self, dex_detected, edges, nodes) -> str:
        if not dex_detected:
            return "Nenhum swap em DEX detectado nas transações analisadas."

        lines = [
            f"⚠️ **Swap detectado em DEX: {', '.join(dex_detected)}**\n",
            "Os fundos foram enviados para um agregador/AMM de troca de tokens. "
            "Isso indica que o token original pode ter sido convertido em outro ativo, "
            "dificultando o rastreamento direto.\n",
        ]

        swap_edges = [e for e in edges if e.get("transfer_type") == "DEX_SWAP" or e.get("dex_name")]
        for edge in swap_edges:
            sig      = edge.get("signature") or "N/A"
            ts       = edge.get("timestamp_human", "desconhecido")
            amt      = edge.get("amount", "?")
            dex_name = edge.get("dex_name") or "DEX"
            frm      = edge.get("from", "?")
            lines.append(f"**Swap identificado:**")
            lines.append(f"- Carteira que executou o swap: `{frm}`")
            lines.append(f"- DEX utilizada: **{dex_name}**")
            lines.append(f"- Valor de entrada: {amt}")
            lines.append(f"- TX: `{sig}`")
            lines.append(f"- Timestamp: {ts}")
            lines.append(f"- Token de saída: **desconhecido** (verificar TX no Solscan/SolanaFM)\n")

        lines += [
            "**Próximos passos para rastrear após o swap:**",
            "1. Abrir a TX no **[Solscan](https://solscan.io)** ou **[SolanaFM](https://solana.fm)** e verificar qual token foi recebido",
            "2. Iniciar nova investigação com o token e carteira de saída do swap",
            "3. Se o token de saída for uma stablecoin (USDC/USDT), risco de depósito em CEX é alto",
        ]

        return "\n".join(lines)

    def _build_entities_table(self, nodes: list) -> str:
        header = "| Endereço | Tipo | Label | Risco | Hop |\n|----------|------|-------|-------|-----|\n"
        rows   = []
        for node in nodes:
            addr  = node["id"]
            ntype = node.get("type", "WALLET")
            label = node.get("label", "Desconhecido")

            if node.get("is_cex"):
                risk = "HIGH ⚠️"
            elif node.get("is_bridge"):
                risk = "MEDIUM 🌉"
            elif node.get("is_dex") or ntype == "DEX_SWAP":
                risk = "LOW 🔄"
                ntype = "DEX_SWAP 🔄"
            elif node.get("is_defi"):
                risk = "LOW 🏦"
            else:
                risk = "LOW"

            rows.append(f"| `{addr}` | {ntype} | {label} | {risk} | {node.get('depth', 0)} |")
        return header + "\n".join(rows) if rows else header + "| — | — | — | — | — |"

    def _build_timeline(self, edges: list, nodes: list) -> str:
        node_labels = {n["id"]: n.get("label", "?") for n in nodes}
        events = []
        for edge in edges:
            ts     = edge.get("timestamp") or 0
            ts_h   = edge.get("timestamp_human", "desconhecido")
            sig    = edge.get("signature") or "?"
            frm    = edge.get("from") or "?"
            to_id  = edge.get("to")   or "?"
            amt    = edge.get("amount", "?")
            to_lbl = node_labels.get(to_id, "?")
            t_type = edge.get("transfer_type", "TRANSFER")
            dex_n  = edge.get("dex_name", "")

            if t_type == "DEX_SWAP" and dex_n:
                action = f"SWAP via {dex_n} | entrada: {amt}"
            else:
                action = f"Valor: {amt}"

            events.append((ts, f"- **{ts_h}**\n"
                           f"  - De: `{frm}`\n"
                           f"  - Para: `{to_id}` ({to_lbl})\n"
                           f"  - {action}\n"
                           f"  - TX: `{sig}`"))

        events.sort(key=lambda x: x[0])
        return "\n".join(e[1] for e in events) if events else "_Nenhum evento temporal encontrado._"

    def _build_metrics_table(self, features: dict, edges: list, dex_detected: list) -> str:
        total_volume = sum(float(e.get("amount", 0) or 0) for e in edges if e.get("amount") is not None)
        dex_str = f"Sim 🔄 ({', '.join(dex_detected)})" if dex_detected else "Não"
        return (
            f"| Métrica | Valor |\n|---------|-------|\n"
            f"| Hops rastreados | {features['num_hops']} |\n"
            f"| Wallets únicas | {features['num_wallets']} |\n"
            f"| Fluxos (arestas) | {len(edges)} |\n"
            f"| Splits detectados (1→N) | {features['num_splits']} |\n"
            f"| Merges detectados (N→1) | {features['num_merges']} |\n"
            f"| Entropia de valores | {features['value_entropy']:.4f} bits |\n"
            f"| Cobertura de labels | {features['label_coverage_ratio']*100:.1f}% |\n"
            f"| Bridge/cross-chain | {'Sim 🌉' if features['has_bridge'] else 'Não'} |\n"
            f"| DEX swap detectado | {dex_str} |\n"
            f"| Volume total rastreado | {total_volume:.4f} |\n"
        )

    def _build_hypothesis(self, cex_detected, bridge_detected, dex_detected, nodes, features) -> str:
        if cex_detected:
            return (
                f"Os fundos foram transferidos para endereço(s) de depósito em "
                f"**{', '.join(cex_detected)}**, com fluxo de {features['num_hops']} hop(s). "
                f"Alta probabilidade de rastreabilidade via KYC da exchange."
            )
        if dex_detected:
            return (
                f"Os fundos foram enviados para swap via **{', '.join(dex_detected)}** — "
                f"técnica utilizada para converter o token original e dificultar o rastreamento. "
                f"O token de saída do swap é desconhecido sem análise adicional da TX. "
                f"Recomenda-se verificar o token recebido e iniciar novo rastreamento a partir da carteira de saída."
            )
        if bridge_detected:
            return (
                f"Os fundos foram encaminhados para **{', '.join(bridge_detected)}**. "
                f"O destino final pode estar em outra blockchain."
            )
        unknown = [n for n in nodes if n.get("depth", 0) > 0 and n.get("label") in ("Wallet Destino", "Wallet Final", None, "", "Desconhecido")]
        if unknown:
            return f"Os fundos estão em {len(unknown)} carteira(s) intermediária(s) sem label conhecida. Monitoramento contínuo recomendado."
        return "Fluxo rastreado. Nenhuma CEX, DEX ou bridge identificada."

    def _build_next_steps(self, cex_detected, bridge_detected, dex_detected, features) -> str:
        steps = []
        if cex_detected:
            steps = [
                f"1. Enviar template de contato formal para **{', '.join(cex_detected)}** solicitando bloqueio preventivo.",
                "2. Preservar todos os hashes de transação como evidência forense.",
                "3. Registrar boletim de ocorrência citando os endereços e TXs identificados.",
                "4. Monitorar o endereço de depósito para novas movimentações.",
                "5. Considerar acionar autoridades locais e/ou INTERPOL.",
            ]
        elif dex_detected:
            steps = [
                f"1. Abrir a TX de swap no **[Solscan](https://solscan.io)** e identificar qual token foi recebido na saída do swap.",
                f"2. Identificar a carteira que recebeu o token de saída.",
                f"3. Iniciar nova investigação SolTrace com o **novo token** e a **carteira de saída**.",
                f"4. Se o token de saída for USDC/USDT/SOL, risco de depósito em CEX é elevado — monitorar.",
                f"5. Considerar escalar para análise de IA para rastreamento multi-token.",
            ]
        elif bridge_detected:
            steps = [
                f"1. Rastrear fundos na chain de destino da bridge: **{', '.join(bridge_detected)}**.",
                "2. Contatar a equipe da bridge com as evidências on-chain.",
                "3. **Escalar para análise de IA** para correlação cross-chain.",
            ]
        else:
            steps = [
                "1. Monitorar as carteiras intermediárias para novas movimentações.",
                "2. Verificar endereços no Solscan/SolanaFM para identificar entidades.",
            ]
            if features.get("num_hops", 0) >= 3:
                steps.append("3. Considerar análise de IA para hops adicionais.")
            else:
                steps.append("3. Aguardar novas transações antes de escalar.")

        return "\n".join(steps)
