"""
SolTrace - Bot Analisador Rule-Based.
Handles simple, deterministic investigation cases without AI costs.

BOT is used when:
- num_hops <= 5
- num_splits <= 1 (linear or near-linear flow)
- num_merges <= 1
- value_entropy <= 2.0 bits
- no bridge/cross-chain interaction
- label_coverage_ratio >= 0.3
"""

import math
from typing import Optional
from datetime import datetime
from collections import defaultdict


class BotAnalyzer:
    """
    Rule-based analyzer for simple fund flow investigation.
    Uses BFS/DFS traversal, deterministic heuristics, and known label matching.
    No AI/LLM calls — zero token cost.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # Feature computation (used by Orchestrator for routing decision)
    # ─────────────────────────────────────────────────────────────────────────

    def compute_features(self, flow_graph: dict, transactions: list[dict]) -> dict:
        """
        Computes decision features from the graph and transactions.
        These features are used by the Orchestrator to decide BOT vs AI.
        """
        nodes = flow_graph.get("nodes", [])
        edges = flow_graph.get("edges", [])

        num_hops = flow_graph.get("summary", {}).get("max_depth", 0)
        num_wallets = len(nodes)
        has_bridge = len(flow_graph.get("bridge_detected", [])) > 0

        # Count splits: source nodes with multiple outgoing edges (1→N)
        out_degree: dict[str, int] = defaultdict(int)
        # Count merges: destination nodes with multiple distinct source nodes (N→1)
        in_sources: dict[str, set] = defaultdict(set)

        for edge in edges:
            frm = edge.get("from", "")
            to = edge.get("to", "")
            if frm:
                out_degree[frm] += 1
            if to and frm:
                in_sources[to].add(frm)

        num_splits = sum(1 for count in out_degree.values() if count > 1)
        num_merges = sum(1 for sources in in_sources.values() if len(sources) > 1)

        # Value entropy (Shannon entropy on normalized transfer amounts)
        amounts = []
        for edge in edges:
            amt = edge.get("amount")
            if amt is not None:
                try:
                    amounts.append(float(amt))
                except (TypeError, ValueError):
                    pass

        value_entropy = 0.0
        if len(amounts) > 1:
            total = sum(amounts)
            if total > 0:
                probs = [a / total for a in amounts]
                value_entropy = -sum(p * math.log2(p) for p in probs if p > 0)

        # Label coverage: ratio of nodes with a recognized label vs unknown wallets
        labeled_nodes = 0
        for n in nodes:
            label = n.get("label", "")
            is_labeled = (
                n.get("is_cex")
                or n.get("is_bridge")
                or n.get("type") in ("CEX", "BRIDGE", "DEFI", "DEX")
                or (label and label not in ("Wallet Destino", "Wallet Final", "Unknown", ""))
            )
            if is_labeled:
                labeled_nodes += 1

        label_coverage_ratio = labeled_nodes / max(len(nodes), 1)

        return {
            "num_hops": num_hops,
            "num_wallets": num_wallets,
            "num_splits": num_splits,
            "num_merges": num_merges,
            "value_entropy": round(value_entropy, 4),
            "has_bridge": has_bridge,
            "label_coverage_ratio": round(label_coverage_ratio, 4),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Main analysis entry point
    # ─────────────────────────────────────────────────────────────────────────

    def analyze(
        self,
        wallet: str,
        token: str,
        amount: float,
        token_name: Optional[str],
        transactions: list[dict],
        flow_graph: dict,
        features: dict,
    ) -> dict:
        """
        Generates a deterministic, rule-based investigation report.
        Returns the same dict structure as SolTraceAgent.investigate().
        """
        cex_detected = flow_graph.get("cex_detected", [])
        bridge_detected = flow_graph.get("bridge_detected", [])
        nodes = flow_graph.get("nodes", [])
        edges = flow_graph.get("edges", [])

        token_display = token_name or token
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        exec_summary, confidence = self._executive_summary(
            cex_detected, bridge_detected, nodes, edges, features, token_display
        )
        ascii_map = self._build_ascii_map(wallet, nodes, edges)
        cex_section = self._build_cex_section(
            cex_detected, edges, nodes, wallet, token_display, amount, now
        )
        entities_table = self._build_entities_table(nodes)
        timeline = self._build_timeline(edges, nodes)
        metrics_table = self._build_metrics_table(features, edges)
        hypothesis = self._build_hypothesis(cex_detected, bridge_detected, nodes, features)
        next_steps = self._build_next_steps(cex_detected, bridge_detected, features)

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
- Valores podem ter sido convertidos via DEX antes do destino final

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
                "wallet": wallet,
                "token": token_display,
                "token_mint": token,
                "amount": amount,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "model": "rule-based-bot",
                "analysis_method": "bot",
                "features": features,
                "transactions_analyzed": len(transactions),
                "graph_nodes": len(nodes),
                "graph_edges": len(edges),
                "cex_detected": cex_detected,
                "bridge_detected": bridge_detected,
            },
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Report section builders
    # ─────────────────────────────────────────────────────────────────────────

    def _executive_summary(
        self, cex_detected, bridge_detected, nodes, edges, features, token_display
    ) -> tuple[str, str]:
        if cex_detected:
            summary = (
                f"Fundos rastreados com **ALTA CONFIANÇA** via análise determinística. "
                f"Detectado depósito em CEX: **{', '.join(cex_detected)}**. "
                f"Fluxo de {features['num_hops']} hop(s), sem padrões complexos de ofuscação. "
                f"Recomenda-se contato imediato com a exchange para bloqueio preventivo."
            )
            return summary, "HIGH"

        if bridge_detected:
            summary = (
                f"Fundos encaminhados para bridge/protocolo cross-chain: **{', '.join(bridge_detected)}**. "
                f"Rastreamento em {features['num_hops']} hop(s). "
                f"Destino final pode estar em outra blockchain — análise de IA recomendada para correlação cross-chain."
            )
            return summary, "MEDIUM"

        dest_nodes = [n for n in nodes if n.get("depth", 0) > 0]
        if dest_nodes:
            labels = list(dict.fromkeys(
                n.get("label", "desconhecida")
                for n in dest_nodes
                if n.get("label") not in ("Wallet Destino", "Wallet Final", None, "")
            ))
            label_str = f"Entidades: {', '.join(labels[:3])}. " if labels else ""
            summary = (
                f"Fundos rastreados por {features['num_hops']} hop(s) para "
                f"{len(dest_nodes)} carteira(s). {label_str}"
                f"Fluxo linear sem padrões complexos de ofuscação detectados pelo BOT."
            )
            return summary, "MEDIUM"

        summary = (
            f"Nenhuma transferência de saída identificada para **{token_display}** "
            f"nas transações analisadas. Os fundos podem permanecer na carteira vítima "
            f"ou o token não consta nas transações coletadas."
        )
        return summary, "LOW"

    def _build_ascii_map(self, wallet: str, nodes: list, edges: list) -> str:
        lines = []
        lines.append(f"[Vítima: {wallet}]")

        # Group nodes by depth
        depth_nodes: dict[int, list] = defaultdict(list)
        for n in nodes:
            if n["id"] != wallet:
                depth_nodes[n.get("depth", 1)].append(n)

        # Map destination id → edge info (first edge reaching this node)
        edge_map: dict[str, dict] = {}
        for edge in edges:
            to = edge.get("to", "")
            if to and to not in edge_map:
                edge_map[to] = edge

        for depth in sorted(depth_nodes.keys()):
            for node in depth_nodes[depth]:
                node_id = node["id"]
                label = node.get("label", "Desconhecido")
                cex_flag = " ⚠️ [CEX]" if node.get("is_cex") else ""
                bridge_flag = " 🌉 [BRIDGE]" if node.get("is_bridge") else ""
                indent = "    " * (depth - 1)

                edge = edge_map.get(node_id, {})
                amt = edge.get("amount", "?")
                sig = edge.get("signature") or "?"
                lines.append(f"{indent}        ↓ {amt}")
                lines.append(f"{indent}        TX: {sig}")
                lines.append(f"{indent}[{label}: {node_id}]{cex_flag}{bridge_flag}")

        return "\n".join(lines) if len(lines) > 1 else f"[Vítima: {wallet}]\n    (sem transferências de saída identificadas)"

    def _build_cex_section(
        self, cex_detected, edges, nodes, wallet, token_display, amount, now
    ) -> str:
        if not cex_detected:
            # Show where funds ended up
            dest_ids = {e.get("to") for e in edges}
            source_ids = {e.get("from") for e in edges}
            final_ids = dest_ids - source_ids - {wallet}
            final_nodes = [n for n in nodes if n["id"] in final_ids]

            if final_nodes:
                addrs = "\n".join(
                    f"  - `{n['id']}` — {n.get('label', 'desconhecido')}"
                    for n in final_nodes[:5]
                )
                return (
                    "Nenhuma CEX identificada nas transações analisadas.\n\n"
                    f"**Endereços finais detectados:**\n{addrs}"
                )
            return (
                "Nenhuma CEX identificada. Os fundos podem ainda estar nas "
                "carteiras intermediárias ou o token não foi movimentado."
            )

        sections = []

        for cex_name in cex_detected:
            cex_nodes = [
                n for n in nodes
                if n.get("is_cex") and cex_name.lower() in n.get("label", "").lower()
            ]
            for cex_node in cex_nodes:
                deposit_edges = [e for e in edges if e.get("to") == cex_node["id"]]
                for dep_edge in deposit_edges[:1]:
                    sig = dep_edge.get("signature", "N/A")
                    ts = dep_edge.get("timestamp_human", "desconhecido")
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

Identificamos através de análise forense blockchain que fundos roubados
foram depositados em endereço associado à vossa exchange.

Detalhes da Ocorrência:
- Carteira da vítima: {wallet}
- Token roubado: {token_display}
- Valor: {amount}
- Hash da transação de depósito: {sig}
- Endereço de depósito na exchange: {cex_node['id']}
- Timestamp: {ts}

Solicitamos urgentemente:
1. Bloqueio preventivo dos fundos no endereço acima
2. Preservação de registros KYC associados
3. Cooperação com as autoridades competentes

Evidências on-chain disponíveis mediante solicitação.
```""")

        if not sections:
            return (
                f"CEX detectada: {', '.join(cex_detected)}\n"
                "(Detalhes de depósito não encontrados nas arestas do grafo — "
                "verifique o endereço diretamente no explorer)"
            )

        return "\n\n".join(sections)

    def _build_entities_table(self, nodes: list) -> str:
        header = (
            "| Endereço | Tipo | Label | Risco | Hop |\n"
            "|----------|------|-------|-------|-----|\n"
        )
        rows = []
        for node in nodes:
            addr = node["id"]
            ntype = node.get("type", "WALLET")
            label = node.get("label", "Desconhecido")
            if node.get("is_cex"):
                risk = "HIGH ⚠️"
            elif node.get("is_bridge"):
                risk = "MEDIUM 🌉"
            else:
                risk = "LOW"
            depth = node.get("depth", 0)
            rows.append(f"| `{addr}` | {ntype} | {label} | {risk} | {depth} |")

        return header + "\n".join(rows) if rows else header + "| — | — | — | — | — |"

    def _build_timeline(self, edges: list, nodes: list) -> str:
        node_labels = {n["id"]: n.get("label", "?") for n in nodes}
        events = []
        for edge in edges:
            ts = edge.get("timestamp") or 0
            ts_h = edge.get("timestamp_human", "desconhecido")
            sig = edge.get("signature") or "?"
            frm = edge.get("from") or "?"
            to_id = edge.get("to") or "?"
            amt = edge.get("amount", "?")
            to_label = node_labels.get(to_id, "?")
            events.append((
                ts,
                f"- **{ts_h}**\n"
                f"  - De: `{frm}`\n"
                f"  - Para: `{to_id}` ({to_label})\n"
                f"  - Valor: {amt}\n"
                f"  - TX: `{sig}`"
            ))

        events.sort(key=lambda x: x[0])
        return "\n".join(e[1] for e in events) if events else "_Nenhum evento temporal encontrado._"

    def _build_metrics_table(self, features: dict, edges: list) -> str:
        total_volume = sum(
            float(e.get("amount", 0) or 0)
            for e in edges
            if e.get("amount") is not None
        )
        return (
            f"| Métrica | Valor |\n"
            f"|---------|-------|\n"
            f"| Hops rastreados | {features['num_hops']} |\n"
            f"| Wallets únicas | {features['num_wallets']} |\n"
            f"| Fluxos (arestas) | {len(edges)} |\n"
            f"| Splits detectados (1→N) | {features['num_splits']} |\n"
            f"| Merges detectados (N→1) | {features['num_merges']} |\n"
            f"| Entropia de valores | {features['value_entropy']:.4f} bits |\n"
            f"| Cobertura de labels | {features['label_coverage_ratio']*100:.1f}% |\n"
            f"| Bridge/cross-chain | {'Sim 🌉' if features['has_bridge'] else 'Não'} |\n"
            f"| Volume total rastreado | {total_volume:.4f} |\n"
        )

    def _build_hypothesis(
        self, cex_detected, bridge_detected, nodes, features
    ) -> str:
        if cex_detected:
            return (
                f"Os fundos foram transferidos diretamente para endereço(s) de depósito em "
                f"**{', '.join(cex_detected)}**, com fluxo linear de {features['num_hops']} hop(s). "
                f"Alta probabilidade de rastreabilidade via KYC da exchange."
            )
        if bridge_detected:
            return (
                f"Os fundos foram encaminhados para bridge/protocolo cross-chain "
                f"(**{', '.join(bridge_detected)}**). O destino final pode estar em outra blockchain. "
                f"Correlação cross-chain é necessária para rastrear o destino final."
            )
        unknown_nodes = [
            n for n in nodes
            if n.get("depth", 0) > 0
            and n.get("label") in ("Wallet Destino", "Wallet Final", None, "")
        ]
        if unknown_nodes:
            return (
                f"Os fundos estão em {len(unknown_nodes)} carteira(s) intermediária(s) sem "
                f"label conhecida. Monitoramento contínuo desses endereços é recomendado."
            )
        return "Fluxo rastreado. Nenhuma CEX ou bridge identificada nas transações analisadas."

    def _build_next_steps(
        self, cex_detected, bridge_detected, features
    ) -> str:
        steps = []
        if cex_detected:
            steps += [
                f"1. Enviar template de contato formal para **{', '.join(cex_detected)}** solicitando bloqueio preventivo.",
                "2. Preservar todos os hashes de transação como evidência forense.",
                "3. Registrar boletim de ocorrência citando os endereços e TXs identificados.",
                "4. Monitorar o endereço de depósito para novas movimentações.",
                "5. Considerar acionar autoridades locais e/ou INTERPOL dependendo da jurisdição.",
            ]
        elif bridge_detected:
            steps += [
                f"1. Rastrear os fundos na chain de destino da bridge: **{', '.join(bridge_detected)}**.",
                "2. Contatar a equipe da bridge com as evidências on-chain.",
                "3. **Escalar para análise de IA** para correlação cross-chain probabilística.",
                "4. Verificar DEXs e CEXs na chain de destino.",
            ]
        else:
            steps += [
                "1. Monitorar as carteiras intermediárias identificadas para novas movimentações.",
                "2. Verificar se algum endereço destino pertence a entidade conhecida via explorer.",
            ]
            if features.get("num_hops", 0) >= 3:
                steps.append("3. Considerar rastrear hops adicionais com análise de IA.")
            else:
                steps.append("3. Aguardar novas transações antes de escalar a análise.")
            steps.append("4. Verificar se o token foi swapado em alguma DEX intermediária.")

        return "\n".join(steps)
