"""
SolTrace - Orquestrador de Investigação.
Decide automaticamente entre análise Rule-Based (BOT) e Agente de IA,
com base em features extraídas do grafo de transações.

Critério técnico de decisão:
  - num_hops         → complexidade de profundidade
  - num_splits       → dispersão (1→N significativo)
  - num_merges       → convergência (N→1 com origens distintas)
  - value_entropy    → fragmentação/uniformidade de valores
  - has_bridge       → interação cross-chain
  - label_coverage   → proporção de entidades conhecidas

Use BOT  → fluxo simples, poucos hops, sem ofuscação relevante
Use AI   → alta entropia, splits, merges, bridge, padrões suspeitos
"""

import os
from typing import Optional

from bot_analyzer import BotAnalyzer
from agent import SolTraceAgent


# ─────────────────────────────────────────────────────────────────────────────
# Decision thresholds
# Adjust these values to control sensitivity of BOT vs AI routing.
# ─────────────────────────────────────────────────────────────────────────────

THRESHOLDS = {
    # Escalate to AI if hops exceed this
    "max_hops_for_bot": 3,
    # Escalate to AI if there are more than this many 1→N split nodes
    "max_splits_for_bot": 1,
    # Escalate to AI if there are more than this many N→1 merge nodes
    "max_merges_for_bot": 1,
    # Escalate to AI if Shannon entropy of transfer amounts exceeds this (bits)
    "max_value_entropy_for_bot": 2.0,
    # Escalate to AI if label coverage is below this ratio.
    # Typical theft: [Victim(labeled), Wallet?(unknown), Wallet?(unknown)]
    # = 1/3 = 33% coverage — should escalate to AI for deeper forensic analysis.
    # Only skip AI when most nodes are known entities (CEX/bridge found directly).
    "min_label_coverage_for_bot": 0.60,
    # Always escalate to AI if bridge/cross-chain interaction is detected
    "escalate_on_bridge": True,
}


class InvestigationOrchestrator:
    """
    Routes investigation requests to BotAnalyzer (rule-based, free)
    or SolTraceAgent (AI, costs tokens) based on graph complexity features.
    """

    def __init__(self):
        self.bot = BotAnalyzer()
        self.agent = SolTraceAgent()

    # ─────────────────────────────────────────────────────────────────────────
    # Routing decision
    # ─────────────────────────────────────────────────────────────────────────

    def should_use_ai(self, features: dict) -> tuple[bool, str]:
        """
        Evaluates the feature vector and returns (use_ai, reason).

        Returns:
            use_ai  (bool): True if AI agent should be used, False if BOT suffices
            reason  (str):  Human-readable explanation of the routing decision
        """
        escalation_reasons: list[str] = []

        if features["num_hops"] > THRESHOLDS["max_hops_for_bot"]:
            escalation_reasons.append(
                f"alto número de hops ({features['num_hops']} > {THRESHOLDS['max_hops_for_bot']})"
            )

        if features["num_splits"] > THRESHOLDS["max_splits_for_bot"]:
            escalation_reasons.append(
                f"splits significativos ({features['num_splits']} nós com padrão 1→N)"
            )

        if features["num_merges"] > THRESHOLDS["max_merges_for_bot"]:
            escalation_reasons.append(
                f"merges complexos ({features['num_merges']} nós com padrão N→1)"
            )

        if features["value_entropy"] > THRESHOLDS["max_value_entropy_for_bot"]:
            escalation_reasons.append(
                f"alta entropia de valores ({features['value_entropy']:.2f} bits — "
                f"fragmentação não uniforme detectada)"
            )

        if THRESHOLDS["escalate_on_bridge"] and features["has_bridge"]:
            escalation_reasons.append(
                "interação cross-chain detectada (bridge) — correlação probabilística necessária"
            )

        if features["label_coverage_ratio"] < THRESHOLDS["min_label_coverage_for_bot"]:
            escalation_reasons.append(
                f"baixa cobertura de labels ({features['label_coverage_ratio']*100:.1f}% < "
                f"{THRESHOLDS['min_label_coverage_for_bot']*100:.0f}%) — "
                f"muitas carteiras sem identidade conhecida"
            )

        use_ai = len(escalation_reasons) > 0

        if use_ai:
            reason = "⚡ Escalando para Agente IA — " + "; ".join(escalation_reasons)
        else:
            reason = (
                f"✅ Usando BOT rule-based — fluxo simples "
                f"({features['num_hops']} hop(s), "
                f"{features['num_splits']} split(s), "
                f"entropy={features['value_entropy']:.2f})"
            )

        return use_ai, reason

    # ─────────────────────────────────────────────────────────────────────────
    # Main routing entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def route(
        self,
        wallet: str,
        token: str,
        amount: float,
        token_name: Optional[str],
        transactions: list[dict],
        flow_graph: dict,
    ) -> dict:
        """
        Computes features, decides BOT vs AI, and executes the appropriate analysis.
        Always returns the same dict structure regardless of which path was taken.
        """
        # Step 1: Compute graph features
        features = self.bot.compute_features(flow_graph, transactions)

        # Step 2: Routing decision
        use_ai, routing_reason = self.should_use_ai(features)

        print(f"[Orchestrator] Features: {features}")
        print(f"[Orchestrator] {routing_reason}")

        # Step 3: Execute analysis
        if use_ai:
            if not os.getenv("GROQ_API_KEY"):
                # Graceful fallback: sem API key, usa BOT com aviso
                print("[Orchestrator] ⚠️  GROQ_API_KEY não configurada — forçando análise BOT.")
                print("[Orchestrator]    Adicione GROQ_API_KEY no arquivo .env para habilitar a IA.")
                routing_reason += " [AVISO: IA indisponível — GROQ_API_KEY ausente no .env. Configure para habilitar análise por Agente de IA]"
                use_ai = False
            else:
                print(f"[Orchestrator] ✅ Escalando para Agente IA (GROQ model: {os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')})")

        if use_ai:
            report = await self.agent.investigate(
                wallet=wallet,
                token=token,
                amount=amount,
                token_name=token_name,
                transactions=transactions,
                flow_graph=flow_graph,
            )
            # Inject orchestrator metadata into the report
            report["metadata"]["analysis_method"] = "ai_agent"
            report["metadata"]["features"] = features
            report["metadata"]["routing_reason"] = routing_reason
        else:
            report = self.bot.analyze(
                wallet=wallet,
                token=token,
                amount=amount,
                token_name=token_name,
                transactions=transactions,
                flow_graph=flow_graph,
                features=features,
            )
            report["metadata"]["routing_reason"] = routing_reason

        return report
