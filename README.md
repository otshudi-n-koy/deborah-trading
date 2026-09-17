# Deborah Trading

Bot de trading algorithmique entièrement automatisé sur EUR/USD, basé sur la
méthodologie ICT/SMC (Smart Money Concepts). Objectif : valider des
challenges prop firm E8 et atteindre l'indépendance financière via un
trading systématique piloté par bot.

Statut au 17/09/2026 : **pipeline en exécution réelle, BUY et SELL actifs**,
sur compte E8 One 5K réel. Historique complet de trades réels depuis
mai 2026, avec plusieurs corrections majeures déployées récemment
(voir "Historique des découvertes majeures").

---

## Résumé de la stratégie

Approche SMC (Smart Money Concepts) multi-timeframe :

1. **Structure** détectée en H1, confirmée par confluence H4
2. **PD Array** (Order Block / FVG) comme zone d'entrée
3. **Zone premium/discount/OTE** calculée par rapport à l'équilibre du range
   — filtre directionnel (voir "Règles de zone")
4. **ATR minimum** 3 pips (filtre de volatilité)
5. **RR minimum** 1.2, identique pour BUY et SELL
6. Exécution via ordres LIMIT cTrader, gestion active (breakeven, trailing)

### Règles de zone (déployées 09-10/09/2026)

Principe SMC de base : acheter en zone bon marché (discount), vendre en zone
chère (premium). Deux règles symétriques actives en production :

- **BUY interdit en zone PREMIUM**
- **SELL interdit en zone DISCOUNT**

Justifiées par post-mortem réel (les pertes BUY étaient systématiquement en
PREMIUM, les pertes SELL en DISCOUNT) et confirmées par backtest complet sur
toute la période disponible :

| Config | n | WR | Kelly |
|---|---|---|---|
| BUY, sans filtre zone | 37 | 67.6% | +1.201 |
| BUY, zone≠PREMIUM | 11 | 63.6% | **+1.456** |
| SELL, sans filtre zone | 21 | 66.7% | +0.986 |
| SELL, zone≠DISCOUNT | 8 | 75.0% | **+1.441** |

---

## Architecture

```
deborah-trading/
├── scripts/                        # /opt/deborah-trading/scripts/ sur le VPS
│   ├── pd_array_detector.py         # Détection zones PD Array (OB/FVG), invalidation
│   ├── structure_analyzer.py        # Calcul structure H1/H4, biais, zone premium/discount/OTE
│   ├── signal_generator.py          # Confluence, ATR, RR, règles de zone, génération signal
│   ├── ctrader_executor_v2.py       # Soumission ordres LIMIT au broker
│   ├── position_monitor.py          # Breakeven, trailing, clôtures, circuit breakers, réconciliation
│   └── ctrader-mcp-server/          # Collecteur de prix cTrader (copie versionnée)
├── (agents Telegram)
│   ├── agent_pre_killzone.py         # Briefings déterministes avant chaque session
│   ├── agent_post_trade.py           # Analyse post-trade (bug de comptage connu, ticket #72)
│   └── agent_event_watcher.py        # Événements temps réel
└── (dashboard)
    └── roadmap-smc/                  # FastAPI + lightweight-charts.js, suivi challenge E8
```

Pipeline cron entièrement automatisé (`* * * * *` pour `signal_generator.py`
et `position_monitor.py`, `*/5 * * * *` pour la structure et les PD arrays) :

```
pd_array_detector.py → structure_analyzer.py → signal_generator.py
    → ctrader_executor_v2.py → position_monitor.py
```

---

## Comptes & infrastructure

| Élément | Valeur |
|---|---|
| Compte cTrader (exécution) | `5024540` (E8 One 5K, Opra Markets), `ctidTraderAccountId=47806214` |
| App cTrader | DeborahForex |
| Token OAuth | `config_smc` (DB `trading`), refresh mensuel automatique (1er du mois, 03h UTC) |
| DB | `trading` (conteneur Docker `postgres-trading`) |
| VPS | Hetzner 5.75.150.1, hostname `n8n-bitcoin` |
| Monitoring | Grafana, Telegram, Kanboard (`project_id=1`) |

Note : ce compte est paramétré pour ne jamais être éliminé du challenge
(risque calibré en conséquence) — un échange de compte avec le projet
`day-trading-antoine` (compte E8 25K réel) est envisagé mais non finalisé.

---

## Gouvernance opérationnelle (depuis le 06/09/2026)

Suite à la découverte d'un circuit breaker s'étant auto-bloqué 13 jours sans
détection (voir historique ci-dessous), une règle permanente a été adoptée :

1. **Post-mortem systématique** de chaque perte réelle — vérification
   factuelle en base, jamais de confiance aveugle dans un rapport d'agent
2. **Application immédiate** des correctifs validés, plutôt que d'attendre
   un grand volume de données avant d'agir
3. **Documentation continue** (Kanboard) au fil de l'eau

Discipline de patch (universelle) :
1. `assert content.count(old) == 1` avant tout remplacement de texte
2. Sauvegarde `.bak` avant modification
3. `python3 -m py_compile` (vérification syntaxique)
4. Test en conditions réelles avant commit
5. Comparaison backtest avant/après pour tout changement de paramètre

---

## Historique des découvertes majeures

| Date | Découverte | Statut |
|---|---|---|
| 18/08 | Bougie H1/H4 figée par l'API cTrader (agrégation M1 live) | Corrigé |
| 24/08 | `consecutive_losses` jamais mis à jour (branche réconciliation) | Corrigé |
| 06/09 | Circuit breaker auto-bloqué 13 jours (impasse auto-entretenue) | Désactivé, gouvernance revue |
| 07/09 | Filtre CHoCH+BOS éliminait un edge BUY réel positif (Kelly +0.513) | Retiré, BUY réactivé (risque réduit) |
| 09/09 | Règle SELL≠DISCOUNT validée le 25/08 jamais déployée en réel | Déployée |
| 09-10/09 | Règle symétrique BUY≠PREMIUM découverte et déployée | Déployé |
| 10/09 | Conflit logique invalidation de zone / remplissage d'ordre LIMIT | Corrigé (ticket #73) |

---

## État de validation

| Composant | Statut |
|---|---|
| Détection structure/PD Array | En production depuis mai 2026 |
| Exécution réelle SELL | Active, RR≥1.2, zone≠DISCOUNT |
| Exécution réelle BUY | Réactivée 07/09, risque réduit de moitié, zone≠PREMIUM |
| Circuit breaker drawdown (jour/total) | Actif, indépendant du bug du 06/09 |
| Circuit breaker `consecutive_losses` | Désactivé (impasse auto-entretenue, jamais réactivé) |
| Gestion position (breakeven/trailing) | Active |
| Annulation d'ordre pending sur invalidation de zone | Corrigée 10/09 (exige une vraie cassure, pas un simple contact) |
| Rapports post-trade automatiques (Telegram) | Comptage streak/WR non fiable (ticket #72, non corrigé) |
| Synchronisation clôtures E8 du vendredi (`ProtoOADealListReq`) | Non implémentée |

---

## Limites connues

1. **Risque BUY réduit de moitié** depuis la réactivation (07/09) — phase de
   prudence, pas encore revenu au risque plein
2. **`agent_post_trade.py`** calcule des streaks/WR erronés de façon
   récurrente (3 occurrences confirmées) — ne jamais faire confiance à ses
   chiffres sans vérification en base
3. **Aucune détection automatique** des clôtures E8 du vendredi soir
   (ManagerAPI) — `position_monitor.py` déduit les résultats uniquement par
   comparaison des bougies M5 aux SL/TP
4. **Échantillons encore modestes** sur les règles de zone récemment
   déployées (n=8 à 11 en backtest) — à surveiller en conditions réelles
5. **Stratégie de continuation/breakout** (chantier distinct) en pause,
   aucune formulation clairement rentable trouvée à ce jour

---

## Discipline de travail

- Ne jamais accepter un résultat de backtest sur petit échantillon comme
  concluant sans le signaler explicitement
- Toujours vérifier les affirmations chiffrées d'un rapport automatique
  (agent Telegram) contre la base de données avant de les citer
- Tout changement de paramètre ou de règle est backtesté avant déploiement,
  puis testé en conditions réelles avant commit
- Deux conventions de timeframe coexistent dans `prices_smc` (`5M`/`1H`/`4H`
  = archive, `M5`/`H1`/`H4` = flux live depuis le 22/05/2026) — toujours
  utiliser la seconde pour tout backtest récent
