# Sympathy moves: tested and rejected (10.11.2026)

Question: when a $1B+ stock opens 10%+ higher, do its closest peers (the five $1B+ stocks with the highest
correlation of daily returns over the 60 sessions before, correlation 0.4+) follow during the day, so that the
bot could alert the peers that have not moved yet?

Data: a year of daily bars of every listed stock (Alpaca), `tools/sympathy_study.py`, 6,008 leader-peer pairs.
Measured from the peer's own open (the earliest an alert could act): up = +10% high within two sessions and
+5% at the close, down = -5% at the close.

| group | pairs | up | down |
|---|---|---|---|
| every $1B+ stock opening within ±3% (base rate) | 529,571 | 1.2% | 2.5% |
| peers that had not moved yet (open within ±3%) | 4,373 | 2.7% | 5.5% |
| the same, correlation 0.7+ | 947 | 2.1% | 5.8% |
| the same, leader up 15%+ at the open and correlation 0.6+ | 822 | 1.9% | 6.0% |
| peers already up 8%+ at the open | 456 | 19.5% | 17.3% |

Peers of a jumping stock move more than other stocks, but down more often than up, and the group's move is
already in the peers' opening price. No segment (correlation, leader gap, leader holding, number of leaders,
size) reached even 7% up. The bot does not alert on it.
