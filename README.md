# Bee Party: four-runner acceptance tests

Open source k6 on four independent standard GitHub-hosted Ubuntu runners. No Grafana Cloud. Only manually dispatched workflows run. Each runner simulates one quarter of the total players; maximum 200 per runner, 800 total.

This repository contains the test driver and encrypted synthetic fixtures, not application source or plaintext credentials. AES-256-GCM keys live in GitHub Actions secrets. Test room and short-lived staging session credentials are encrypted in the fixture, masked in logs, and revoked after verification. Raw metric points and execution evidence are encrypted before upload; artifact retention is one day. Tests are restricted to the isolated staging environment.

The controller waits for every player and verifies four distinct source addresses with equal player counts before starting the game. Twenty percent of players disconnect for five seconds halfway through the round, then reconnect. The client uses compact protocol 3, 20 Hz simulation input, immutable retries, a 60-tick pending limit, and up to 30 ticks per coalesced packet. Final scores, rankings, and persisted history are independently checked against precomputed legal input trajectories.

All original 27 acceptance conditions are evaluated over the combined raw samples after download. Percentiles are calculated from pooled samples, never averaged across runners. A successful workflow only means evidence collection completed; acceptance results are determined separately. Missing or incomplete shards cannot pass.

Standard runners for public repositories are free under GitHub's Actions billing policy. This workflow does not use larger runners, paid add-ons, or a cloud load-testing service.
