# 0008: Keep repositories current with an incremental background sync

- Status: Accepted
- Date: 2026-11-20

## Context

The live poller of [ADR 0007](0007-server-side-live-run-polling.md) keeps the runs of repositories that were already synced current, but everything else only changed when a user pressed Sync: new repositories, new or changed workflows, repository metadata, and scores. In practice people pressed Sync every day or two, and each press was a full pass: per repository it listed workflows, listed 50 runs, and recomputed the score, which alone costs around ten GitHub requests. For a few hundred repositories that takes minutes and a large part of the rate-limit budget, to learn that almost nothing changed.

The sync progress bar was also unreliable. The browser kept only the newest WebSocket message, so events arriving in the same tick overwrote each other; a rejected start request, or a lost `sync:complete`, left the overlay on "Syncing" forever; and the reset timer of a finished sync could wipe the next one.

## Decision

The backend runs an incremental sync for users who are watching, every `AUTO_SYNC_INTERVAL` (default 30m, minimum 5m, `0` disables). It qualifies a user only after they synced at least one repository themselves, so an account is never imported without the first Sync press. The sync is silent: no progress overlay, and one `sync:refresh` WebSocket event when it stored something so open views refetch.

For a repository the instance already stores, the incremental sync lists workflows and the 50 newest runs with `If-None-Match`; a `304` costs no rate-limit budget and writes nothing, and an ETag is remembered only after every matching write succeeded. A repository that is new to the instance gets the full treatment. A score is recomputed only when it is missing, older than 7 days, or the repository was pushed to since and the score is at least 6 hours old. The manual Sync button stays a full re-import and is the escape hatch.

Syncs are tracked per user: a second sync cannot start while one runs (`409`), and `GET /api/repositories/sync/status` reports the state. The browser consumes WebSocket events through a subscription that delivers every message in order, follows the status endpoint while a sync is shown so a lost event cannot strand the overlay, resumes the progress after a page reload, shows failures, and never lets one sync's reset timer cancel the next sync.

## Alternatives considered

- Shorten the manual sync only. Rejected: it would still be a manual chore, and most of its cost is re-reading unchanged repositories.
- A timer that fully syncs every user, watching or not. Rejected for the reason ADR 0007 gives: it spends rate-limit budget nobody benefits from.
- Detect changes with `pushed_at` instead of ETags. Rejected: it needs a schema change and misses workflow changes made through the API; ETags are exact and already used by the poller.
- Persist the last sync time and the ETags. Deferred: after a restart the first automatic sync re-reads every repository once, which is one cheap pass, and a schema change is not justified yet.

## Consequences

New repositories, workflow changes and fresh runs appear within `AUTO_SYNC_INTERVAL` of somebody opening the dashboard, so the Sync button is rarely needed. GitHub traffic of an automatic sync is bounded by the number of changed repositories plus one listing request per 100 repositories, and is zero when nobody is looking. A repository that is no longer listed by GitHub is not removed, as before. The tracker and the ETags live in memory, so each backend restart costs one extra pass.
