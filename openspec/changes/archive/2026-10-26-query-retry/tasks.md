## 1. Coordinator

- [x] 1.1 Split `QueryCoordinator::execute` into `execute_once`, whose failure carries the run's worker ids
- [x] 1.2 `with_query_retry`: re-run on a lost worker of the failed run, attempt limit, backoff, no worker left; skip writes; unit tests over scripted runs
- [x] 1.3 `arneb_query_retries_total` counter and `retrying query after worker loss` log

## 2. Config

- [x] 2.1 `[cluster] retry_policy` / `query_retry_attempts` + env overrides and validation; tests
- [x] 2.2 Coordinator applies and logs the effective values

## 3. Tier 2

- [x] 3.1 `fault_injection.sh` `RETRY=1` mode; bench compose passes `ARNEB_RETRY_POLICY`
- [x] 3.2 Run it with and without `RETRY=1` on the local SF10 bench cluster; record the output in `measurements/`

## 4. Docs

- [x] 4.1 `configuration.md` (`[cluster]` retry fields), `distributed.md` (Query retry), `observability.md` (counter), roadmap
