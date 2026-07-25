## MODIFIED Requirements

### Requirement: Cross-engine correctness hash

The runner SHALL compute a canonical hash of each query's result set on every engine and surface any divergence to the report. The canonical form MUST:
- represent NULL as the literal sentinel `\N`,
- format floating-point values to a fixed number of **significant digits**, chosen to sit inside the precision f64 actually carries,
- format timestamps as RFC 3339 in UTC,
- sort rows lexicographically by their canonical row representation before hashing,
- produce the hash via SHA-256 over the joined canonical rows.

Floating-point comparison MUST be scale-invariant: the same relative difference between two values MUST be judged the same way regardless of their magnitude. Fixed-decimal formatting does not satisfy this — six fractional digits on a value near `5.7e10` demands seventeen significant digits, more than an f64 holds, so the comparison becomes strictly impossible to satisfy at large magnitudes while staying lax at small ones.

The chosen precision MUST absorb summation-order noise. Two engines that partition an aggregate differently sum the same column in a different order and, by the non-associativity of floating-point addition, produce results differing in the final unit in the last place. That is not a correctness difference and MUST NOT be reported as one. The precision MUST still expose genuine computation errors, which are orders of magnitude larger than one part in the last representable digit.

Rounding alone cannot decide this. Any comparison built on "round, then compare the text" has rounding boundaries, and two values straddling one are reported as different however closely they agree — TPC-H q09 produced `309901366.4294996` against `309901366.4295008`, agreeing to fifteen significant digits, yet the twelfth digit rounds up on one side and down on the other. Reducing the precision only relocates the boundary.

The runner MUST therefore persist a second hash at a coarser precision, and the report MUST use it to adjudicate: when the strict hashes differ but the coarse hashes agree, the query MUST be reported as a rounding-boundary artifact with an explanation, NOT as a correctness divergence. When both differ, it MUST be reported as a correctness divergence.

The runner MUST compute both hashes from the first measurement run of each query (not warmup) and persist them in the result JSON.

#### Scenario: Two engines agree on a query

- **WHEN** Arneb and DataFusion both run `q06` and produce equivalent result sets up to canonicalization
- **THEN** the persisted SHA-256 digests for `q06` are identical for both engines

#### Scenario: Two engines disagree on a query

- **WHEN** Arneb and Trino produce result sets that differ after canonicalization for `q11`
- **THEN** the persisted SHA-256 digests differ and the divergence is exposed to the report layer

#### Scenario: Summation-order noise is not a divergence

- **WHEN** two engines compute the same `SUM` over a large column and their results differ only in the final unit in the last place, because they summed the rows in a different order
- **THEN** their canonical forms are identical and no divergence is reported

#### Scenario: Comparison strictness does not depend on magnitude

- **WHEN** two values differing by a given relative amount are compared, and another pair differing by the same relative amount at a magnitude ten orders larger is compared
- **THEN** both pairs are judged the same way — either both divergent or both equal

#### Scenario: A real computation error still diverges

- **WHEN** two engines produce results differing by more than the canonical precision retains
- **THEN** their canonical forms differ and the divergence is reported

#### Scenario: A rounding boundary is not reported as a divergence

- **WHEN** two engines produce values that agree well beyond the strict precision but straddle a rounding boundary in its last retained digit, so the strict hashes differ and the coarse hashes agree
- **THEN** the report describes the query as a floating-point boundary artifact and does not list it as a correctness divergence

#### Scenario: A real difference survives adjudication

- **WHEN** two engines produce genuinely different values, so both the strict and the coarse hashes differ
- **THEN** the report lists the query as a correctness divergence

### Requirement: Skipped queries are first-class

The runner SHALL accept a structured per-engine skip list (e.g., declaring that an engine cannot run a query today because it requires an unsupported SQL feature). A skipped query MUST be recorded in the result JSON with `status: "skipped"`, an explanatory `reason`, and no run timings, and MUST NOT block other engines from executing the same query.

Every skip entry MUST be justified by an observed failure of that query on that engine, and the recorded reason MUST describe that observed failure. Entries derived from reading the SQL rather than executing it are prohibited: an unverified entry silently removes a query from the comparison and publishes a fabricated reason for doing so, which is worse than having no skip list at all.

#### Scenario: An engine skips a query another engine runs

- **WHEN** the skip list declares a verified skip for one engine on `q21`
- **THEN** that engine's result for `q21` is recorded as skipped with the observed reason, while the other engines still execute `q21` normally and record their statistics

#### Scenario: A skip entry is not backed by an observed failure

- **WHEN** a query declared skipped for an engine is executed against that engine and succeeds
- **THEN** the entry is removed, because the comparison was silently excluding a query the engine can run
