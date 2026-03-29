## ADDED Requirements

### Requirement: JoinHashMap provides efficient key-to-rows lookup
The system SHALL implement a JoinHashMap that maps hashed join key values to lists of (batch_index, row_index) pairs from the build side.

#### Scenario: Building hash table
- **WHEN** build-side batches are inserted into JoinHashMap
- **THEN** each row's join key columns SHALL be hashed and the row's location stored

#### Scenario: Probing hash table
- **WHEN** a probe-side row's key is looked up in JoinHashMap
- **THEN** it SHALL return all build-side rows with matching key values

#### Scenario: No match found
- **WHEN** a probe-side row's key has no entries in the hash table
- **THEN** the lookup SHALL return an empty result
