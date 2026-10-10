CREATE TABLE nexus_events (
    event_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    revision BIGINT NOT NULL,
    phase TINYINT UNSIGNED NOT NULL,
    registration_opens_utc DATETIME(6) NOT NULL,
    registration_closes_utc DATETIME(6) NOT NULL,
    starts_utc DATETIME(6) NOT NULL,
    ends_utc DATETIME(6) NOT NULL,
    maximum_participants SMALLINT UNSIGNED NOT NULL,
    target_system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ruleset_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    CONSTRAINT ck_nexus_events_revision CHECK (revision > 0),
    CONSTRAINT ck_nexus_events_participants CHECK (maximum_participants BETWEEN 1 AND 200)
) ENGINE=InnoDB;

CREATE TABLE event_definition_audit (
    audit_id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_service VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    definition_revision BIGINT NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (audit_id),
    KEY ix_event_definition_audit_event (event_id, audit_id)
) ENGINE=InnoDB;

CREATE TABLE event_lifecycle_requests (
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    idempotency_key VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_sha256 BINARY(32) NOT NULL,
    decision_json JSON NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (request_id),
    UNIQUE KEY uq_event_lifecycle_actor_idempotency (actor_id, idempotency_key)
) ENGINE=InnoDB;

CREATE TABLE event_lifecycle_audit (
    audit_id BIGINT NOT NULL AUTO_INCREMENT,
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    correlation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status TINYINT UNSIGNED NOT NULL,
    previous_phase TINYINT UNSIGNED NULL,
    next_phase TINYINT UNSIGNED NULL,
    previous_revision BIGINT NULL,
    next_revision BIGINT NULL,
    error_code VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload_sha256 BINARY(32) NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (audit_id),
    UNIQUE KEY uq_event_lifecycle_audit_request (request_id),
    KEY ix_event_lifecycle_audit_event (event_id, audit_id)
) ENGINE=InnoDB;
