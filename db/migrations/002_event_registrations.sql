CREATE TABLE event_registrations (
    registration_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    account_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    session_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    character_id BIGINT NOT NULL,
    lease_version BIGINT NOT NULL,
    registered_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (registration_id),
    UNIQUE KEY uq_event_registration_account (event_id, account_id),
    UNIQUE KEY uq_event_registration_character (event_id, character_id),
    CONSTRAINT fk_event_registration_event FOREIGN KEY (event_id)
        REFERENCES nexus_events (event_id) ON DELETE RESTRICT,
    CONSTRAINT ck_event_registration_character CHECK (character_id > 0),
    CONSTRAINT ck_event_registration_lease CHECK (lease_version > 0)
) ENGINE=InnoDB;

CREATE TABLE event_registration_requests (
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    account_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    idempotency_key VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_sha256 BINARY(32) NOT NULL,
    decision_json JSON NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (request_id),
    UNIQUE KEY uq_event_registration_request_idempotency (account_id, idempotency_key)
) ENGINE=InnoDB;

CREATE TABLE event_registration_audit (
    audit_id BIGINT NOT NULL AUTO_INCREMENT,
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    correlation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    account_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status TINYINT UNSIGNED NOT NULL,
    registration_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    error_code VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload_sha256 BINARY(32) NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (audit_id),
    UNIQUE KEY uq_event_registration_audit_request (request_id),
    KEY ix_event_registration_audit_event (event_id, audit_id)
) ENGINE=InnoDB;
