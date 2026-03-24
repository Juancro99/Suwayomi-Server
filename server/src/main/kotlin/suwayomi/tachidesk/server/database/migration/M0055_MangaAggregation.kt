package suwayomi.tachidesk.server.database.migration

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import de.neonew.exposed.migrations.helpers.SQLMigration

@Suppress("ClassName", "unused")
class M0055_MangaAggregation : SQLMigration() {
    override val sql =
        """
        CREATE TABLE IF NOT EXISTS works (
            id UUID PRIMARY KEY,
            canonical_title VARCHAR(512) NOT NULL,
            normalized_title VARCHAR(512) NOT NULL,
            cover_url VARCHAR(2048),
            type VARCHAR(32) NOT NULL DEFAULT 'unknown',
            status VARCHAR(32) NOT NULL DEFAULT 'unknown',
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0
        );

        CREATE INDEX IF NOT EXISTS works_normalized_title_idx ON works(normalized_title);

        CREATE TABLE IF NOT EXISTS work_aliases (
            id UUID PRIMARY KEY,
            work_id UUID NOT NULL,
            alias VARCHAR(512) NOT NULL,
            normalized_alias VARCHAR(512) NOT NULL,
            source VARCHAR(32) NOT NULL,
            confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            CONSTRAINT fk_work_aliases_work FOREIGN KEY (work_id) REFERENCES works(id) ON DELETE CASCADE,
            CONSTRAINT uq_work_aliases_work_normalized_alias UNIQUE (work_id, normalized_alias)
        );

        CREATE TABLE IF NOT EXISTS source_entries (
            id UUID PRIMARY KEY,
            work_id UUID NOT NULL,
            manga_id INT NOT NULL,
            source_id BIGINT NOT NULL,
            source_name VARCHAR(128) NOT NULL,
            source_manga_id VARCHAR(2048) NOT NULL,
            source_title VARCHAR(512) NOT NULL,
            normalized_source_title VARCHAR(512) NOT NULL,
            chapters_count INT NOT NULL DEFAULT 0,
            last_chapter DOUBLE PRECISION,
            last_update BIGINT,
            language VARCHAR(32) NOT NULL,
            match_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            quality_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            raw_metadata TEXT,
            CONSTRAINT fk_source_entries_work FOREIGN KEY (work_id) REFERENCES works(id) ON DELETE CASCADE,
            CONSTRAINT fk_source_entries_manga FOREIGN KEY (manga_id) REFERENCES manga(id) ON DELETE CASCADE,
            CONSTRAINT uq_source_entries_manga UNIQUE (manga_id)
        );

        CREATE INDEX IF NOT EXISTS source_entries_normalized_source_title_idx ON source_entries(normalized_source_title);

        CREATE TABLE IF NOT EXISTS source_policy (
            source_id BIGINT PRIMARY KEY,
            source_name VARCHAR(128) NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            trusted BOOLEAN NOT NULL DEFAULT TRUE,
            rank_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            language_priority INT NOT NULL DEFAULT 0,
            install_locked BOOLEAN NOT NULL DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS merge_candidates (
            id UUID PRIMARY KEY,
            source_entry_id UUID NOT NULL,
            candidate_work_id UUID NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            CONSTRAINT fk_merge_candidates_source_entry FOREIGN KEY (source_entry_id) REFERENCES source_entries(id) ON DELETE CASCADE,
            CONSTRAINT fk_merge_candidates_candidate_work FOREIGN KEY (candidate_work_id) REFERENCES works(id) ON DELETE CASCADE,
            CONSTRAINT uq_merge_candidates_source_entry_candidate_work UNIQUE (source_entry_id, candidate_work_id)
        );
        """.trimIndent()
}
