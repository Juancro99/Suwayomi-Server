package suwayomi.tachidesk.manga.model.table

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.sql.ReferenceOption
import org.jetbrains.exposed.sql.Table
import suwayomi.tachidesk.manga.model.table.columns.truncatingVarchar

object WorkTable : Table("works") {
    val id = uuid("id")
    val canonicalTitle = truncatingVarchar("canonical_title", 512)
    val normalizedTitle = truncatingVarchar("normalized_title", 512).index()
    val coverUrl = varchar("cover_url", 2048).nullable()
    val type = varchar("type", 32).default("unknown")
    val status = varchar("status", 32).default("unknown")
    val createdAt = long("created_at")
    val updatedAt = long("updated_at")
    val confidenceScore = double("confidence_score").default(0.0)

    override val primaryKey = PrimaryKey(id)
}

object WorkAliasTable : Table("work_aliases") {
    val id = uuid("id")
    val workId = reference("work_id", WorkTable.id, onDelete = ReferenceOption.CASCADE)
    val alias = truncatingVarchar("alias", 512)
    val normalizedAlias = truncatingVarchar("normalized_alias", 512)
    val aliasSource = varchar("source", 32)
    val confidence = double("confidence").default(0.0)

    init {
        uniqueIndex("work_aliases_work_id_normalized_alias_uindex", workId, normalizedAlias)
    }

    override val primaryKey = PrimaryKey(id)
}

object WorkSourceEntryTable : Table("source_entries") {
    val id = uuid("id")
    val workId = reference("work_id", WorkTable.id, onDelete = ReferenceOption.CASCADE)
    val mangaId = reference("manga_id", MangaTable.id, onDelete = ReferenceOption.CASCADE).uniqueIndex()
    val sourceId = long("source_id")
    val sourceName = varchar("source_name", 128)
    val sourceMangaId = varchar("source_manga_id", 2048)
    val sourceTitle = truncatingVarchar("source_title", 512)
    val normalizedSourceTitle = truncatingVarchar("normalized_source_title", 512).index()
    val chaptersCount = integer("chapters_count").default(0)
    val lastChapter = double("last_chapter").nullable()
    val lastUpdate = long("last_update").nullable()
    val language = varchar("language", 32)
    val matchScore = double("match_score").default(0.0)
    val qualityScore = double("quality_score").default(0.0)
    val rawMetadata = text("raw_metadata").nullable()

    override val primaryKey = PrimaryKey(id)
}

object SourcePolicyTable : Table("source_policy") {
    val sourceId = long("source_id")
    val sourceName = varchar("source_name", 128)
    val enabled = bool("enabled").default(true)
    val trusted = bool("trusted").default(true)
    val rankWeight = double("rank_weight").default(0.0)
    val languagePriority = integer("language_priority").default(0)
    val installLocked = bool("install_locked").default(false)

    override val primaryKey = PrimaryKey(sourceId)
}

object MergeCandidateTable : Table("merge_candidates") {
    val id = uuid("id")
    val sourceEntryId = reference("source_entry_id", WorkSourceEntryTable.id, onDelete = ReferenceOption.CASCADE)
    val candidateWorkId = reference("candidate_work_id", WorkTable.id, onDelete = ReferenceOption.CASCADE)
    val score = double("score")
    val status = varchar("status", 32).default("pending")

    init {
        uniqueIndex("merge_candidates_source_entry_candidate_work_uindex", sourceEntryId, candidateWorkId)
    }

    override val primaryKey = PrimaryKey(id)
}
