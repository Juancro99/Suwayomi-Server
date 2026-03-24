package suwayomi.tachidesk.manga.model.dataclass

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

data class WorkAliasDataClass(
    val id: String,
    val workId: String,
    val alias: String,
    val normalizedAlias: String,
    val source: String,
    val confidence: Double,
)

data class WorkSourceEntryDataClass(
    val id: String,
    val workId: String,
    val mangaId: Int,
    val sourceId: String,
    val sourceName: String,
    val sourceIconUrl: String?,
    val sourceMangaId: String,
    val sourceTitle: String,
    val normalizedSourceTitle: String,
    val chaptersCount: Int,
    val lastChapter: Double?,
    val lastUpdate: Long?,
    val language: String,
    val matchScore: Double,
    val qualityScore: Double,
    val thumbnailUrl: String?,
    val recommended: Boolean,
)

data class WorkDataClass(
    val id: String,
    val canonicalTitle: String,
    val normalizedTitle: String,
    val coverUrl: String?,
    val type: String,
    val status: String,
    val createdAt: Long,
    val updatedAt: Long,
    val confidenceScore: Double,
    val searchScore: Double?,
    val recommendedSourceId: String?,
    val aliases: List<WorkAliasDataClass>,
    val sources: List<WorkSourceEntryDataClass>,
)

data class WorkSearchResultDataClass(
    val query: String,
    val normalizedQuery: String,
    val works: List<WorkDataClass>,
    val searchedSources: Int,
    val matchedSources: Int,
)

data class SourcePolicyDataClass(
    val sourceId: String,
    val sourceName: String,
    val enabled: Boolean,
    val trusted: Boolean,
    val rankWeight: Double,
    val languagePriority: Int,
    val installLocked: Boolean,
)
