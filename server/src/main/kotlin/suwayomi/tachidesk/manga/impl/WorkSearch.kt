package suwayomi.tachidesk.manga.impl

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.github.reactivecircus.cache4k.Cache
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.inList
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import suwayomi.tachidesk.manga.model.dataclass.MangaDataClass
import suwayomi.tachidesk.manga.model.dataclass.SourcePolicyDataClass
import suwayomi.tachidesk.manga.model.dataclass.WorkAliasDataClass
import suwayomi.tachidesk.manga.model.dataclass.WorkDataClass
import suwayomi.tachidesk.manga.model.dataclass.WorkSearchResultDataClass
import suwayomi.tachidesk.manga.model.dataclass.WorkSourceEntryDataClass
import suwayomi.tachidesk.manga.impl.extension.Extension
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ExtensionTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MergeCandidateTable
import suwayomi.tachidesk.manga.model.table.SourcePolicyTable
import suwayomi.tachidesk.manga.model.table.SourceTable
import suwayomi.tachidesk.manga.model.table.WorkAliasTable
import suwayomi.tachidesk.manga.model.table.WorkSourceEntryTable
import suwayomi.tachidesk.manga.model.table.WorkTable
import suwayomi.tachidesk.server.serverConfig
import java.text.Normalizer
import java.time.Instant
import java.util.UUID
import kotlin.math.max
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

object WorkSearch {
    private const val SAFE_MATCH_THRESHOLD = 80.0
    private const val PROBABLE_MATCH_THRESHOLD = 60.0
    private val SOURCE_STATS_REFRESH_INTERVAL = 12.hours
    private val SOURCE_FAILURE_COOLDOWN = 5.minutes
    private val SOURCE_SEARCH_TIMEOUT = 15.seconds
    private val SOURCE_STATS_TIMEOUT = 20.seconds
    private val searchCache = Cache.Builder<String, WorkSearchResultDataClass>().expireAfterWrite(5.minutes).build()
    private val searchFailureCooldown = Cache.Builder<Long, Unit>().expireAfterWrite(SOURCE_FAILURE_COOLDOWN).build()
    private val sourceStatsFailureCooldown = Cache.Builder<Long, Unit>().expireAfterWrite(SOURCE_FAILURE_COOLDOWN).build()

    @Serializable
    data class MergeInput(
        val targetWorkId: String,
        val workIds: List<String>,
    )

    @Serializable
    data class AliasInput(
        val alias: String,
        val source: String = "user",
        val confidence: Double = 1.0,
    )

    @Serializable
    data class SourcePolicyPatchInput(
        val enabled: Boolean? = null,
        val trusted: Boolean? = null,
        val rankWeight: Double? = null,
        val languagePriority: Int? = null,
        val installLocked: Boolean? = null,
    )

    private data class WorkCandidate(
        val id: UUID,
        val canonicalTitle: String,
        val normalizedTitle: String,
        val aliases: MutableSet<String>,
    )

    private data class ChapterStats(
        val chaptersCount: Int,
        val lastChapter: Double?,
        val lastUpdate: Long?,
    )

    private data class SourceDescriptor(
        val id: Long,
        val name: String,
        val language: String,
    )

    private data class SourceEntryRefreshTarget(
        val entryId: UUID,
        val mangaId: Int,
        val sourceId: Long,
        val policy: SourcePolicyDataClass,
    )

    fun invalidateCache() {
        searchCache.invalidateAll()
    }

    suspend fun search(query: String): WorkSearchResultDataClass {
        val normalizedQuery = normalize(query)
        require(normalizedQuery.isNotBlank()) { "query must not be blank" }

        searchCache.get(normalizedQuery)?.let { return it }

        val enabledPolicies = loadSourcePolicies().filter { it.enabled }
        val sourceSemaphore = Semaphore(serverConfig.maxSourcesInParallel.value.coerceAtLeast(1))
        val mangasBySource =
            coroutineScope {
                enabledPolicies.map { policy ->
                    async {
                        policy to searchSourceMangaList(policy.sourceId.toLong(), query, sourceSemaphore)
                    }
                }.awaitAll()
            }

        val mangas = mangasBySource.flatMap { it.second }.distinctBy { it.id }
        val matchedSources = mangasBySource.count { it.second.isNotEmpty() }
        val result =
            aggregateSearchResults(
                query = query,
                normalizedQuery = normalizedQuery,
                mangas = mangas,
                matchedSources = matchedSources,
                searchedSources = enabledPolicies.size,
                policiesBySource = enabledPolicies.associateBy { it.sourceId.toLong() },
            )

        searchCache.put(normalizedQuery, result)
        return result
    }

    fun getWork(workId: UUID): WorkDataClass? {
        refreshWorkSourceStats(workId)
        return transaction { loadWorks(setOf(workId)).firstOrNull() }
    }

    fun getWorkSources(workId: UUID): List<WorkSourceEntryDataClass> {
        refreshWorkSourceStats(workId)
        return transaction { loadWorks(setOf(workId)).firstOrNull()?.sources.orEmpty() }
    }

    fun addAlias(
        workId: UUID,
        input: AliasInput,
    ): WorkDataClass {
        transaction {
            require(input.alias.isNotBlank()) { "alias must not be blank" }
            requireWorkExists(workId)
            insertAliasIfMissing(workId, input.alias, input.source, input.confidence)
            touchWork(workId, input.confidence)
        }
        invalidateCache()
        return getWork(workId) ?: error("work $workId not found after alias update")
    }

    fun mergeWorks(input: MergeInput): WorkDataClass {
        val targetWorkId = UUID.fromString(input.targetWorkId)
        val workIds = input.workIds.map(UUID::fromString).distinct().filterNot { it == targetWorkId }

        transaction {
            requireWorkExists(targetWorkId)
            if (workIds.isNotEmpty()) {
                val sourceWorks = WorkTable.selectAll().where { WorkTable.id inList workIds }.toList()
                sourceWorks.forEach { row ->
                    insertAliasIfMissing(targetWorkId, row[WorkTable.canonicalTitle], "system", 1.0)
                }

                val aliasRows = WorkAliasTable.selectAll().where { WorkAliasTable.workId inList workIds }.toList()
                aliasRows.forEach { row ->
                    insertAliasIfMissing(
                        workId = targetWorkId,
                        alias = row[WorkAliasTable.alias],
                        source = row[WorkAliasTable.aliasSource],
                        confidence = row[WorkAliasTable.confidence],
                    )
                }

                WorkSourceEntryTable.update({ WorkSourceEntryTable.workId inList workIds }) {
                    it[workId] = targetWorkId
                }

                MergeCandidateTable.update({ MergeCandidateTable.candidateWorkId inList workIds }) {
                    it[candidateWorkId] = targetWorkId
                }

                WorkAliasTable.deleteWhere { WorkAliasTable.workId inList workIds }
                WorkTable.deleteWhere { WorkTable.id inList workIds }
                touchWork(targetWorkId, 1.0)
            }
        }

        invalidateCache()
        return getWork(targetWorkId) ?: error("work $targetWorkId not found after merge")
    }

    fun getSourcePolicy(sourceId: Long): SourcePolicyDataClass {
        val policies = loadSourcePolicies()
        return policies.firstOrNull { it.sourceId == sourceId.toString() } ?: error("source $sourceId not found")
    }

    fun updateSourcePolicy(
        sourceId: Long,
        input: SourcePolicyPatchInput,
    ): SourcePolicyDataClass {
        transaction {
            val sourceRow = SourceTable.selectAll().where { SourceTable.id eq sourceId }.firstOrNull() ?: error("source $sourceId not found")
            ensurePolicyExists(sourceRow)

            SourcePolicyTable.update({ SourcePolicyTable.sourceId eq sourceId }) {
                input.enabled?.let { value -> it[enabled] = value }
                input.trusted?.let { value -> it[trusted] = value }
                input.rankWeight?.let { value -> it[rankWeight] = value }
                input.languagePriority?.let { value -> it[languagePriority] = value }
                input.installLocked?.let { value -> it[installLocked] = value }
                it[SourcePolicyTable.sourceName] = sourceRow[SourceTable.name]
            }
        }

        invalidateCache()
        return getSourcePolicy(sourceId)
    }

    fun ensureExtensionMutationAllowed(pkgName: String) {
        val isLocked =
            transaction {
                val extensionId =
                    ExtensionTable
                        .selectAll()
                        .where { ExtensionTable.pkgName eq pkgName }
                        .firstOrNull()
                        ?.get(ExtensionTable.id)
                        ?.value
                        ?: return@transaction false

                val sourceIds =
                    SourceTable
                        .selectAll()
                        .where { SourceTable.extension eq extensionId }
                        .map { it[SourceTable.id].value }

                if (sourceIds.isEmpty()) {
                    false
                } else {
                    SourcePolicyTable
                        .selectAll()
                        .where { (SourcePolicyTable.sourceId inList sourceIds) and (SourcePolicyTable.installLocked eq true) }
                        .any()
                }
            }

        check(!isLocked) { "Extension $pkgName is locked by source policy" }
    }

    fun normalize(text: String): String {
        val withoutTags =
            text.replace(
                Regex("""\[[^\]]*]|\([^)]*official[^)]*\)|\([^)]*es[^)]*\)|\{[^}]*}""", RegexOption.IGNORE_CASE),
                " ",
            )
        val withoutAccents =
            Normalizer
                .normalize(withoutTags.lowercase(), Normalizer.Form.NFD)
                .replace(Regex("""\p{Mn}+"""), "")

        return withoutAccents
            .replace(Regex("""[^a-z0-9]+"""), " ")
            .trim()
            .replace(Regex("""\s+"""), " ")
    }

    fun previewMatchScore(
        title: String,
        canonicalTitle: String,
        aliases: List<String> = emptyList(),
    ): Double =
        calculateMatchScore(
            normalizedTitle = normalize(title),
            candidate =
                WorkCandidate(
                    id = UUID.randomUUID(),
                    canonicalTitle = canonicalTitle,
                    normalizedTitle = normalize(canonicalTitle),
                    aliases = aliases.mapTo(mutableSetOf(), ::normalize),
                ),
        )

    private fun aggregateSearchResults(
        query: String,
        normalizedQuery: String,
        mangas: List<MangaDataClass>,
        matchedSources: Int,
        searchedSources: Int,
        policiesBySource: Map<Long, SourcePolicyDataClass>,
    ): WorkSearchResultDataClass =
        transaction {
            if (mangas.isEmpty()) {
                return@transaction WorkSearchResultDataClass(query, normalizedQuery, emptyList(), searchedSources, matchedSources)
            }

            val allCandidates = loadAllWorkCandidates()
            val sourceEntriesByMangaId =
                WorkSourceEntryTable
                    .selectAll()
                    .where { WorkSourceEntryTable.mangaId inList mangas.map { it.id } }
                    .associateBy { it[WorkSourceEntryTable.mangaId].value }
            val sourceDescriptors =
                SourceTable
                    .selectAll()
                    .where { SourceTable.id inList mangas.map { it.sourceId.toLong() }.distinct() }
                    .associate {
                        it[SourceTable.id].value to
                            SourceDescriptor(
                                id = it[SourceTable.id].value,
                                name = it[SourceTable.name],
                                language = it[SourceTable.lang],
                            )
                    }

            val touchedWorkIds = linkedSetOf<UUID>()

            mangas.forEach { manga ->
                val sourceId = manga.sourceId.toLong()
                val sourceDescriptor = sourceDescriptors[sourceId] ?: return@forEach
                val policy = policiesBySource[sourceId] ?: defaultPolicy(sourceDescriptor)
                val normalizedTitle = normalize(manga.title)
                val chapterStats = loadChapterStats(manga.id)

                val existingSourceEntry = sourceEntriesByMangaId[manga.id]
                val assignedWorkId: UUID
                val matchScore: Double

                if (existingSourceEntry != null) {
                    assignedWorkId = existingSourceEntry[WorkSourceEntryTable.workId]
                    val workCandidate = allCandidates[assignedWorkId]
                    matchScore =
                        if (workCandidate != null) {
                            calculateMatchScore(normalizedTitle, workCandidate)
                        } else {
                            SAFE_MATCH_THRESHOLD
                        }
                } else {
                    val bestMatch =
                        allCandidates.values
                            .map { candidate -> candidate to calculateMatchScore(normalizedTitle, candidate) }
                            .maxByOrNull { it.second }

                    if (bestMatch != null && bestMatch.second >= SAFE_MATCH_THRESHOLD) {
                        assignedWorkId = bestMatch.first.id
                        matchScore = bestMatch.second
                        insertAliasIfMissing(assignedWorkId, manga.title, "system", (bestMatch.second / 100.0).coerceAtMost(1.0))
                        allCandidates[assignedWorkId]?.aliases?.add(normalizedTitle)
                    } else {
                        assignedWorkId =
                            createWork(
                                title = manga.title,
                                normalizedTitle = normalizedTitle,
                                coverUrl = manga.thumbnailUrl,
                                type = guessType(manga),
                                status = mapStatus(manga.status),
                                confidenceScore = (bestMatch?.second ?: SAFE_MATCH_THRESHOLD) / 100.0,
                            )
                        allCandidates[assignedWorkId] =
                            WorkCandidate(
                                id = assignedWorkId,
                                canonicalTitle = manga.title,
                                normalizedTitle = normalizedTitle,
                                aliases = mutableSetOf(),
                            )
                        matchScore = SAFE_MATCH_THRESHOLD
                    }

                    if (bestMatch != null && bestMatch.second in PROBABLE_MATCH_THRESHOLD..<SAFE_MATCH_THRESHOLD && assignedWorkId != bestMatch.first.id) {
                        val sourceEntryId =
                            upsertSourceEntry(
                                workId = assignedWorkId,
                                manga = manga,
                                source = sourceDescriptor,
                                chapterStats = chapterStats,
                                matchScore = matchScore,
                                qualityScore = calculateQualityScore(chapterStats, policy),
                            )
                        insertMergeCandidate(sourceEntryId, bestMatch.first.id, bestMatch.second)
                        touchedWorkIds += assignedWorkId
                        return@forEach
                    }
                }

                val qualityScore = calculateQualityScore(chapterStats, policy)
                upsertSourceEntry(
                    workId = assignedWorkId,
                    manga = manga,
                    source = sourceDescriptor,
                    chapterStats = chapterStats,
                    matchScore = matchScore,
                    qualityScore = qualityScore,
                )
                updateWorkMetadata(
                    workId = assignedWorkId,
                    manga = manga,
                    normalizedTitle = normalizedTitle,
                    confidenceScore = (matchScore / 100.0).coerceAtMost(1.0),
                )
                touchedWorkIds += assignedWorkId
            }

            WorkSearchResultDataClass(
                query = query,
                normalizedQuery = normalizedQuery,
                works = loadWorks(touchedWorkIds, normalizedQuery),
                searchedSources = searchedSources,
                matchedSources = matchedSources,
            )
        }

    private fun loadSourcePolicies(): List<SourcePolicyDataClass> =
        transaction {
            val sourceRows = SourceTable.selectAll().toList()
            sourceRows.forEach(::ensurePolicyExists)

            val policyRows =
                SourcePolicyTable
                    .selectAll()
                    .where { SourcePolicyTable.sourceId inList sourceRows.map { it[SourceTable.id].value } }
                    .associateBy { it[SourcePolicyTable.sourceId] }

            sourceRows.map { sourceRow ->
                val sourceId = sourceRow[SourceTable.id].value
                val policyRow = policyRows[sourceId]
                SourcePolicyDataClass(
                    sourceId = sourceId.toString(),
                    sourceName = policyRow?.get(SourcePolicyTable.sourceName) ?: sourceRow[SourceTable.name],
                    enabled = policyRow?.get(SourcePolicyTable.enabled) ?: true,
                    trusted = policyRow?.get(SourcePolicyTable.trusted) ?: true,
                    rankWeight = policyRow?.get(SourcePolicyTable.rankWeight) ?: 0.0,
                    languagePriority = policyRow?.get(SourcePolicyTable.languagePriority) ?: 0,
                    installLocked = policyRow?.get(SourcePolicyTable.installLocked) ?: false,
                )
            }
        }

    private fun ensurePolicyExists(sourceRow: ResultRow) {
        val sourceId = sourceRow[SourceTable.id].value
        val exists = SourcePolicyTable.selectAll().where { SourcePolicyTable.sourceId eq sourceId }.firstOrNull() != null
        if (!exists) {
            SourcePolicyTable.insert {
                it[this.sourceId] = sourceId
                it[sourceName] = sourceRow[SourceTable.name]
                it[enabled] = true
                it[trusted] = true
                it[rankWeight] = 0.0
                it[languagePriority] = 0
                it[installLocked] = false
            }
        }
    }

    private fun defaultPolicy(source: SourceDescriptor) =
        SourcePolicyDataClass(
            sourceId = source.id.toString(),
            sourceName = source.name,
            enabled = true,
            trusted = true,
            rankWeight = 0.0,
            languagePriority = 0,
            installLocked = false,
        )

    private fun loadAllWorkCandidates(): MutableMap<UUID, WorkCandidate> {
        val workRows = WorkTable.selectAll().toList()
        if (workRows.isEmpty()) {
            return linkedMapOf()
        }

        val aliasesByWorkId =
            WorkAliasTable
                .selectAll()
                .where { WorkAliasTable.workId inList workRows.map { it[WorkTable.id] } }
                .groupBy { it[WorkAliasTable.workId] }
                .mapValues { entry -> entry.value.mapTo(mutableSetOf()) { it[WorkAliasTable.normalizedAlias] } }

        return workRows.associate {
            val id = it[WorkTable.id]
            id to
                WorkCandidate(
                    id = id,
                    canonicalTitle = it[WorkTable.canonicalTitle],
                    normalizedTitle = it[WorkTable.normalizedTitle],
                    aliases = aliasesByWorkId[id] ?: mutableSetOf(),
                )
        }.toMutableMap()
    }

    private fun loadWorks(
        workIds: Set<UUID>,
        normalizedQuery: String? = null,
    ): List<WorkDataClass> {
        if (workIds.isEmpty()) {
            return emptyList()
        }

        val workRows = WorkTable.selectAll().where { WorkTable.id inList workIds.toList() }.toList()
        val aliasRows = WorkAliasTable.selectAll().where { WorkAliasTable.workId inList workIds.toList() }.toList()
        val sourceEntryRows = WorkSourceEntryTable.selectAll().where { WorkSourceEntryTable.workId inList workIds.toList() }.toList()
        val mangaRows =
            MangaTable
                .selectAll()
                .where { MangaTable.id inList sourceEntryRows.map { it[WorkSourceEntryTable.mangaId].value } }
                .associateBy { it[MangaTable.id].value }
        val sourceRows =
            SourceTable
                .selectAll()
                .where { SourceTable.id inList sourceEntryRows.map { it[WorkSourceEntryTable.sourceId] }.distinct() }
                .associateBy { it[SourceTable.id].value }
        val extensionRows =
            ExtensionTable
                .selectAll()
                .where { ExtensionTable.id inList sourceRows.values.map { it[SourceTable.extension].value }.distinct() }
                .associateBy { it[ExtensionTable.id].value }

        val aliasesByWorkId = aliasRows.groupBy { it[WorkAliasTable.workId] }
        val sourcesByWorkId = sourceEntryRows.groupBy { it[WorkSourceEntryTable.workId] }

        return workRows.map { workRow ->
            val sourceEntries =
                sourcesByWorkId[workRow[WorkTable.id]]
                    .orEmpty()
                    .sortedWith(
                        compareByDescending<ResultRow> { it[WorkSourceEntryTable.qualityScore] }
                            .thenByDescending { it[WorkSourceEntryTable.matchScore] }
                            .thenByDescending { it[WorkSourceEntryTable.chaptersCount] }
                            .thenByDescending { it[WorkSourceEntryTable.lastUpdate] ?: 0L }
                            .thenBy { it[WorkSourceEntryTable.sourceName] },
                    )
            val recommendedSourceId = sourceEntries.firstOrNull()?.get(WorkSourceEntryTable.id)?.toString()
            val sources =
                sourceEntries.mapIndexed { index, row ->
                    WorkSourceEntryDataClass(
                        id = row[WorkSourceEntryTable.id].toString(),
                        workId = row[WorkSourceEntryTable.workId].toString(),
                        mangaId = row[WorkSourceEntryTable.mangaId].value,
                        sourceId = row[WorkSourceEntryTable.sourceId].toString(),
                        sourceName = row[WorkSourceEntryTable.sourceName],
                        sourceIconUrl =
                            sourceRows[row[WorkSourceEntryTable.sourceId]]
                                ?.let { sourceRow -> extensionRows[sourceRow[SourceTable.extension].value] }
                                ?.let { extensionRow -> Extension.getExtensionIconUrl(extensionRow[ExtensionTable.apkName]) },
                        sourceMangaId = row[WorkSourceEntryTable.sourceMangaId],
                        sourceTitle = row[WorkSourceEntryTable.sourceTitle],
                        normalizedSourceTitle = row[WorkSourceEntryTable.normalizedSourceTitle],
                        chaptersCount = row[WorkSourceEntryTable.chaptersCount],
                        lastChapter = row[WorkSourceEntryTable.lastChapter],
                        lastUpdate = row[WorkSourceEntryTable.lastUpdate],
                        language = row[WorkSourceEntryTable.language],
                        matchScore = row[WorkSourceEntryTable.matchScore],
                        qualityScore = row[WorkSourceEntryTable.qualityScore],
                        thumbnailUrl = mangaRows[row[WorkSourceEntryTable.mangaId].value]?.get(MangaTable.thumbnail_url),
                        recommended = index == 0,
                    )
                }
            val candidate =
                WorkCandidate(
                    id = workRow[WorkTable.id],
                    canonicalTitle = workRow[WorkTable.canonicalTitle],
                    normalizedTitle = workRow[WorkTable.normalizedTitle],
                    aliases =
                        aliasesByWorkId[workRow[WorkTable.id]]
                            .orEmpty()
                            .mapTo(mutableSetOf()) { it[WorkAliasTable.normalizedAlias] },
                )

            WorkDataClass(
                id = workRow[WorkTable.id].toString(),
                canonicalTitle = workRow[WorkTable.canonicalTitle],
                normalizedTitle = workRow[WorkTable.normalizedTitle],
                coverUrl = workRow[WorkTable.coverUrl] ?: sources.firstOrNull()?.thumbnailUrl,
                type = workRow[WorkTable.type],
                status = workRow[WorkTable.status],
                createdAt = workRow[WorkTable.createdAt],
                updatedAt = workRow[WorkTable.updatedAt],
                confidenceScore = workRow[WorkTable.confidenceScore],
                searchScore = normalizedQuery?.let { calculateMatchScore(it, candidate) },
                recommendedSourceId = recommendedSourceId,
                aliases =
                    aliasesByWorkId[workRow[WorkTable.id]]
                        .orEmpty()
                        .map { aliasRow ->
                            WorkAliasDataClass(
                                id = aliasRow[WorkAliasTable.id].toString(),
                                workId = aliasRow[WorkAliasTable.workId].toString(),
                                alias = aliasRow[WorkAliasTable.alias],
                                normalizedAlias = aliasRow[WorkAliasTable.normalizedAlias],
                                source = aliasRow[WorkAliasTable.aliasSource],
                                confidence = aliasRow[WorkAliasTable.confidence],
                            )
                        },
                sources = sources,
            )
        }.sortedWith(
            compareByDescending<WorkDataClass> { it.searchScore ?: 0.0 }
                .thenByDescending { it.sources.maxOfOrNull(WorkSourceEntryDataClass::qualityScore) ?: 0.0 }
                .thenBy { it.canonicalTitle },
        )
    }

    private fun refreshWorkSourceStats(workId: UUID) =
        runBlocking {
            val policiesBySource = loadSourcePolicies().associateBy { it.sourceId.toLong() }
            val refreshTargets =
                transaction {
                    val sourceRows =
                        WorkSourceEntryTable
                            .innerJoin(MangaTable)
                            .select(
                                WorkSourceEntryTable.columns + MangaTable.columns,
                            ).where { WorkSourceEntryTable.workId eq workId }
                            .toList()
                    if (sourceRows.isEmpty()) {
                        return@transaction emptyList()
                    }

                    val now = Instant.now().epochSecond

                    sourceRows.mapNotNull { row ->
                        val sourceId = row[WorkSourceEntryTable.sourceId]
                        val policy = policiesBySource[sourceId] ?: return@mapNotNull null
                        val chaptersLastFetchedAt = row[MangaTable.chaptersLastFetchedAt]
                        val shouldRefresh =
                            row[WorkSourceEntryTable.chaptersCount] <= 0 ||
                                row[WorkSourceEntryTable.lastUpdate] == null ||
                                chaptersLastFetchedAt <= 0L ||
                                (now - chaptersLastFetchedAt) >= SOURCE_STATS_REFRESH_INTERVAL.inWholeSeconds

                        if (!shouldRefresh) {
                            return@mapNotNull null
                        }

                        SourceEntryRefreshTarget(
                            entryId = row[WorkSourceEntryTable.id],
                            mangaId = row[WorkSourceEntryTable.mangaId].value,
                            sourceId = sourceId,
                            policy = policy,
                        )
                    }
                }
            val sourceSemaphore = Semaphore(serverConfig.maxSourcesInParallel.value.coerceAtLeast(1))

            coroutineScope {
                refreshTargets.map { target ->
                    async {
                        sourceSemaphore.withPermit {
                            refreshSourceEntryStats(target)
                        }
                    }
                }.awaitAll()
            }
        }

    private suspend fun refreshSourceEntryStats(target: SourceEntryRefreshTarget) {
        if (sourceStatsFailureCooldown.get(target.sourceId) != null) {
            return
        }

        val didRefresh =
            withTimeoutOrNull(SOURCE_STATS_TIMEOUT) {
                runCatching { Chapter.fetchChapterList(target.mangaId) }.isSuccess
            } ?: false
        if (!didRefresh) {
            sourceStatsFailureCooldown.put(target.sourceId, Unit)
            return
        }
        sourceStatsFailureCooldown.invalidate(target.sourceId)

        val updatedStats = transaction { loadChapterStats(target.mangaId) }
        val qualityScore = calculateQualityScore(updatedStats, target.policy)

        transaction {
            WorkSourceEntryTable.update({ WorkSourceEntryTable.id eq target.entryId }) {
                it[chaptersCount] = updatedStats.chaptersCount
                it[lastChapter] = updatedStats.lastChapter
                it[lastUpdate] = updatedStats.lastUpdate
                it[WorkSourceEntryTable.qualityScore] = qualityScore
            }
        }
    }

    private fun calculateMatchScore(
        normalizedTitle: String,
        candidate: WorkCandidate,
    ): Double {
        val exactMatch = if (normalizedTitle == candidate.normalizedTitle) 1.0 else 0.0
        val aliasMatch = if (normalizedTitle in candidate.aliases) 1.0 else 0.0
        val comparableTitles = listOf(candidate.normalizedTitle) + candidate.aliases
        val fuzzySimilarity = comparableTitles.maxOf { similarity(normalizedTitle, it) }
        val tokenOverlap = comparableTitles.maxOf { tokenOverlap(normalizedTitle, it) }

        return (exactMatch * 100) + (aliasMatch * 80) + (fuzzySimilarity * 60) + (tokenOverlap * 40)
    }

    private fun similarity(
        left: String,
        right: String,
    ): Double {
        if (left.isBlank() || right.isBlank()) {
            return 0.0
        }
        if (left == right) {
            return 1.0
        }

        val distance = levenshtein(left, right)
        val maxLength = max(left.length, right.length)
        return (1.0 - (distance.toDouble() / maxLength.toDouble())).coerceIn(0.0, 1.0)
    }

    private fun levenshtein(
        left: String,
        right: String,
    ): Int {
        val distances = IntArray(right.length + 1) { it }
        for (leftIndex in 1..left.length) {
            var previousDiagonal = distances[0]
            distances[0] = leftIndex
            for (rightIndex in 1..right.length) {
                val previous = distances[rightIndex]
                distances[rightIndex] =
                    if (left[leftIndex - 1] == right[rightIndex - 1]) {
                        previousDiagonal
                    } else {
                        minOf(distances[rightIndex] + 1, distances[rightIndex - 1] + 1, previousDiagonal + 1)
                    }
                previousDiagonal = previous
            }
        }
        return distances[right.length]
    }

    private fun tokenOverlap(
        left: String,
        right: String,
    ): Double {
        val leftTokens = left.split(" ").filter(String::isNotBlank).toSet()
        val rightTokens = right.split(" ").filter(String::isNotBlank).toSet()
        if (leftTokens.isEmpty() || rightTokens.isEmpty()) {
            return 0.0
        }

        val intersection = leftTokens.intersect(rightTokens).size.toDouble()
        val union = leftTokens.union(rightTokens).size.toDouble()
        return if (union == 0.0) 0.0 else (intersection / union).coerceIn(0.0, 1.0)
    }

    private fun createWork(
        title: String,
        normalizedTitle: String,
        coverUrl: String?,
        type: String,
        status: String,
        confidenceScore: Double,
    ): UUID {
        val now = Instant.now().epochSecond
        val id = UUID.randomUUID()
        WorkTable.insert {
            it[this.id] = id
            it[canonicalTitle] = title
            it[WorkTable.normalizedTitle] = normalizedTitle
            it[WorkTable.coverUrl] = coverUrl
            it[WorkTable.type] = type
            it[WorkTable.status] = status
            it[createdAt] = now
            it[updatedAt] = now
            it[WorkTable.confidenceScore] = confidenceScore.coerceIn(0.0, 1.0)
        }
        return id
    }

    private fun insertAliasIfMissing(
        workId: UUID,
        alias: String,
        source: String,
        confidence: Double,
    ) {
        val normalizedAlias = normalize(alias)
        if (normalizedAlias.isBlank()) {
            return
        }

        val work = WorkTable.selectAll().where { WorkTable.id eq workId }.firstOrNull() ?: return
        if (work[WorkTable.normalizedTitle] == normalizedAlias) {
            return
        }

        val exists =
            WorkAliasTable
                .selectAll()
                .where { (WorkAliasTable.workId eq workId) and (WorkAliasTable.normalizedAlias eq normalizedAlias) }
                .firstOrNull() != null

        if (!exists) {
            WorkAliasTable.insert {
                it[id] = UUID.randomUUID()
                it[this.workId] = workId
                it[this.alias] = alias
                it[this.normalizedAlias] = normalizedAlias
                it[aliasSource] = source
                it[this.confidence] = confidence.coerceIn(0.0, 1.0)
            }
        }
    }

    private fun upsertSourceEntry(
        workId: UUID,
        manga: MangaDataClass,
        source: SourceDescriptor,
        chapterStats: ChapterStats,
        matchScore: Double,
        qualityScore: Double,
    ): UUID {
        val existing = WorkSourceEntryTable.selectAll().where { WorkSourceEntryTable.mangaId eq manga.id }.firstOrNull()
        val entryId = existing?.get(WorkSourceEntryTable.id) ?: UUID.randomUUID()

        if (existing == null) {
            WorkSourceEntryTable.insert {
                it[id] = entryId
                it[this.workId] = workId
                it[mangaId] = manga.id
                it[sourceId] = source.id
                it[sourceName] = source.name
                it[sourceMangaId] = manga.url
                it[sourceTitle] = manga.title
                it[normalizedSourceTitle] = normalize(manga.title)
                it[chaptersCount] = chapterStats.chaptersCount
                it[lastChapter] = chapterStats.lastChapter
                it[lastUpdate] = chapterStats.lastUpdate
                it[language] = source.language
                it[this.matchScore] = matchScore
                it[this.qualityScore] = qualityScore
                it[rawMetadata] = """{"url":"${manga.url.replace("\"", "\\\"")}","sourceId":"${manga.sourceId}"}"""
            }
        } else {
            WorkSourceEntryTable.update({ WorkSourceEntryTable.id eq entryId }) {
                it[this.workId] = workId
                it[sourceId] = source.id
                it[sourceName] = source.name
                it[sourceMangaId] = manga.url
                it[sourceTitle] = manga.title
                it[normalizedSourceTitle] = normalize(manga.title)
                it[chaptersCount] = chapterStats.chaptersCount
                it[lastChapter] = chapterStats.lastChapter
                it[lastUpdate] = chapterStats.lastUpdate
                it[language] = source.language
                it[this.matchScore] = matchScore
                it[this.qualityScore] = qualityScore
                it[rawMetadata] = """{"url":"${manga.url.replace("\"", "\\\"")}","sourceId":"${manga.sourceId}"}"""
            }
        }

        return entryId
    }

    private fun insertMergeCandidate(
        sourceEntryId: UUID,
        candidateWorkId: UUID,
        score: Double,
    ) {
        val exists =
            MergeCandidateTable
                .selectAll()
                .where {
                    (MergeCandidateTable.sourceEntryId eq sourceEntryId) and
                        (MergeCandidateTable.candidateWorkId eq candidateWorkId)
                }.firstOrNull() != null

        if (!exists) {
            MergeCandidateTable.insert {
                it[id] = UUID.randomUUID()
                it[this.sourceEntryId] = sourceEntryId
                it[this.candidateWorkId] = candidateWorkId
                it[this.score] = score
                it[status] = "pending"
            }
        }
    }

    private fun loadChapterStats(mangaId: Int): ChapterStats {
        val chapterRows = ChapterTable.selectAll().where { ChapterTable.manga eq mangaId }.toList()
        if (chapterRows.isEmpty()) {
            return ChapterStats(chaptersCount = 0, lastChapter = null, lastUpdate = null)
        }

        return ChapterStats(
            chaptersCount = chapterRows.size,
            lastChapter = chapterRows.maxOfOrNull { it[ChapterTable.chapter_number].toDouble() }?.takeIf { it >= 0.0 },
            lastUpdate =
                chapterRows
                    .maxOfOrNull { normalizeEpochSeconds(it[ChapterTable.date_upload]) }
                    .takeIf { it != null && it > 0 },
        )
    }

    private fun calculateQualityScore(
        stats: ChapterStats,
        policy: SourcePolicyDataClass,
    ): Double {
        val now = Instant.now().epochSecond
        val age = stats.lastUpdate?.let { now - normalizeEpochSeconds(it) }
        val latestChapter = stats.lastChapter?.takeIf { it > 0.0 } ?: stats.chaptersCount.toDouble()
        val recencyBonus =
            when {
                age == null -> 0.0
                age <= 1.days.inWholeSeconds -> 12.0
                age <= 7.days.inWholeSeconds -> 8.0
                age <= 30.days.inWholeSeconds -> 3.0
                else -> 0.0
            }
        val outdatedPenalty = if (age != null && age > 90.days.inWholeSeconds) 10.0 else 0.0
        val missingChaptersPenalty = if (stats.chaptersCount == 0) 15.0 else 0.0
        val sourceWeight = (if (policy.trusted) 10.0 else 0.0) + (policy.rankWeight * 10.0)

        return (minOf(stats.chaptersCount, 200) * 0.05) +
            (minOf(latestChapter, 400.0) * 0.5) +
            recencyBonus +
            sourceWeight +
            policy.languagePriority -
            missingChaptersPenalty -
            outdatedPenalty
    }

    private fun normalizeEpochSeconds(timestamp: Long): Long = if (timestamp > 10_000_000_000L) timestamp / 1000 else timestamp

    private suspend fun searchSourceMangaList(
        sourceId: Long,
        query: String,
        sourceSemaphore: Semaphore,
    ): List<MangaDataClass> {
        if (searchFailureCooldown.get(sourceId) != null) {
            return emptyList()
        }

        val result =
            sourceSemaphore.withPermit {
                withTimeoutOrNull(SOURCE_SEARCH_TIMEOUT) {
                    runCatching { Search.sourceSearch(sourceId, query, 1).mangaList }.getOrNull()
                }
            }

        return result?.also { searchFailureCooldown.invalidate(sourceId) } ?: run {
            searchFailureCooldown.put(sourceId, Unit)
            emptyList()
        }
    }

    private fun updateWorkMetadata(
        workId: UUID,
        manga: MangaDataClass,
        normalizedTitle: String,
        confidenceScore: Double,
    ) {
        val existing = WorkTable.selectAll().where { WorkTable.id eq workId }.firstOrNull() ?: return
        val now = Instant.now().epochSecond
        val currentConfidence = existing[WorkTable.confidenceScore]
        val currentCanonicalTitle = existing[WorkTable.canonicalTitle]
        val shouldPromoteTitle = normalizedTitle == normalize(currentCanonicalTitle) && manga.title.length < currentCanonicalTitle.length

        WorkTable.update({ WorkTable.id eq workId }) {
            it[updatedAt] = now
            it[WorkTable.confidenceScore] = max(currentConfidence, confidenceScore.coerceIn(0.0, 1.0))
            if (existing[WorkTable.coverUrl].isNullOrBlank() && !manga.thumbnailUrl.isNullOrBlank()) {
                it[coverUrl] = manga.thumbnailUrl
            }
            if (shouldPromoteTitle) {
                it[canonicalTitle] = manga.title
                it[WorkTable.normalizedTitle] = normalizedTitle
            }
            if (existing[WorkTable.type] == "unknown") {
                it[type] = guessType(manga)
            }
            if (existing[WorkTable.status] == "unknown") {
                it[status] = mapStatus(manga.status)
            }
        }
    }

    private fun touchWork(
        workId: UUID,
        confidenceScore: Double,
    ) {
        val existing = WorkTable.selectAll().where { WorkTable.id eq workId }.firstOrNull() ?: return
        WorkTable.update({ WorkTable.id eq workId }) {
            it[updatedAt] = Instant.now().epochSecond
            it[WorkTable.confidenceScore] = max(existing[WorkTable.confidenceScore], confidenceScore.coerceIn(0.0, 1.0))
        }
    }

    private fun requireWorkExists(workId: UUID) {
        check(WorkTable.selectAll().where { WorkTable.id eq workId }.firstOrNull() != null) { "work $workId not found" }
    }

    private fun guessType(manga: MangaDataClass): String {
        val haystack = (manga.genre + listOfNotNull(manga.description, manga.title)).joinToString(" ").lowercase()
        return when {
            "manhwa" in haystack -> "manhwa"
            "manhua" in haystack -> "manhua"
            "manga" in haystack -> "manga"
            else -> "unknown"
        }
    }

    private fun mapStatus(status: String): String =
        when (status.uppercase()) {
            "ONGOING", "ON_HIATUS" -> "ongoing"
            "COMPLETED", "PUBLISHING_FINISHED" -> "completed"
            else -> "unknown"
        }
}
