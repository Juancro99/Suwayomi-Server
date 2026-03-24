package suwayomi.tachidesk.manga.controller

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.javalin.http.HttpStatus
import kotlinx.serialization.json.Json
import suwayomi.tachidesk.manga.impl.WorkSearch
import suwayomi.tachidesk.manga.model.dataclass.WorkDataClass
import suwayomi.tachidesk.manga.model.dataclass.WorkSearchResultDataClass
import suwayomi.tachidesk.manga.model.dataclass.WorkSourceEntryDataClass
import suwayomi.tachidesk.server.JavalinSetup.Attribute
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.JavalinSetup.getAttribute
import suwayomi.tachidesk.server.user.requireUser
import suwayomi.tachidesk.server.util.handler
import suwayomi.tachidesk.server.util.pathParam
import suwayomi.tachidesk.server.util.queryParam
import suwayomi.tachidesk.server.util.withOperation
import uy.kohesive.injekt.injectLazy
import java.util.UUID

object WorkController {
    private val json: Json by injectLazy()

    val search =
        handler(
            queryParam("q", ""),
            documentWith = {
                withOperation {
                    summary("Aggregated search")
                    description("Search installed sources and aggregate matches into canonical works.")
                }
            },
            behaviorOf = { ctx, query ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                ctx.future {
                    future { WorkSearch.search(query) }
                        .thenApply { ctx.json(it) }
                }
            },
            withResults = {
                json<WorkSearchResultDataClass>(HttpStatus.OK)
            },
        )

    val retrieve =
        handler(
            pathParam<String>("workId"),
            documentWith = {
                withOperation {
                    summary("Get work")
                    description("Fetch a canonical work and all of its aggregated sources.")
                }
            },
            behaviorOf = { ctx, workId ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                val work = WorkSearch.getWork(UUID.fromString(workId)) ?: throw IllegalArgumentException("work $workId not found")
                ctx.json(work)
            },
            withResults = {
                json<WorkDataClass>(HttpStatus.OK)
                httpCode(HttpStatus.NOT_FOUND)
            },
        )

    val sources =
        handler(
            pathParam<String>("workId"),
            documentWith = {
                withOperation {
                    summary("Get work sources")
                    description("List all source entries associated with a canonical work.")
                }
            },
            behaviorOf = { ctx, workId ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                ctx.json(WorkSearch.getWorkSources(UUID.fromString(workId)))
            },
            withResults = {
                json<Array<WorkSourceEntryDataClass>>(HttpStatus.OK)
            },
        )

    val merge =
        handler(
            documentWith = {
                withOperation {
                    summary("Merge works")
                    description("Manually merge multiple canonical works into a target work.")
                }
                body<WorkSearch.MergeInput>()
            },
            behaviorOf = { ctx ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                val input = json.decodeFromString<WorkSearch.MergeInput>(ctx.body())
                ctx.json(WorkSearch.mergeWorks(input))
            },
            withResults = {
                json<WorkDataClass>(HttpStatus.OK)
            },
        )

    val addAlias =
        handler(
            pathParam<String>("workId"),
            documentWith = {
                withOperation {
                    summary("Add work alias")
                    description("Add a manual alias to a canonical work.")
                }
                body<WorkSearch.AliasInput>()
            },
            behaviorOf = { ctx, workId ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()
                val input = json.decodeFromString<WorkSearch.AliasInput>(ctx.body())
                ctx.json(WorkSearch.addAlias(UUID.fromString(workId), input))
            },
            withResults = {
                json<WorkDataClass>(HttpStatus.OK)
            },
        )
}
