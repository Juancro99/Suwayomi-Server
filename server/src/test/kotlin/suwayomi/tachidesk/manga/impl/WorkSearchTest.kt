package suwayomi.tachidesk.manga.impl

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkSearchTest {
    @Test
    fun `normalize removes tags punctuation and accents`() {
        assertEquals(
            "solo leveling",
            WorkSearch.normalize("Sóló Leveling [ES] (Official)"),
        )
    }

    @Test
    fun `variant titles pass safe match threshold`() {
        val score = WorkSearch.previewMatchScore("Solo Leveling [ES]", "Solo Leveling")

        assertTrue(score >= 80.0, "expected a safe match but got $score")
    }

    @Test
    fun `alias titles pass safe match threshold`() {
        val score =
            WorkSearch.previewMatchScore(
                title = "Only I Level Up",
                canonicalTitle = "Solo Leveling",
                aliases = listOf("Only I Level Up"),
            )

        assertTrue(score >= 80.0, "expected an alias match but got $score")
    }

    @Test
    fun `sequels and spin offs stay below grouping threshold`() {
        val sequelScore = WorkSearch.previewMatchScore("Naruto Shippuden", "Naruto")
        val spinoffScore = WorkSearch.previewMatchScore("Berserk of Gluttony", "Berserk")

        assertTrue(sequelScore < 60.0, "expected sequel score below threshold but got $sequelScore")
        assertTrue(spinoffScore < 60.0, "expected spinoff score below threshold but got $spinoffScore")
    }
}
