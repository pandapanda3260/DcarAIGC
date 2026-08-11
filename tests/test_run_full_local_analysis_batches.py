from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import tempfile
import unittest
import urllib.error
from contextlib import ExitStack, closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests import test_run_local_analysis_canary as canary_tests

from scripts import run_full_local_analysis_batches as batches
from scripts import run_local_analysis_canary as local
from v8 import duplicates, evaluation, media


class FullLocalAnalysisBatchesMilestoneTest(unittest.TestCase):
    """Phase-0 tests reuse the already audited Step3/local-analysis fixture."""

    def __getattr__(self, name: str):
        fixture = self.__dict__.get("fixture")
        if fixture is not None:
            return getattr(fixture, name)
        raise AttributeError(name)

    def setUp(self) -> None:
        self.fixture = canary_tests.LocalAnalysisCanaryControllerTest(
            methodName="runTest"
        )
        self.fixture.setUp()
        self.fixture._add_source_content(2)
        self.fixture._add_source_content(3)
        self.profile = batches.HistoryProfile(
            universe_count=3,
            eligible_count=3,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
        )

    def tearDown(self) -> None:
        try:
            self.fixture.tearDown()
        finally:
            # The embedded TestCase is not driven by unittest's runner, so its
            # addCleanup stack (including capture.RAW_ROOT restoration) must be
            # drained explicitly before the next full-controller test.
            self.fixture.doCleanups()

    def _batch_arguments(self, **overrides):
        values = {
            "source_db_path": self.source_db,
            "source_completion_path": self.source_completion,
            "expected_source_db_sha256": self.source_db_sha,
            "expected_source_completion_sha256": self.source_completion_sha,
            "db_path": self.db,
            "media_root": self.media_root,
            "run_root": self.run_root,
            "through_batch": 1,
            "workers": 1,
            "profile": self.profile,
        }
        values.update(overrides)
        return values

    def _pipeline_patches(self, *, media_side_effect=None) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(patch.object(local, "_local_tools", return_value=self.tools))
        stack.enter_context(
            patch.object(
                local.urllib.request,
                "build_opener",
                return_value=SimpleNamespace(
                    open=lambda request, **_kwargs: canary_tests._Response(
                        str(request.full_url), b"video" * 1000
                    )
                ),
            )
        )
        stack.enter_context(
            patch.object(
                media,
                "process_content_media",
                side_effect=media_side_effect or self.fixture._fake_media,
            )
        )
        stack.enter_context(
            patch.object(
                evaluation,
                "evaluate_content",
                side_effect=self.fixture._fake_evaluation,
            )
        )
        stack.enter_context(
            patch.object(
                duplicates,
                "fingerprint_content",
                side_effect=self.fixture._fake_fingerprint,
            )
        )
        return stack

    def _fake_review_pending_evaluation(
        self,
        content_id: int,
        *,
        db_path: Path,
        pending_content_ids: frozenset[int] = frozenset({2}),
    ):
        pending_review = content_id in pending_content_ids
        result = self.fixture._fake_evaluation(
            content_id,
            db_path=db_path,
            pending_review=int(pending_review),
        )
        if pending_review:
            captured_at = "2026-08-11T00:00:00+00:00"
            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            try:
                evaluation_row = connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (result.evaluation_id,),
                ).fetchone()
                artifacts, _components, _evidence_sha = (
                    evaluation._current_evidence_state(connection, content_id)
                )
                content = connection.execute(
                    "SELECT * FROM content_items WHERE id=?",
                    (content_id,),
                ).fetchone()
                asr = evaluation._read_json(artifacts["asr_path"])
                ocr = evaluation._read_json(artifacts["ocr_path"])
                manual_rows = artifacts["manual_rows"]
                manual_text = "\n".join(
                    str(row.get("text_value") or "") for row in manual_rows
                )
                body_text = "\n".join(
                    value
                    for value in (
                        str(content["title"] or ""),
                        str(content["body"] or ""),
                        manual_text,
                    )
                    if value
                )
                content_score = evaluation._automotive_score(
                    f"{body_text}\n{asr.get('text') or ''}\n"
                    f"{ocr.get('combined_text') or ''}",
                    selling_included=False,
                )
                audience_score, action_score, valid_commenters = (
                    evaluation._comment_scores(connection, content_id)
                )
                acquisition_score = evaluation._acquisition_score(
                    content_score,
                    audience_score,
                    0,
                    action_score,
                )
                match = {
                    "id": "X10",
                    "score": 72,
                    "scene": "media",
                    "reason": "fixture gray",
                    "source": "desc",
                }
                payload = json.loads(evaluation_row["payload_json"])
                payload.update(
                    {
                        "primary_selling_point_id": "X10",
                        "selling_point_score": 72,
                        "selling_point_included": False,
                        "pending_review": True,
                        "content_direction": "media",
                        "content_automotive_score": content_score,
                        "audience_automotive_score": audience_score,
                        "action_intent_score": action_score,
                        "valid_unique_commenters": valid_commenters,
                        "acquisition_potential": acquisition_score,
                        "matches": [match],
                    }
                )
                connection.execute(
                    """
                    UPDATE evaluation_versions
                    SET primary_selling_point_code='X10',selling_point_score=72,
                        selling_point_included=0,content_direction='media',
                        content_automotive_score=?,audience_automotive_score=?,
                        acquisition_potential_score=?,payload_json=?
                    WHERE id=?
                    """,
                    (
                        content_score,
                        audience_score,
                        acquisition_score,
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        result.evaluation_id,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO evaluation_matches(
                        evaluation_id,selling_point_code,scene,match_role,
                        score,evidence_json
                    ) VALUES (?,'X10','media','primary',72,?)
                    """,
                    (
                        result.evaluation_id,
                        json.dumps(
                            match,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO review_queue(
                        content_id,evaluation_id,reason_code,priority,status,
                        assigned_to,created_at,updated_at,resolved_at
                    ) VALUES (?,?,'evaluation_gray_zone',50,'pending',NULL,?,?,NULL)
                    """,
                    (
                        content_id,
                        result.evaluation_id,
                        captured_at,
                        captured_at,
                    ),
                )
                connection.commit()
            finally:
                connection.close()
        return result

    @staticmethod
    def _fake_review_pending_runtime(_connection, release):
        match = {
            "id": "X10",
            "score": 72,
            "scene": "media",
            "reason": "fixture gray",
            "source": "desc",
        }
        matcher = SimpleNamespace(
            matcher_rule_sha256="a" * 64,
            thresholds={
                "included_min": 75,
                "review_min": 60,
                "max_secondary": 2,
            },
            match_points=lambda *_args, **_kwargs: [dict(match)],
        )
        return SimpleNamespace(
            release=dict(release),
            taxonomy_version=str(release["taxonomy_version"]),
            taxonomy={"X10": {}},
            allowed_scenes={"X10": {"media"}},
            matcher=matcher,
        )

    def _fake_insufficient_media(self, content_id: int, **kwargs):
        result = self.fixture._fake_media(content_id, **kwargs)
        weak_text = "二手车选购注意检查真实车况"
        self.assertEqual(evaluation._chinese_count(weak_text), 13)
        replacements = {
            "asr": {
                "status": "success",
                "text": weak_text,
            },
            "ocr": {
                "status": "success",
                "combined_text": weak_text,
                "ocr_observation_count": 1,
                "source_count": 1,
            },
        }
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        try:
            for artifact_type, body in replacements.items():
                row = connection.execute(
                    "SELECT id,local_path FROM evidence_artifacts "
                    "WHERE content_id=? AND artifact_type=?",
                    (content_id, artifact_type),
                ).fetchone()
                self.assertIsNotNone(row)
                path = Path(str(row["local_path"]))
                path.write_text(
                    json.dumps(body, ensure_ascii=False, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                connection.execute(
                    "UPDATE evidence_artifacts SET sha256=?,byte_size=? WHERE id=?",
                    (local._sha256_file(path), path.stat().st_size, int(row["id"])),
                )
            connection.commit()
        finally:
            connection.close()
        return result

    def _fake_v2_media(self, content_id: int, **kwargs):
        result = self.fixture._fake_media(content_id, **kwargs)
        weak_text = "二手车选购注意检查真实车况"
        self.assertEqual(evaluation._chinese_count(weak_text), 13)
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT id,local_path FROM evidence_artifacts "
                "WHERE content_id=? AND artifact_type='asr'",
                (content_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            path = Path(str(row["local_path"]))
            path.write_text(
                json.dumps(
                    {"status": "success", "text": weak_text},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            connection.execute(
                "UPDATE evidence_artifacts SET sha256=?,byte_size=? WHERE id=?",
                (local._sha256_file(path), path.stat().st_size, int(row["id"])),
            )
            connection.commit()
        finally:
            connection.close()
        return result

    def _fake_insufficient_evaluation(
        self, content_id: int, *, db_path: Path
    ):
        result = self.fixture._fake_evaluation(content_id, db_path=db_path)
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        try:
            artifacts, _components, evidence_sha = (
                evaluation._current_evidence_state(connection, content_id)
            )
            content = connection.execute(
                "SELECT * FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
            asr = evaluation._read_json(artifacts["asr_path"])
            ocr = evaluation._read_json(artifacts["ocr_path"])
            manual_rows = artifacts["manual_rows"]
            manual_text = "\n".join(
                str(row.get("text_value") or "") for row in manual_rows
            )
            body_text = "\n".join(
                value
                for value in (
                    str(content["title"] or ""),
                    str(content["body"] or ""),
                    manual_text,
                )
                if value
            )
            evidence_level, evidence_summary = evaluation._evidence_level(
                content_type=str(content["content_type"]),
                text=body_text,
                media_path=artifacts["media_path"],
                asr=asr,
                ocr=ocr,
                manual_rows=manual_rows,
            )
            self.assertIn(evidence_level, {"V0", "V1"})
            audience_score, action_score, valid_commenters = (
                evaluation._comment_scores(connection, content_id)
            )
            acquisition_score = evaluation._acquisition_score(
                None, audience_score, None, action_score
            )
            payload = {
                "evaluation_status": "insufficient_evidence",
                "evidence_level": evidence_level,
                "evidence_summary": evidence_summary,
                "primary_selling_point_id": "",
                "selling_point_score": None,
                "selling_point_included": False,
                "pending_review": True,
                "content_direction": "unknown",
                "content_automotive_score": None,
                "audience_automotive_score": audience_score,
                "action_intent_score": action_score,
                "valid_unique_commenters": valid_commenters,
                "acquisition_potential": acquisition_score,
                "matches": [],
                "evaluation_source": "automatic",
                "release_id": "canary-release",
            }
            connection.execute(
                "DELETE FROM evaluation_matches WHERE evaluation_id=?",
                (result.evaluation_id,),
            )
            connection.execute(
                "DELETE FROM review_queue WHERE content_id=?", (content_id,)
            )
            connection.execute(
                """
                UPDATE evaluation_versions
                SET evaluation_status='insufficient_evidence',evidence_level=?,
                    primary_selling_point_code=NULL,selling_point_score=NULL,
                    selling_point_included=0,content_direction='unknown',
                    content_automotive_score=NULL,audience_automotive_score=?,
                    acquisition_potential_score=?,pending_review=1,payload_json=?
                WHERE id=?
                """,
                (
                    evidence_level,
                    audience_score,
                    acquisition_score,
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    result.evaluation_id,
                ),
            )
            connection.execute(
                "UPDATE content_items SET evaluation_content_direction='unknown' "
                "WHERE id=?",
                (content_id,),
            )
            connection.commit()
        finally:
            connection.close()
        return SimpleNamespace(
            evaluation_id=result.evaluation_id,
            evidence_envelope_id=result.evidence_envelope_id,
            content_id=content_id,
            evidence_sha256=evidence_sha,
            evidence_level=evidence_level,
            created=result.created,
        )

    def _clear_source_text(self, content_id: int) -> None:
        raw_path = Path(str(self.source_fixtures[content_id]["raw_path"]))
        raw_body = json.loads(raw_path.read_text())
        raw_body["data"]["title"] = ""
        raw_body["data"]["body"] = ""
        raw_path.write_text(
            json.dumps(raw_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        connection = sqlite3.connect(self.source_db)
        try:
            connection.execute(
                "UPDATE content_items SET title='',body='' WHERE id=?",
                (content_id,),
            )
            connection.execute(
                "UPDATE provider_raw_responses SET sha256=?,byte_size=? "
                "WHERE id=?",
                (
                    local._sha256_file(raw_path),
                    raw_path.stat().st_size,
                    content_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        self.fixture._refresh_step3_proof()

    def _make_source_non_https(self, content_id: int) -> None:
        fixture = self.source_fixtures[content_id]
        raw_path = Path(fixture["raw_path"])
        raw_body = json.loads(raw_path.read_text())
        url = f"http://v{content_id}.rednotecdn.com/non-https.mp4"
        raw_body["data"]["media_urls"] = [url]
        raw_path.write_text(
            json.dumps(raw_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        connection = sqlite3.connect(self.source_db)
        try:
            old_path = Path(
                str(
                    connection.execute(
                        "SELECT local_path FROM evidence_artifacts "
                        "WHERE content_id=? AND artifact_type='media_source'",
                        (content_id,),
                    ).fetchone()[0]
                )
            )
            connection.execute(
                "DELETE FROM evidence_artifacts "
                "WHERE content_id=? AND artifact_type='media_source'",
                (content_id,),
            )
            connection.execute(
                "UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=?",
                (
                    local._sha256_file(raw_path),
                    raw_path.stat().st_size,
                    content_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        old_path.unlink()
        media.store_media_source_manifest(
            content_id,
            media_kind="video",
            urls=[url],
            raw_response_id=content_id,
            db_path=self.source_db,
            media_root=self.source_root,
        )
        fixture["urls"] = [url]
        self.fixture._refresh_step3_proof()

    def _make_source_image(
        self, content_id: int, urls: list[str]
    ) -> None:
        fixture = self.source_fixtures[content_id]
        raw_path = Path(fixture["raw_path"])
        raw_body = json.loads(raw_path.read_text())
        raw_body["data"]["content_type"] = "image"
        raw_body["data"]["media_urls"] = list(urls)
        discovery_id = 9_000 + content_id
        discovery_path = self.raw_root / f"discovery-{content_id}.json"
        platform_content_id = f"canary-{content_id}"
        discovery_body = {
            "data": {
                "aweme_list": [
                    {
                        "aweme_id": platform_content_id,
                        "images": [
                            {
                                "download_url_list": [url],
                                "url_list": [],
                            }
                            for url in urls
                        ],
                    }
                ]
            }
        }
        discovery_path.write_text(
            json.dumps(discovery_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        discovery_sha = local._sha256_file(discovery_path)
        source_captured_at = raw_body["source_captured_at"]
        raw_body["source_raw_response_id"] = discovery_id
        raw_body["source_sha256"] = discovery_sha
        raw_body["derived_from_operation"] = "douyin_user_posts"
        raw_path.write_text(
            json.dumps(raw_body, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        connection = sqlite3.connect(self.source_db)
        try:
            connection.execute(
                "UPDATE content_items SET platform_content_id=? WHERE id=?",
                (platform_content_id, content_id),
            )
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    id,account_id,content_id,provider,operation,local_path,sha256,
                    byte_size,http_status,captured_at,source
                ) VALUES (?,39,NULL,'fixture','douyin_user_posts',?,?,?,200,?,
                          'live_applied')
                ON CONFLICT(id) DO UPDATE SET
                    account_id=excluded.account_id,content_id=NULL,
                    provider=excluded.provider,operation=excluded.operation,
                    local_path=excluded.local_path,sha256=excluded.sha256,
                    byte_size=excluded.byte_size,http_status=excluded.http_status,
                    captured_at=excluded.captured_at,source=excluded.source
                """,
                (
                    discovery_id,
                    str(discovery_path),
                    discovery_sha,
                    discovery_path.stat().st_size,
                    source_captured_at,
                ),
            )
            old_path = Path(
                str(
                    connection.execute(
                        "SELECT local_path FROM evidence_artifacts "
                        "WHERE content_id=? AND artifact_type='media_source'",
                        (content_id,),
                    ).fetchone()[0]
                )
            )
            connection.execute(
                "DELETE FROM evidence_artifacts "
                "WHERE content_id=? AND artifact_type='media_source'",
                (content_id,),
            )
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=?",
                (content_id,),
            )
            connection.execute(
                "UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=?",
                (
                    local._sha256_file(raw_path),
                    raw_path.stat().st_size,
                    content_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        old_path.unlink()
        media.store_media_source_manifest(
            content_id,
            media_kind="image",
            urls=urls,
            raw_response_id=content_id,
            db_path=self.source_db,
            media_root=self.source_root,
        )
        fixture["urls"] = list(urls)
        self.fixture._refresh_step3_proof()

    def _prepare_real_image_low_evidence(
        self, *, clear_source_text: bool = False
    ):
        weak_text = "二手车选购注意检查真实车况"
        self.assertEqual(evaluation._chinese_count(weak_text), 13)
        bodies: dict[str, bytes] = {}
        link_to_content_id: dict[str, int] = {}
        for content_id in (1, 2, 3):
            url = (
                f"https://p{content_id}.douyinpic.com/"
                f"insufficient-{content_id}.jpg"
            )
            self._make_source_image(content_id, [url])
            bodies[url] = b"\xff\xd8\xff" + bytes([64 + content_id]) * 700
            link_to_content_id[
                str(self.source_fixtures[content_id]["link_id"])
            ] = content_id
        if clear_source_text:
            for content_id in (1, 2, 3):
                self._clear_source_text(content_id)

        def open_image(request, **_kwargs):
            url = str(request.full_url)
            return canary_tests._Response(
                url, bodies[url], content_type="image/jpeg"
            )

        def weak_ocr(
            _manifest_path,
            target,
            *,
            binary_path=None,
            validated_frame_paths=None,
        ):
            self.assertIsNotNone(binary_path)
            frames = list(validated_frame_paths or ())
            self.assertEqual(len(frames), 1)
            content_id = link_to_content_id[target.parent.name]
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "processor_version": media.processor_versions()["ocr"],
                    "source_count": 1,
                    "ocr_observation_count": 1,
                    "combined_text": weak_text,
                    "observations": [
                        {
                            "content_id": content_id,
                            "text": weak_text,
                        }
                    ],
                },
            )
            return target

        def materialized_runtime(connection, release):
            runtime = self._fake_review_pending_runtime(connection, release)

            def unexpected_match(*_args, **_kwargs):
                raise AssertionError(
                    "V0/V1 evaluation must not invoke matcher"
                )

            runtime.matcher.match_points = unexpected_match
            return runtime

        return weak_text, open_image, weak_ocr, materialized_runtime

    def _image_download_root(
        self, content_id: int, urls: list[str]
    ) -> Path:
        flat_source_sha256 = media._media_source_identity("image", urls)[1]
        groups = media.douyin_image_source_groups(
            urls, [[url] for url in urls]
        )
        source_sha256 = media.image_download_binding_sha256(
            flat_source_sha256, media.image_groups_sha256(groups)
        )
        return (
            self.media_root
            / str(self.source_fixtures[content_id]["link_id"])
            / "downloads"
            / source_sha256
            / "images"
        )

    def _prepare_real_image_unsupported_wal(
        self,
        *,
        unsupported_content_id: int = 2,
        on_unsupported_failure=None,
    ):
        image_urls = {
            1: ["https://p1.douyinpic.com/canary-1.jpg"],
            2: ["https://p2.douyinpic.com/canary-2.jpg"],
            3: ["https://p3.douyinpic.com/canary-3.jpg"],
        }
        image_urls[unsupported_content_id] = [
            f"https://p{unsupported_content_id}.douyinpic.com/"
            f"canary-{index}.jpg"
            for index in range(10)
        ]
        for content_id, urls in image_urls.items():
            self._make_source_image(content_id, urls)
        real_process_content_media = media.process_content_media
        unsupported_groups = {3, 7, 8}
        bodies: dict[str, tuple[bytes, str]] = {}
        for content_id, urls in image_urls.items():
            for index, url in enumerate(urls):
                if (
                    content_id == unsupported_content_id
                    and index in unsupported_groups
                ):
                    bodies[url] = (b"VVIC" + b"x" * 700, "image/vvic")
                else:
                    bodies[url] = (
                        b"\xff\xd8\xff" + bytes([65 + index]) * 700,
                        "image/jpeg",
                    )

        def open_image(request, **_kwargs):
            url = str(request.full_url)
            body, content_type = bodies[url]
            return canary_tests._Response(
                url, body, content_type=content_type
            )

        def media_effect(content_id: int, **kwargs):
            self.calls["media"] += 1
            if content_id == unsupported_content_id:
                try:
                    return real_process_content_media(content_id, **kwargs)
                except media.MediaProcessingError:
                    if on_unsupported_failure is not None:
                        on_unsupported_failure(
                            self._image_download_root(
                                content_id, image_urls[content_id]
                            )
                        )
                    raise
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with kwargs["urlopen_fn"](request, timeout=90) as response:
                response.read()
            failure_message = (
                f"image download incomplete: later image item {content_id}"
            )
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=f"MediaProcessingError: {failure_message}",
                    media_kind="image",
                )
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        return image_urls, open_image, media_effect

    def _assert_deferred_mutation_blocks(self, mutation) -> None:
        failure_message = "media download failed: injected controlled timeout"

        def fail_open(request, **_kwargs):
            url = str(request.full_url)
            if url in set(self.source_fixtures[2]["urls"]):
                raise urllib.error.URLError("injected controlled timeout")
            return canary_tests._Response(url, b"video" * 1000)

        def media_effect(content_id: int, **kwargs):
            if content_id != 2:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with self.assertRaises(urllib.error.URLError):
                kwargs["urlopen_fn"](request, timeout=90)
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=f"MediaProcessingError: {failure_message}",
                )
                mutation(connection)
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=fail_open),
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())

    def _assert_review_pending_mutation_blocks(
        self, mutation, *, runtime_effect=None
    ) -> None:
        def mutated_review(content_id: int, *, db_path: Path):
            result = self._fake_review_pending_evaluation(
                content_id,
                db_path=db_path,
            )
            if content_id == 2:
                connection = sqlite3.connect(db_path)
                connection.row_factory = sqlite3.Row
                try:
                    mutation(connection, result.evaluation_id)
                    connection.commit()
                finally:
                    connection.close()
            return result

        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=mutated_review,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=(
                runtime_effect or self._fake_review_pending_runtime
            ),
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())
        self.assertFalse((self.run_root / "progress/000002.progress.json").exists())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=mutated_review,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=(
                runtime_effect or self._fake_review_pending_runtime
            ),
        ), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("invalid review recovery opened network"),
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def _assert_insufficient_mutation_blocks(self, mutation) -> None:
        def media_effect(content_id: int, **kwargs):
            if content_id == 2:
                return self._fake_insufficient_media(content_id, **kwargs)
            return self.fixture._fake_media(content_id, **kwargs)

        def mutated_evaluation(content_id: int, *, db_path: Path):
            if content_id != 2:
                return self.fixture._fake_evaluation(
                    content_id, db_path=db_path
                )
            result = self._fake_insufficient_evaluation(
                content_id, db_path=db_path
            )
            connection = sqlite3.connect(db_path)
            connection.row_factory = sqlite3.Row
            try:
                mutation(connection, result.evaluation_id)
                connection.commit()
            finally:
                connection.close()
            return result

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=mutated_evaluation,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())
        self.assertFalse((self.run_root / "progress/000002.progress.json").exists())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("invalid insufficient opened network"),
        ), patch.object(
            media,
            "process_content_media",
            side_effect=AssertionError("invalid insufficient reran media"),
        ), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=AssertionError("invalid insufficient reran evaluator"),
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError("invalid insufficient ran fingerprint"),
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def _insert_retryable_download_slot(
        self,
        connection: sqlite3.Connection,
        *,
        content_id: int,
        error_message: str,
        media_kind: str = "video",
    ) -> None:
        _, source_sha = media._media_source_identity(
            media_kind, self.source_fixtures[content_id]["urls"]
        )
        processor_version = (
            media.IMAGE_DOWNLOAD_VERSION
            if media_kind == "image"
            else media.VIDEO_DOWNLOAD_VERSION
        )
        if media_kind == "image":
            urls = list(self.source_fixtures[content_id]["urls"])
            groups = media.douyin_image_source_groups(
                urls, [[url] for url in urls]
            )
            source_sha = media.image_download_binding_sha256(
                source_sha, media.image_groups_sha256(groups)
            )
        connection.execute(
            """INSERT INTO media_processing_slots(
                   content_id,source_sha256,processor_type,processor_version,
                   status,output_artifact_id,attempt_count,error_message,
                   created_at,updated_at
               ) VALUES (?,?,'download',?,'retryable_failed',NULL,1,?,
                         CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (
                content_id,
                source_sha,
                processor_version,
                error_message,
            ),
        )

    def _fork_apply(self, *extra_patches, **argument_overrides) -> int:
        child = os.fork()
        if child == 0:
            try:
                with self._pipeline_patches(), ExitStack() as stack:
                    for context in extra_patches:
                        stack.enter_context(context)
                    batches.run_batches(
                        **self._batch_arguments(**argument_overrides)
                    )
            except BaseException:
                os._exit(71)
            os._exit(0)
        _pid, wait_status = os.waitpid(child, 0)
        return wait_status

    @staticmethod
    def _mutated(value):
        if value is None:
            return {"tampered": True}
        if isinstance(value, bool):
            return not value
        if isinstance(value, int):
            return value + 1
        if isinstance(value, str):
            return value + "-tampered"
        if isinstance(value, list):
            return [*value, "tampered"]
        if isinstance(value, dict):
            return {**value, "tampered": True}
        raise AssertionError(type(value))

    def _resign_last_deferred_terminal_as_failed(
        self, *, ordinal: int = 3
    ) -> None:
        item_path = self.run_root / f"items/{ordinal:06d}.receipt.json"
        item = json.loads(item_path.read_text())
        ledger_path = self.run_root / f"network/{ordinal:06d}.network.json"
        ledger = json.loads(ledger_path.read_text())
        terminal = ledger["events"][-1]
        self.assertEqual(terminal["outcome"], "succeeded")
        terminal["outcome"] = "failed"
        terminal["error"] = (
            f"{item['failure']['type']}: {item['failure']['message']}"
        )
        terminal["status"] = None
        terminal["mime"] = None
        terminal["declared_bytes"] = None
        terminal["bytes"] = 0
        terminal["charged_bytes"] = 0
        terminal["response_sha256"] = None
        ledger["total_bytes"] = 0
        ledger["budget_consumed_bytes"] = 0
        ledger["overrun"] = False
        ledger_path.write_bytes(local._canonical_bytes(ledger))

        binding = item["result"]["validated"].get("failure_binding")
        if isinstance(binding, dict):
            binding["terminal_event_outcome"] = "failed"
            binding["terminal_event_sha256"] = batches._json_sha(terminal)
            if "terminal_transcript_sha256" in binding:
                binding["terminal_transcript_sha256"] = batches._json_sha(
                    ledger["events"]
                )
            if "terminal_evidence_class" in binding:
                binding["terminal_evidence_class"] = "transport_failed"
            if "terminal_error_policy" in binding:
                binding["terminal_error_policy"] = (
                    "must_be_nonempty_without_response_body"
                )
            if "terminal_error_sha256" in binding:
                binding["terminal_error_sha256"] = batches._json_sha(
                    terminal["error"]
                )
        item["after"]["network_ledger_sha256"] = local._sha256_file(
            ledger_path
        )
        item["after"]["network_budget_consumed_bytes"] = 0
        item_path.write_bytes(local._canonical_bytes(item))

        progress_path = self.run_root / f"progress/{ordinal:06d}.progress.json"
        progress = json.loads(progress_path.read_text())
        progress["item_receipt_sha256"] = local._sha256_file(item_path)
        progress_path.write_bytes(local._canonical_bytes(progress))

        batch_path = self.run_root / "batches/000001.receipt.json"
        batch = json.loads(batch_path.read_text())
        batch["item_receipts"][-1][1] = local._sha256_file(item_path)
        batch["item_receipts_sha256"] = batches._json_sha(
            batch["item_receipts"]
        )
        batch["audit"]["batch_delta"][-1]["receipt_sha256"] = (
            local._sha256_file(item_path)
        )
        batch["audit"]["batch_delta"][-1]["network_ledger_sha256"] = (
            local._sha256_file(ledger_path)
        )
        batch["audit"]["batch_delta_sha256"] = batches._json_sha(
            batch["audit"]["batch_delta"]
        )
        batch["audit"]["logical_head_sha256"] = batches._json_sha(
            {
                "previous_logical_head_sha256": batch["audit"][
                    "previous_logical_head_sha256"
                ],
                "batch_index": 1,
                "batch_delta_sha256": batch["audit"][
                    "batch_delta_sha256"
                ],
            }
        )
        batch_path.write_bytes(local._canonical_bytes(batch))

        completion_path = (
            self.run_root / "completions/000001.completion.json"
        )
        completion = json.loads(completion_path.read_text())
        completion["progress_head_sha256"] = local._sha256_file(
            progress_path
        )
        completion["audit"] = batch["audit"]
        completion_path.write_bytes(local._canonical_bytes(completion))

    def test_default_plan_is_zero_write_and_freezes_three_item_first_batch(self) -> None:
        before = self.fixture._tree_state(self.root)

        with patch.object(local, "_local_tools", return_value=self.tools):
            result = batches.plan_batches(**self._batch_arguments())

        self.assertEqual(result["status"], "planned")
        self.assertFalse(result["apply"])
        self.assertFalse(result["existing_run"])
        self.assertEqual(result["universe_count"], 3)
        self.assertEqual(result["eligible_count"], 3)
        self.assertEqual(result["static_deferred_count"], 0)
        self.assertEqual(result["batch_action"]["content_ids"], [1, 2, 3])
        self.assertEqual(result["provider_calls_planned"], 0)
        self.assertFalse(result["full_history_complete"])
        self.assertEqual(self.fixture._tree_state(self.root), before)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.media_root.exists())
        self.assertFalse(self.run_root.exists())

    def test_v1_insufficient_evidence_is_stable_terminal_not_deferred(
        self,
    ) -> None:
        def media_effect(content_id: int, **kwargs):
            if content_id == 2:
                return self._fake_insufficient_media(content_id, **kwargs)
            return self.fixture._fake_media(content_id, **kwargs)

        def evaluation_effect(content_id: int, *, db_path: Path):
            if content_id == 2:
                return self._fake_insufficient_evaluation(
                    content_id, db_path=db_path
                )
            return self.fixture._fake_evaluation(content_id, db_path=db_path)

        def fingerprint_effect(content_id: int, *, db_path: Path):
            self.assertNotEqual(content_id, 2)
            return self.fixture._fake_fingerprint(content_id, db_path=db_path)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            evaluation, "evaluate_content", side_effect=evaluation_effect
        ), patch.object(
            duplicates, "fingerprint_content", side_effect=fingerprint_effect
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipt = json.loads(
            (self.run_root / "items/000002.receipt.json").read_text()
        )
        completion = json.loads(
            (self.run_root / "completions/000001.completion.json").read_text()
        )
        self.assertEqual(receipt["status"], "insufficient_evidence")
        self.assertIsNone(receipt["failure"])
        self.assertEqual(receipt["result"]["evaluation"]["evidence_level"], "V1")
        self.assertIsNone(receipt["result"]["fingerprint_source_sha256"])
        self.assertFalse(receipt["result"]["validated"]["formal_eligible"])
        self.assertEqual(receipt["result"]["validated"]["fingerprint_files"], 0)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(
            result["eligible"],
            {
                "total": 3,
                "attempted": 3,
                "succeeded": 2,
                "review_pending": 0,
                "runtime_deferred": 0,
                "insufficient_evidence": 1,
            },
        )
        evidence = completion["insufficient_evidence"]
        self.assertEqual(evidence["count"], 1)
        self.assertEqual(len(evidence["batch_delta"]), 1)
        self.assertEqual(
            evidence["batch_delta"][0],
            {
                "ordinal": 2,
                "content_id": 2,
                "evaluation_id": receipt["result"]["validated"]["evaluation_id"],
                "evidence_level": "V1",
                "evidence_sha256": receipt["result"]["evaluation"][
                    "evidence_sha256"
                ],
                "insufficient_binding_sha256": batches._json_sha(
                    {
                        "evaluation": receipt["result"]["evaluation"],
                        "validated": receipt["result"]["validated"],
                    }
                ),
            },
        )
        self.assertEqual(completion["runtime_deferred"]["count"], 0)
        self.assertEqual(completion["review_pending"]["count"], 0)
        self.assertFalse(completion["full_history_complete"])
        self.assertFalse(completion["publication_allowed"])
        with closing(local._immutable_connection(self.db)) as connection:
            evaluation_row = connection.execute(
                "SELECT evaluation_status,evidence_level,pending_review "
                "FROM evaluation_versions WHERE content_id=2"
            ).fetchone()
            queue_count = connection.execute(
                "SELECT COUNT(*) FROM review_queue WHERE content_id=2"
            ).fetchone()[0]
            fingerprint_count = connection.execute(
                "SELECT COUNT(*) FROM duplicate_fingerprints WHERE content_id=2"
            ).fetchone()[0]
        self.assertEqual(
            tuple(evaluation_row), ("insufficient_evidence", "V1", 1)
        )
        self.assertEqual(queue_count, 0)
        self.assertEqual(fingerprint_count, 0)
        stable_tree = self.fixture._tree_state(self.analysis_root)
        with self._pipeline_patches(), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("insufficient idempotency opened network"),
        ), patch.object(
            media,
            "process_content_media",
            side_effect=AssertionError("insufficient idempotency reran media"),
        ), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=AssertionError(
                "insufficient idempotency reran evaluation"
            ),
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError(
                "insufficient idempotency ran fingerprint"
            ),
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            second = batches.run_batches(**self._batch_arguments())
        self.assertTrue(second["idempotent"])
        self.assertEqual(second["processed_this_invocation"], 0)
        self.assertEqual(second["status"], "partial")
        self.assertEqual(
            self.fixture._tree_state(self.analysis_root), stable_tree
        )

    def test_v0_insufficient_evidence_is_same_stable_terminal(self) -> None:
        self._clear_source_text(2)

        def media_effect(content_id: int, **kwargs):
            if content_id == 2:
                return self._fake_insufficient_media(content_id, **kwargs)
            return self.fixture._fake_media(content_id, **kwargs)

        def evaluation_effect(content_id: int, *, db_path: Path):
            if content_id == 2:
                return self._fake_insufficient_evaluation(
                    content_id, db_path=db_path
                )
            return self.fixture._fake_evaluation(content_id, db_path=db_path)

        def fingerprint_effect(content_id: int, *, db_path: Path):
            self.assertNotEqual(content_id, 2)
            return self.fixture._fake_fingerprint(content_id, db_path=db_path)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            evaluation, "evaluate_content", side_effect=evaluation_effect
        ), patch.object(
            duplicates, "fingerprint_content", side_effect=fingerprint_effect
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipt = json.loads(
            (self.run_root / "items/000002.receipt.json").read_text()
        )
        self.assertEqual(receipt["status"], "insufficient_evidence")
        self.assertEqual(receipt["result"]["evaluation"]["evidence_level"], "V0")
        self.assertEqual(result["eligible"]["insufficient_evidence"], 1)

    def test_real_image_manifest_ocr_only_13_chinese_is_v1_insufficient(
        self,
    ) -> None:
        weak_text, open_image, weak_ocr, materialized_runtime = (
            self._prepare_real_image_low_evidence()
        )

        with patch.object(
            local, "_local_tools", return_value=self.tools
        ), patch.object(
            media, "_run_ocr", side_effect=weak_ocr
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            new=materialized_runtime,
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError("image V1 must not fingerprint"),
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipts = [
            json.loads(
                (self.run_root / f"items/{ordinal:06d}.receipt.json").read_text()
            )
            for ordinal in (1, 2, 3)
        ]
        completion = json.loads(
            (self.run_root / "completions/000001.completion.json").read_text()
        )
        ledger = json.loads(
            (self.run_root / "network/000002.network.json").read_text()
        )
        self.assertEqual(
            [receipt["status"] for receipt in receipts],
            ["insufficient_evidence"] * 3,
        )
        self.assertTrue(all(receipt["failure"] is None for receipt in receipts))
        self.assertEqual(
            receipts[1]["result"]["evaluation"]["evidence_level"], "V1"
        )
        self.assertEqual(
            receipts[1]["result"]["media"]["media_kind"], "image"
        )
        self.assertEqual(
            set(receipts[1]["result"]["media"]["artifacts"]),
            {"media", "ocr"},
        )
        self.assertIsNone(
            receipts[1]["result"]["fingerprint_source_sha256"]
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["eligible"]["insufficient_evidence"], 3)
        self.assertEqual(completion["insufficient_evidence"]["count"], 3)
        self.assertFalse(completion["publication_allowed"])
        self.assertTrue(
            (self.run_root / "items/000003.receipt.json").is_file()
        )
        self.assertTrue(
            (self.run_root / "progress/000003.progress.json").is_file()
        )
        self.assertEqual(len(ledger["events"]), 1)
        self.assertEqual(ledger["events"][0]["outcome"], "succeeded")
        self.assertEqual(ledger["events"][0]["mime"], "image/jpeg")
        with closing(local._immutable_connection(self.db)) as connection:
            artifact_rows = connection.execute(
                "SELECT artifact_type,local_path FROM evidence_artifacts "
                "WHERE content_id=2 AND artifact_type!='media_source' "
                "ORDER BY artifact_type"
            ).fetchall()
            evaluation_row = connection.execute(
                "SELECT evaluation_status,evidence_level,pending_review "
                "FROM evaluation_versions WHERE content_id=2"
            ).fetchone()
            queue_count = connection.execute(
                "SELECT COUNT(*) FROM review_queue WHERE content_id=2"
            ).fetchone()[0]
            fingerprint_count = connection.execute(
                "SELECT COUNT(*) FROM duplicate_fingerprints WHERE content_id=2"
            ).fetchone()[0]
            slot_types = {
                str(row["processor_type"])
                for row in connection.execute(
                    "SELECT processor_type FROM media_processing_slots "
                    "WHERE content_id=2"
                )
            }
        self.assertEqual(
            [str(row["artifact_type"]) for row in artifact_rows],
            ["media_manifest", "ocr"],
        )
        manifest = json.loads(Path(str(artifact_rows[0]["local_path"])).read_text())
        ocr = json.loads(Path(str(artifact_rows[1]["local_path"])).read_text())
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(len(manifest["image_paths"]), 1)
        self.assertEqual(ocr["combined_text"], weak_text)
        self.assertEqual(evaluation._chinese_count(ocr["combined_text"]), 13)
        self.assertEqual(
            tuple(evaluation_row), ("insufficient_evidence", "V1", 1)
        )
        self.assertEqual(queue_count, 0)
        self.assertEqual(fingerprint_count, 0)
        self.assertEqual(slot_types, {"download", "ocr"})

    def test_real_image_manifest_ocr_only_without_source_text_is_v0(
        self,
    ) -> None:
        _weak_text, open_image, weak_ocr, materialized_runtime = (
            self._prepare_real_image_low_evidence(clear_source_text=True)
        )
        with patch.object(
            local, "_local_tools", return_value=self.tools
        ), patch.object(
            media, "_run_ocr", side_effect=weak_ocr
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            new=materialized_runtime,
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError("image V0 must not fingerprint"),
        ):
            result = batches.run_batches(**self._batch_arguments())
        receipts = [
            json.loads(
                (self.run_root / f"items/{ordinal:06d}.receipt.json").read_text()
            )
            for ordinal in (1, 2, 3)
        ]
        self.assertEqual(
            [receipt["status"] for receipt in receipts],
            ["insufficient_evidence"] * 3,
        )
        self.assertEqual(
            [
                receipt["result"]["evaluation"]["evidence_level"]
                for receipt in receipts
            ],
            ["V0"] * 3,
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["eligible"]["insufficient_evidence"], 3)

    def test_real_image_v1_postcommit_sigkill_recovers_without_replay(
        self,
    ) -> None:
        _weak_text, open_image, weak_ocr, materialized_runtime = (
            self._prepare_real_image_low_evidence()
        )

        def kill_after_database_commit(content_id: int) -> None:
            if content_id == 3:
                os.kill(os.getpid(), signal.SIGKILL)

        child = os.fork()
        if child == 0:
            try:
                with patch.object(
                    local, "_local_tools", return_value=self.tools
                ), patch.object(
                    media, "_run_ocr", side_effect=weak_ocr
                ), patch.object(
                    local.urllib.request,
                    "build_opener",
                    return_value=SimpleNamespace(open=open_image),
                ), patch.object(
                    evaluation,
                    "_load_release_runtime",
                    new=materialized_runtime,
                ), patch.object(
                    duplicates,
                    "fingerprint_content",
                    side_effect=AssertionError(
                        "image V1 must not fingerprint"
                    ),
                ), patch.object(
                    batches,
                    "_after_item_database_commit",
                    new=kill_after_database_commit,
                ):
                    batches.run_batches(**self._batch_arguments())
            except BaseException:
                os._exit(71)
            os._exit(0)
        _pid, wait_status = os.waitpid(child, 0)
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "items/000003.intent.json").is_file())
        self.assertFalse((self.run_root / "items/000003.receipt.json").exists())
        ledger_path = self.run_root / "network/000003.network.json"
        ledger_before = ledger_path.read_bytes()
        database_before = self.db.read_bytes()
        sidecars_before = {
            path.name: path.read_bytes()
            for path in local._database_sidecars(self.db)
        }
        media_before = self.fixture._tree_state(self.media_root)

        with patch.object(
            local, "_local_tools", return_value=self.tools
        ), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("image V1 recovery opened network"),
        ), patch.object(
            media,
            "process_content_media",
            side_effect=AssertionError("image V1 recovery reran media"),
        ), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=AssertionError("image V1 recovery reran evaluation"),
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError("image V1 recovery ran fingerprint"),
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            new=materialized_runtime,
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipt = json.loads(
            (self.run_root / "items/000003.receipt.json").read_text()
        )
        completion = json.loads(
            (self.run_root / "completions/000001.completion.json").read_text()
        )
        self.assertEqual(receipt["status"], "insufficient_evidence")
        self.assertEqual(
            receipt["result"]["evaluation"]["evidence_level"], "V1"
        )
        self.assertTrue(receipt["recovered_after_commit"])
        self.assertIsNone(receipt["failure"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(completion["insufficient_evidence"]["count"], 3)
        self.assertEqual(self.db.read_bytes(), database_before)
        self.assertEqual(
            {
                path.name: path.read_bytes()
                for path in local._database_sidecars(self.db)
            },
            sidecars_before,
        )
        self.assertEqual(ledger_path.read_bytes(), ledger_before)
        self.assertEqual(self.fixture._tree_state(self.media_root), media_before)
        self.assertTrue(
            (self.run_root / "progress/000003.progress.json").is_file()
        )
        self.assertTrue(
            (self.run_root / "batches/000001.receipt.json").is_file()
        )

    def test_insufficient_multiple_active_evaluations_are_zero_write_blocked(
        self,
    ) -> None:
        def add_second_active(connection, evaluation_id):
            row = connection.execute(
                "SELECT * FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            columns = [name for name in row.keys() if name != "id"]
            values = [row[name] for name in columns]
            values[columns.index("evidence_sha256")] = "b" * 64
            connection.execute(
                "INSERT INTO evaluation_versions("
                + ",".join(columns)
                + ") VALUES ("
                + ",".join("?" for _ in columns)
                + ")",
                values,
            )

        self._assert_insufficient_mutation_blocks(add_second_active)

    def test_insufficient_evaluator_summary_drift_is_zero_write_blocked(
        self,
    ) -> None:
        def drift_summary(connection, evaluation_id):
            row = connection.execute(
                "SELECT payload_json FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            payload = json.loads(row["payload_json"])
            payload["evidence_summary"] = "tampered but internally canonical"
            connection.execute(
                "UPDATE evaluation_versions SET payload_json=? WHERE id=?",
                (
                    evaluation.canonical_json(payload),
                    evaluation_id,
                ),
            )

        self._assert_insufficient_mutation_blocks(drift_summary)

    def test_v1_postcommit_receipt_gap_recovers_without_any_pipeline_replay(
        self,
    ) -> None:
        def media_effect(content_id: int, **kwargs):
            if content_id == 3:
                return self._fake_insufficient_media(content_id, **kwargs)
            return self.fixture._fake_media(content_id, **kwargs)

        def evaluation_then_kill(content_id: int, *, db_path: Path):
            if content_id != 3:
                return self.fixture._fake_evaluation(content_id, db_path=db_path)
            self._fake_insufficient_evaluation(content_id, db_path=db_path)
            os.kill(os.getpid(), signal.SIGKILL)
            raise AssertionError("SIGKILL did not terminate child")

        wait_status = self._fork_apply(
            patch.object(media, "process_content_media", side_effect=media_effect),
            patch.object(
                evaluation,
                "evaluate_content",
                side_effect=evaluation_then_kill,
            ),
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "items/000003.intent.json").is_file())
        self.assertFalse((self.run_root / "items/000003.receipt.json").exists())
        ledger_path = self.run_root / "network/000003.network.json"
        ledger_before = ledger_path.read_bytes()
        db_before = local._sha256_file(self.db)
        media_before = self.fixture._tree_state(self.media_root)

        with self._pipeline_patches(), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("V1 receipt recovery opened network"),
        ), patch.object(
            media,
            "process_content_media",
            side_effect=AssertionError("V1 receipt recovery reran media"),
        ), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=AssertionError("V1 receipt recovery reran evaluation"),
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError("V1 receipt recovery ran fingerprint"),
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipt = json.loads(
            (self.run_root / "items/000003.receipt.json").read_text()
        )
        self.assertEqual(receipt["status"], "insufficient_evidence")
        self.assertTrue(receipt["recovered_after_commit"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(local._sha256_file(self.db), db_before)
        self.assertEqual(ledger_path.read_bytes(), ledger_before)
        self.assertEqual(self.fixture._tree_state(self.media_root), media_before)

    def test_insufficient_pre_evaluation_commit_sigkill_is_manual_block(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self.profile = batches.HistoryProfile(
            universe_count=4,
            eligible_count=4,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3, 4),
        )

        def evaluation_kill_before_commit(content_id: int, *, db_path: Path):
            if content_id != 3:
                return self.fixture._fake_evaluation(
                    content_id, db_path=db_path
                )
            os.kill(os.getpid(), signal.SIGKILL)
            raise AssertionError("SIGKILL did not terminate child")

        wait_status = self._fork_apply(
            patch.object(
                evaluation,
                "evaluate_content",
                side_effect=evaluation_kill_before_commit,
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "items/000003.intent.json").is_file())
        self.assertFalse((self.run_root / "items/000003.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000004.intent.json").exists())
        before = self.fixture._tree_state(self.analysis_root)

        with self._pipeline_patches(), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("pre-evaluation recovery opened network"),
        ), patch.object(
            media,
            "process_content_media",
            side_effect=AssertionError("pre-evaluation recovery reran media"),
        ), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=AssertionError(
                "pre-evaluation recovery reran evaluator"
            ),
        ), patch.object(
            duplicates,
            "fingerprint_content",
            side_effect=AssertionError(
                "pre-evaluation recovery ran fingerprint"
            ),
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "manual_required"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertFalse((self.run_root / "items/000003.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000004.intent.json").exists())

    def test_terminal_status_contract_requires_v3_schema(self) -> None:
        self.assertEqual(
            batches.SCHEMA_VERSION,
            "full-local-analysis-batches-v3",
        )

    def test_insufficient_completion_chain_is_independent_and_tamper_evident(
        self,
    ) -> None:
        for content_id in (4, 5, 6, 7):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        insufficient_ids = frozenset({2, 4, 6})

        def media_effect(content_id: int, **kwargs):
            if content_id in insufficient_ids:
                return self._fake_insufficient_media(content_id, **kwargs)
            return self.fixture._fake_media(content_id, **kwargs)

        def evaluation_effect(content_id: int, *, db_path: Path):
            if content_id in insufficient_ids:
                return self._fake_insufficient_evaluation(
                    content_id, db_path=db_path
                )
            return self.fixture._fake_evaluation(content_id, db_path=db_path)

        def fingerprint_effect(content_id: int, *, db_path: Path):
            self.assertNotIn(content_id, insufficient_ids)
            return self.fixture._fake_fingerprint(content_id, db_path=db_path)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            evaluation, "evaluate_content", side_effect=evaluation_effect
        ), patch.object(
            duplicates, "fingerprint_content", side_effect=fingerprint_effect
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            first = batches.run_batches(**self._batch_arguments())
            second = batches.run_batches(
                **self._batch_arguments(through_batch=2)
            )
            third = batches.run_batches(
                **self._batch_arguments(through_batch=3)
            )

        self.assertEqual(
            [first["status"], second["status"], third["status"]],
            ["partial", "partial", "partial"],
        )
        completion_paths = [
            self.run_root / f"completions/{index:06d}.completion.json"
            for index in (1, 2, 3)
        ]
        completions = [
            json.loads(path.read_text()) for path in completion_paths
        ]
        self.assertEqual(
            [row["insufficient_evidence"]["count"] for row in completions],
            [1, 2, 3],
        )
        self.assertEqual(
            [
                [item["ordinal"] for item in row["insufficient_evidence"]["batch_delta"]]
                for row in completions
            ],
            [[2], [4], [6]],
        )
        self.assertTrue(
            all(row["review_pending"]["count"] == 0 for row in completions)
        )
        self.assertTrue(
            all(row["runtime_deferred"]["count"] == 0 for row in completions)
        )

        second_completion = completions[1]
        second_chain = second_completion["insufficient_evidence"]
        second_chain["batch_delta"][0]["evaluation_id"] += 1000
        second_chain["batch_delta_sha256"] = batches._json_sha(
            second_chain["batch_delta"]
        )
        second_chain["head_sha256"] = batches._json_sha(
            {
                "previous_head_sha256": second_chain[
                    "previous_head_sha256"
                ],
                "completion_sequence": 2,
                "batch_delta_sha256": second_chain[
                    "batch_delta_sha256"
                ],
            }
        )
        completion_paths[1].write_bytes(
            local._canonical_bytes(second_completion)
        )
        third_completion = completions[2]
        third_completion["previous_completion_sha256"] = local._sha256_file(
            completion_paths[1]
        )
        third_chain = third_completion["insufficient_evidence"]
        third_chain["previous_head_sha256"] = second_chain["head_sha256"]
        third_chain["head_sha256"] = batches._json_sha(
            {
                "previous_head_sha256": third_chain[
                    "previous_head_sha256"
                ],
                "completion_sequence": 3,
                "batch_delta_sha256": third_chain[
                    "batch_delta_sha256"
                ],
            }
        )
        completion_paths[2].write_bytes(
            local._canonical_bytes(third_completion)
        )
        before = self.fixture._tree_state(self.analysis_root)

        with self._pipeline_patches(), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "insufficient_evidence batch delta|completion 2.*重派生",
        ):
            batches.run_batches(**self._batch_arguments(through_batch=3))
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)

    def test_v2_v3_and_deferred_remain_distinct_under_v3(self) -> None:
        failure_message = "media download failed: v3 regression timeout"

        def fail_open(request, **_kwargs):
            url = str(request.full_url)
            if url in set(self.source_fixtures[3]["urls"]):
                raise urllib.error.URLError("v3 regression timeout")
            return canary_tests._Response(url, b"video" * 1000)

        def media_effect(content_id: int, **kwargs):
            if content_id == 2:
                return self._fake_v2_media(content_id, **kwargs)
            if content_id != 3:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(
                kwargs["download_urls"][0]
            )
            with self.assertRaises(urllib.error.URLError):
                kwargs["urlopen_fn"](request, timeout=90)
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=(
                        f"MediaProcessingError: {failure_message}"
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=fail_open),
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipts = [
            json.loads(
                (self.run_root / f"items/{ordinal:06d}.receipt.json").read_text()
            )
            for ordinal in (1, 2, 3)
        ]
        completion = json.loads(
            (self.run_root / "completions/000001.completion.json").read_text()
        )
        self.assertEqual(
            [receipt["status"] for receipt in receipts],
            ["succeeded", "succeeded", "deferred"],
        )
        self.assertEqual(
            [
                receipts[index]["result"]["evaluation"]["evidence_level"]
                for index in (0, 1)
            ],
            ["V3", "V2"],
        )
        self.assertEqual(
            result["eligible"],
            {
                "total": 3,
                "attempted": 3,
                "succeeded": 2,
                "review_pending": 0,
                "runtime_deferred": 1,
                "insufficient_evidence": 0,
            },
        )
        self.assertEqual(completion["insufficient_evidence"]["count"], 0)
        self.assertEqual(
            completion["insufficient_evidence"]["batch_delta"], []
        )
        with closing(local._immutable_connection(self.db)) as connection:
            fingerprint_ids = {
                int(row["content_id"])
                for row in connection.execute(
                    "SELECT content_id FROM duplicate_fingerprints"
                )
            }
        self.assertEqual(fingerprint_ids, {1, 2})

    def test_review_pending_is_distinct_terminal_continues_and_is_idempotent(
        self,
    ) -> None:
        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=self._fake_review_pending_evaluation,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            first = batches.run_batches(**self._batch_arguments())

        receipts = [
            json.loads(
                (self.run_root / f"items/{ordinal:06d}.receipt.json").read_text()
            )
            for ordinal in range(1, 4)
        ]
        completion = json.loads(
            (self.run_root / "completions/000001.completion.json").read_text()
        )
        self.assertEqual(
            [receipt["status"] for receipt in receipts],
            ["succeeded", "review_pending", "succeeded"],
        )
        self.assertIsNone(receipts[1]["failure"])
        self.assertIs(receipts[1]["result"]["validated"]["formal_eligible"], False)
        self.assertIs(receipts[1]["result"]["validated"]["review_pending"], True)
        self.assertIs(type(receipts[1]["result"]["validated"]["evaluation_id"]), int)
        self.assertIs(type(receipts[1]["result"]["validated"]["queue_id"]), int)
        self.assertEqual(
            set(receipts[1]["result"]),
            {
                "content_id",
                "media",
                "evaluation",
                "fingerprint_source_sha256",
                "network_transcript",
                "network_transcript_sha256",
                "validated",
            },
        )
        second_network_path = self.run_root / "network/000002.network.json"
        second_network = json.loads(second_network_path.read_text())
        self.assertEqual(len(second_network["events"]), 1)
        self.assertEqual(second_network["events"][0]["outcome"], "succeeded")
        with closing(local._immutable_connection(self.db)) as connection:
            evaluation_row = connection.execute(
                """
                SELECT * FROM evaluation_versions
                WHERE content_id=2 AND invalidated_at IS NULL
                """
            ).fetchone()
            queue_row = connection.execute(
                "SELECT * FROM review_queue WHERE content_id=2"
            ).fetchone()
            formal = local.evaluation_selectors_module.formal_eligible_release_evaluations(
                connection,
                "canary-release",
                [2],
            )
        self.assertEqual(evaluation_row["evaluation_source"], "automatic")
        self.assertIn(evaluation_row["evidence_level"], {"V2", "V3"})
        self.assertIs(type(evaluation_row["pending_review"]), int)
        self.assertEqual(evaluation_row["pending_review"], 1)
        self.assertIs(json.loads(evaluation_row["payload_json"])["pending_review"], True)
        self.assertEqual(set(formal), set())
        self.assertIs(type(queue_row["id"]), int)
        self.assertIs(type(queue_row["evaluation_id"]), int)
        self.assertIs(type(queue_row["priority"]), int)
        self.assertEqual(queue_row["evaluation_id"], evaluation_row["id"])
        self.assertEqual(queue_row["reason_code"], "evaluation_gray_zone")
        self.assertEqual(queue_row["priority"], 50)
        self.assertEqual(queue_row["status"], "pending")
        self.assertIsNone(queue_row["assigned_to"])
        self.assertIsNone(queue_row["resolved_at"])
        self.assertEqual(queue_row["created_at"], queue_row["updated_at"])
        self.assertEqual(first["status"], "partial")
        self.assertEqual(
            first["eligible"],
            {
                "total": 3,
                "attempted": 3,
                "succeeded": 2,
                "review_pending": 1,
                "runtime_deferred": 0,
                "insufficient_evidence": 0,
            },
        )
        self.assertEqual(completion["review_pending"]["count"], 1)
        self.assertEqual(
            completion["review_pending"]["batch_delta"],
            [
                {
                    "ordinal": 2,
                    "content_id": 2,
                    "evaluation_id": receipts[1]["result"]["validated"][
                        "evaluation_id"
                    ],
                    "queue_id": receipts[1]["result"]["validated"]["queue_id"],
                    "review_binding_sha256": batches._json_sha(
                        receipts[1]["result"]["validated"]
                    ),
                }
            ],
        )
        self.assertEqual(
            self.calls,
            {"media": 3, "evaluation": 3, "fingerprint": 3},
        )
        before = self.fixture._tree_state(self.analysis_root)
        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=self._fake_review_pending_evaluation,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("idempotent review run opened network"),
        ):
            second = batches.run_batches(**self._batch_arguments())
        self.assertTrue(second["idempotent"])
        self.assertEqual(second["processed_this_invocation"], 0)
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(
            self.calls,
            {"media": 3, "evaluation": 3, "fingerprint": 3},
        )
        self.assertEqual(
            json.loads(second_network_path.read_text())["events"],
            second_network["events"],
        )

    def test_review_pending_postcommit_receipt_gap_recovers_without_replay(
        self,
    ) -> None:
        def pending_third(content_id: int, *, db_path: Path):
            return self._fake_review_pending_evaluation(
                content_id,
                db_path=db_path,
                pending_content_ids=frozenset({3}),
            )

        def kill_after_third_commit(content_id: int) -> None:
            if content_id == 3:
                os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                evaluation,
                "evaluate_content",
                side_effect=pending_third,
            ),
            patch.object(
                evaluation,
                "_load_release_runtime",
                side_effect=self._fake_review_pending_runtime,
            ),
            patch.object(
                batches,
                "_after_item_database_commit",
                side_effect=kill_after_third_commit,
            ),
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "items/000003.intent.json").is_file())
        self.assertTrue((self.run_root / "network/000003.network.json").is_file())
        self.assertFalse((self.run_root / "items/000003.receipt.json").exists())
        self.assertFalse((self.run_root / "progress/000003.progress.json").exists())
        self.assertFalse((self.run_root / "batches/000001.receipt.json").exists())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )
        ledger_path = self.run_root / "network/000003.network.json"
        ledger_before = ledger_path.read_bytes()
        ledger_sha_before = local._sha256_file(ledger_path)
        ledger_value = json.loads(ledger_before)
        self.assertEqual(len(ledger_value["events"]), 1)
        self.assertEqual(ledger_value["events"][0]["outcome"], "succeeded")
        with closing(local._immutable_connection(self.db)) as connection:
            evaluation_before = connection.execute(
                """
                SELECT id,pending_review FROM evaluation_versions
                WHERE content_id=3 AND invalidated_at IS NULL
                """
            ).fetchone()
            queue_before = connection.execute(
                "SELECT id,evaluation_id FROM review_queue WHERE content_id=3"
            ).fetchone()
            fingerprint_count_before = connection.execute(
                "SELECT COUNT(*) FROM duplicate_fingerprints WHERE content_id=3"
            ).fetchone()[0]
        self.assertIsNotNone(evaluation_before)
        self.assertIsNotNone(queue_before)
        self.assertIs(type(evaluation_before["pending_review"]), int)
        self.assertEqual(evaluation_before["pending_review"], 1)
        self.assertEqual(queue_before["evaluation_id"], evaluation_before["id"])
        self.assertEqual(fingerprint_count_before, 1)

        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=pending_third,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("receipt recovery opened network"),
        ):
            result = batches.run_batches(**self._batch_arguments())

        third = json.loads(
            (self.run_root / "items/000003.receipt.json").read_text()
        )
        self.assertEqual(third["status"], "review_pending")
        self.assertTrue(third["recovered_after_commit"])
        self.assertIsNone(third["failure"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(self.calls, {"media": 0, "evaluation": 0, "fingerprint": 0})
        self.assertEqual(ledger_path.read_bytes(), ledger_before)
        self.assertEqual(local._sha256_file(ledger_path), ledger_sha_before)
        with closing(local._immutable_connection(self.db)) as connection:
            evaluations = connection.execute(
                """
                SELECT id,pending_review FROM evaluation_versions
                WHERE content_id=3 AND invalidated_at IS NULL
                """
            ).fetchall()
            queues = connection.execute(
                "SELECT id,evaluation_id FROM review_queue WHERE content_id=3"
            ).fetchall()
            fingerprints = connection.execute(
                "SELECT id FROM duplicate_fingerprints WHERE content_id=3"
            ).fetchall()
        self.assertEqual(
            [(row["id"], row["pending_review"]) for row in evaluations],
            [(evaluation_before["id"], 1)],
        )
        self.assertEqual(
            [(row["id"], row["evaluation_id"]) for row in queues],
            [(queue_before["id"], evaluation_before["id"])],
        )
        self.assertEqual(len(fingerprints), 1)

    def test_review_pending_requires_unique_gray_queue(self) -> None:
        def remove_queue(connection, _evaluation_id):
            connection.execute("DELETE FROM review_queue WHERE content_id=2")

        self._assert_review_pending_mutation_blocks(remove_queue)

    def test_review_pending_requires_queue_status_pending(self) -> None:
        def move_queue_to_in_review(connection, _evaluation_id):
            connection.execute(
                "UPDATE review_queue SET status='in_review' WHERE content_id=2"
            )

        self._assert_review_pending_mutation_blocks(move_queue_to_in_review)

    def test_review_pending_requires_active_automatic_evaluation(self) -> None:
        def migrate_source(connection, evaluation_id):
            row = connection.execute(
                "SELECT payload_json FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            payload = json.loads(row["payload_json"])
            payload["evaluation_source"] = "migrated_from_v5"
            connection.execute(
                """
                UPDATE evaluation_versions
                SET evaluation_source='migrated_from_v5',payload_json=?
                WHERE id=?
                """,
                (evaluation.canonical_json(payload), evaluation_id),
            )

        self._assert_review_pending_mutation_blocks(migrate_source)

    def test_review_pending_requires_json_boolean_true(self) -> None:
        def replace_boolean_with_integer(connection, evaluation_id):
            row = connection.execute(
                "SELECT payload_json FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            payload = json.loads(row["payload_json"])
            payload["pending_review"] = 1
            connection.execute(
                "UPDATE evaluation_versions SET payload_json=? WHERE id=?",
                (evaluation.canonical_json(payload), evaluation_id),
            )

        self._assert_review_pending_mutation_blocks(replace_boolean_with_integer)

    def test_review_pending_rejects_self_consistent_stored_match_forge(
        self,
    ) -> None:
        def forge_match(connection, evaluation_id):
            row = connection.execute(
                "SELECT payload_json FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            payload = json.loads(row["payload_json"])
            forged = dict(payload["matches"][0])
            forged["score"] = 70
            payload["matches"] = [forged]
            payload["selling_point_score"] = 70
            connection.execute(
                """
                UPDATE evaluation_versions
                SET selling_point_score=70,payload_json=? WHERE id=?
                """,
                (evaluation.canonical_json(payload), evaluation_id),
            )
            connection.execute(
                """
                UPDATE evaluation_matches SET score=70,evidence_json=?
                WHERE evaluation_id=? AND selling_point_code='X10'
                """,
                (evaluation.canonical_json(forged), evaluation_id),
            )

        self._assert_review_pending_mutation_blocks(forge_match)

    def test_review_pending_rejects_matcher_scene_outside_taxonomy_contract(
        self,
    ) -> None:
        def forge_scene(connection, evaluation_id):
            row = connection.execute(
                "SELECT payload_json FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            payload = json.loads(row["payload_json"])
            forged = dict(payload["matches"][0])
            forged["scene"] = "new_car"
            payload["matches"] = [forged]
            payload["content_direction"] = "new_car"
            connection.execute(
                """
                UPDATE evaluation_versions
                SET content_direction='new_car',payload_json=? WHERE id=?
                """,
                (evaluation.canonical_json(payload), evaluation_id),
            )
            connection.execute(
                """
                UPDATE evaluation_matches SET scene='new_car',evidence_json=?
                WHERE evaluation_id=? AND selling_point_code='X10'
                """,
                (evaluation.canonical_json(forged), evaluation_id),
            )
            connection.execute(
                """
                UPDATE content_items SET evaluation_content_direction='new_car'
                WHERE id=2
                """
            )

        def invalid_scene_runtime(_connection, release):
            runtime = self._fake_review_pending_runtime(
                _connection, release
            )
            match = {
                "id": "X10",
                "score": 72,
                "scene": "new_car",
                "reason": "fixture gray",
                "source": "desc",
            }
            runtime.matcher.match_points = (
                lambda *_args, **_kwargs: [dict(match)]
            )
            return runtime

        self._assert_review_pending_mutation_blocks(
            forge_scene,
            runtime_effect=invalid_scene_runtime,
        )

    def test_review_pending_completion_chain_is_cumulative_across_batches(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self.fixture._add_source_content(5)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=self._fake_review_pending_evaluation,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            first = batches.run_batches(**self._batch_arguments())
            second = batches.run_batches(
                **self._batch_arguments(through_batch=2)
            )

        first_path = self.run_root / "completions/000001.completion.json"
        second_path = self.run_root / "completions/000002.completion.json"
        first_completion = json.loads(first_path.read_text())
        second_completion = json.loads(second_path.read_text())
        first_review = first_completion["review_pending"]
        second_review = second_completion["review_pending"]
        self.assertEqual(first["status"], "partial")
        self.assertEqual(second["status"], "partial")
        self.assertEqual(first_review["count"], 1)
        self.assertEqual(len(first_review["batch_delta"]), 1)
        self.assertEqual(first_review["batch_delta"][0]["ordinal"], 2)
        self.assertEqual(second_review["count"], 1)
        self.assertEqual(second_review["batch_delta"], [])
        self.assertEqual(
            second_review["previous_head_sha256"],
            first_review["head_sha256"],
        )
        self.assertEqual(
            second_review["head_sha256"],
            batches._json_sha(
                {
                    "previous_head_sha256": first_review["head_sha256"],
                    "completion_sequence": 2,
                    "batch_delta_sha256": batches._json_sha([]),
                }
            ),
        )
        self.assertEqual(
            second_completion["previous_completion_sha256"],
            local._sha256_file(first_path),
        )
        self.assertEqual(
            second["eligible"],
            {
                "total": 5,
                "attempted": 5,
                "succeeded": 4,
                "review_pending": 1,
                "runtime_deferred": 0,
                "insufficient_evidence": 0,
            },
        )

    def test_review_pending_completion_rejects_self_consistent_id_forge(
        self,
    ) -> None:
        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=self._fake_review_pending_evaluation,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            batches.run_batches(**self._batch_arguments())
        completion_path = self.run_root / "completions/000001.completion.json"
        completion = json.loads(completion_path.read_text())
        review = completion["review_pending"]
        review["batch_delta"][0]["evaluation_id"] += 1
        review["batch_delta_sha256"] = batches._json_sha(
            review["batch_delta"]
        )
        review["head_sha256"] = batches._json_sha(
            {
                "previous_head_sha256": review["previous_head_sha256"],
                "completion_sequence": 1,
                "batch_delta_sha256": review["batch_delta_sha256"],
            }
        )
        completion_path.write_bytes(local._canonical_bytes(completion))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("completion forge opened network"),
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "completion 1|review_pending",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_review_pending_receipt_coordinated_resign_is_rederived(self) -> None:
        def pending_third(content_id: int, *, db_path: Path):
            return self._fake_review_pending_evaluation(
                content_id,
                db_path=db_path,
                pending_content_ids=frozenset({3}),
            )

        with self._pipeline_patches(), patch.object(
            evaluation,
            "evaluate_content",
            side_effect=pending_third,
        ), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ):
            batches.run_batches(**self._batch_arguments())

        receipt_path = self.run_root / "items/000003.receipt.json"
        receipt = json.loads(receipt_path.read_text())
        original_evaluation_id = receipt["result"]["validated"]["evaluation_id"]
        original_queue_id = receipt["result"]["validated"]["queue_id"]
        receipt["result"]["validated"]["media_files"] += 1
        self.assertEqual(
            receipt["result"]["validated"]["evaluation_id"],
            original_evaluation_id,
        )
        self.assertEqual(
            receipt["result"]["validated"]["queue_id"],
            original_queue_id,
        )
        receipt_path.write_bytes(local._canonical_bytes(receipt))
        receipt_sha = local._sha256_file(receipt_path)

        progress_path = self.run_root / "progress/000003.progress.json"
        progress = json.loads(progress_path.read_text())
        progress["item_receipt_sha256"] = receipt_sha
        progress_path.write_bytes(local._canonical_bytes(progress))

        batch_path = self.run_root / "batches/000001.receipt.json"
        batch = json.loads(batch_path.read_text())
        batch["item_receipts"][-1][1] = receipt_sha
        batch["item_receipts_sha256"] = batches._json_sha(
            batch["item_receipts"]
        )
        batch["audit"]["batch_delta"][-1]["receipt_sha256"] = receipt_sha
        batch["audit"]["batch_delta_sha256"] = batches._json_sha(
            batch["audit"]["batch_delta"]
        )
        batch["audit"]["logical_head_sha256"] = batches._json_sha(
            {
                "previous_logical_head_sha256": batch["audit"][
                    "previous_logical_head_sha256"
                ],
                "batch_index": 1,
                "batch_delta_sha256": batch["audit"]["batch_delta_sha256"],
            }
        )
        batch_path.write_bytes(local._canonical_bytes(batch))

        completion_path = self.run_root / "completions/000001.completion.json"
        completion = json.loads(completion_path.read_text())
        completion["progress_head_sha256"] = local._sha256_file(progress_path)
        completion["audit"] = batch["audit"]
        review = completion["review_pending"]
        review["batch_delta"][0]["review_binding_sha256"] = batches._json_sha(
            receipt["result"]["validated"]
        )
        review["batch_delta_sha256"] = batches._json_sha(
            review["batch_delta"]
        )
        review["head_sha256"] = batches._json_sha(
            {
                "previous_head_sha256": review["previous_head_sha256"],
                "completion_sequence": 1,
                "batch_delta_sha256": review["batch_delta_sha256"],
            }
        )
        completion_path.write_bytes(local._canonical_bytes(completion))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), patch.object(
            evaluation,
            "_load_release_runtime",
            side_effect=self._fake_review_pending_runtime,
        ), patch.object(
            local.urllib.request,
            "build_opener",
            side_effect=AssertionError("receipt resign opened network"),
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "validated投影漂移|强终态重验",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_three_item_apply_succeeds_and_second_apply_is_physically_idempotent(
        self,
    ) -> None:
        provider_before = self.fixture._protected_digests(self.source_db)
        history_before: list[tuple[int, str]]
        with closing(local._immutable_connection(self.source_db)) as connection:
            history_before = [
                (int(row["id"]), str(row["source_group"]))
                for row in connection.execute(
                    "SELECT id,source_group FROM content_items ORDER BY id"
                )
            ]

        with self._pipeline_patches():
            first = batches.run_batches(**self._batch_arguments())

        self.assertEqual(first["status"], "eligible_complete")
        self.assertEqual(first["processed_this_invocation"], 3)
        self.assertEqual(first["eligible"], {
            "total": 3,
            "attempted": 3,
            "succeeded": 3,
            "review_pending": 0,
            "runtime_deferred": 0,
            "insufficient_evidence": 0,
        })
        self.assertEqual(first["provider_calls"], 0)
        self.assertFalse(first["full_history_complete"])
        self.assertFalse(first["publication_allowed"])
        self.assertEqual(
            first["current_database"]["sha256"],
            local._sha256_file(self.db),
        )
        self.assertEqual(
            first["current_database"]["byte_size"], self.db.stat().st_size
        )
        self.assertEqual(first["resume_guard"]["completed_count"], 3)
        self.assertGreater(first["resume_guard"]["output_hashed_files"], 0)
        self.assertGreater(first["resume_guard"]["output_hashed_bytes"], 0)
        self.assertEqual(self.calls, {"media": 3, "evaluation": 3, "fingerprint": 3})
        first_tree = self.fixture._tree_state(self.analysis_root)
        first_db_sha = local._sha256_file(self.db)
        first_completion = self.run_root / "completions" / "000001.completion.json"
        first_completion_sha = local._sha256_file(first_completion)

        with self._pipeline_patches():
            second = batches.run_batches(**self._batch_arguments())

        self.assertTrue(second["idempotent"])
        self.assertEqual(second["processed_this_invocation"], 0)
        self.assertEqual(second["resume_guard"], first["resume_guard"])
        self.assertEqual(self.calls, {"media": 3, "evaluation": 3, "fingerprint": 3})
        self.assertEqual(local._sha256_file(self.db), first_db_sha)
        self.assertEqual(
            local._sha256_file(first_completion),
            first_completion_sha,
        )
        self.assertEqual(self.fixture._tree_state(self.analysis_root), first_tree)
        self.assertEqual(self.fixture._protected_digests(self.db), provider_before)
        with closing(local._immutable_connection(self.db)) as connection:
            history_after = [
                (int(row["id"]), str(row["source_group"]))
                for row in connection.execute(
                    "SELECT id,source_group FROM content_items ORDER BY id"
                )
            ]
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM evaluation_versions "
                        "WHERE content_id IN (1,2,3) AND invalidated_at IS NULL"
                    ).fetchone()[0]
                ),
                3,
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM duplicate_fingerprints "
                        "WHERE content_id IN (1,2,3)"
                    ).fetchone()[0]
                ),
                3,
            )
        self.assertEqual(history_after, history_before)

    def test_real_sigkill_after_item_two_network_opening_is_manual_block(
        self,
    ) -> None:
        class KillOnSecond:
            def open(inner_self, request, **_kwargs):
                url = str(request.full_url)
                if "canary-2" in url:
                    os.kill(os.getpid(), signal.SIGKILL)
                return canary_tests._Response(url, b"video" * 1000)

        wait_status = self._fork_apply(
            patch.object(
                local.urllib.request,
                "build_opener",
                return_value=KillOnSecond(),
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "batches/000001.intent.json").is_file())
        self.assertTrue((self.run_root / "items/000001.receipt.json").is_file())
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())

        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "manual_required|durable processing attempt",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())

    def test_failed_terminal_network_event_before_db_is_manual_block(self) -> None:
        def fail_open(*_args, **_kwargs):
            raise local.urllib.error.URLError("injected terminal failure")

        def fail_then_kill(_content_id: int, **kwargs):
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            try:
                kwargs["urlopen_fn"](request, timeout=90)
            except local.urllib.error.URLError:
                os.kill(os.getpid(), signal.SIGKILL)
            raise AssertionError("SIGKILL did not terminate child")

        wait_status = self._fork_apply(
            patch.object(
                local.urllib.request,
                "build_opener",
                return_value=SimpleNamespace(open=fail_open),
            ),
            patch.object(
                media, "process_content_media", side_effect=fail_then_kill
            ),
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)

        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "manual_required|durable processing attempt",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse((self.run_root / "items/000001.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000002.intent.json").exists())

    def test_succeeded_terminal_network_event_before_db_is_manual_block(
        self,
    ) -> None:
        def succeed_then_kill(_content_id: int, **kwargs):
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with kwargs["urlopen_fn"](request, timeout=90) as response:
                response.read()
            os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                media, "process_content_media", side_effect=succeed_then_kill
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)

        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "manual_required|durable processing attempt",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse((self.run_root / "items/000001.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000002.intent.json").exists())

    def test_real_sigkill_after_item_two_db_commit_recovers_receipt_without_rerun(
        self,
    ) -> None:
        def kill_after_commit(content_id: int) -> None:
            if content_id == 2:
                os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                batches,
                "_after_item_database_commit",
                side_effect=kill_after_commit,
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())

        with self._pipeline_patches():
            result = batches.run_batches(**self._batch_arguments())

        second = json.loads(
            (self.run_root / "items/000002.receipt.json").read_text()
        )
        ledger = json.loads(
            (self.run_root / "network/000002.network.json").read_text()
        )
        self.assertEqual(result["status"], "eligible_complete")
        self.assertEqual(second["status"], "succeeded")
        self.assertTrue(second["recovered_after_commit"])
        self.assertEqual(len(ledger["events"]), 1)
        self.assertEqual(ledger["events"][0]["outcome"], "succeeded")
        self.assertEqual(self.calls, {"media": 1, "evaluation": 1, "fingerprint": 1})

    def test_real_sigkill_with_pending_batch_resumes_remaining_ordinals(self) -> None:
        original = batches._run_item

        def kill_before_second(*args, **kwargs):
            if int(kwargs["ordinal"]) == 2:
                os.kill(os.getpid(), signal.SIGKILL)
            return original(*args, **kwargs)

        wait_status = self._fork_apply(
            patch.object(batches, "_run_item", side_effect=kill_before_second)
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertTrue((self.run_root / "batches/000001.intent.json").is_file())
        self.assertTrue((self.run_root / "items/000001.receipt.json").is_file())
        self.assertFalse((self.run_root / "items/000002.intent.json").exists())

        with self._pipeline_patches():
            result = batches.run_batches(**self._batch_arguments())

        self.assertEqual(result["status"], "eligible_complete")
        self.assertEqual(result["processed_this_invocation"], 2)
        self.assertEqual(self.calls, {"media": 2, "evaluation": 2, "fingerprint": 2})

    def test_second_batch_sigkill_validates_previous_completion_and_resumes(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self.fixture._add_source_content(5)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            image_batch_size=25,
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        original = batches._run_item

        def kill_before_fifth(*args, **kwargs):
            if int(kwargs["ordinal"]) == 5:
                os.kill(os.getpid(), signal.SIGKILL)
            return original(*args, **kwargs)

        wait_status = self._fork_apply(
            patch.object(batches, "_run_item", side_effect=kill_before_fifth),
            through_batch=2,
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "batches/000002.intent.json").is_file())
        self.assertTrue((self.run_root / "items/000004.receipt.json").is_file())
        self.assertFalse((self.run_root / "items/000005.intent.json").exists())

        with self._pipeline_patches():
            result = batches.run_batches(
                **self._batch_arguments(through_batch=2)
            )

        self.assertEqual(result["status"], "eligible_complete")
        self.assertEqual(result["processed_this_invocation"], 1)
        self.assertEqual(self.calls, {"media": 4, "evaluation": 4, "fingerprint": 4})

    def test_batch_receipt_to_completion_sigkill_window_is_recovered(self) -> None:
        def kill_before_completion(*_args, **_kwargs):
            os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                batches, "_write_completion", side_effect=kill_before_completion
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "batches/000001.receipt.json").is_file())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )

        with self._pipeline_patches():
            result = batches.run_batches(**self._batch_arguments())

        self.assertTrue(result["idempotent"])
        self.assertEqual(result["processed_this_invocation"], 0)
        self.assertTrue(
            (self.run_root / "completions/000001.completion.json").is_file()
        )
        self.assertEqual(self.calls, {"media": 0, "evaluation": 0, "fingerprint": 0})

    def test_multi_batch_provisional_receipts_recover_after_end_scan_sigkill(
        self,
    ) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        original = batches._assert_global_invariants
        gate_calls = 0

        def kill_before_end_scan(*args, **kwargs):
            nonlocal gate_calls
            gate_calls += 1
            if gate_calls == 1:
                os.kill(os.getpid(), signal.SIGKILL)
            return original(*args, **kwargs)

        wait_status = self._fork_apply(
            patch.object(
                batches,
                "_assert_global_invariants",
                side_effect=kill_before_end_scan,
            ),
            through_batch=3,
            max_new_batches=2,
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "batches/000002.receipt.json").is_file())
        self.assertTrue((self.run_root / "batches/000003.receipt.json").is_file())
        self.assertFalse(
            (self.run_root / "completions/000002.completion.json").exists()
        )
        before_calls = dict(self.calls)

        with self._pipeline_patches():
            result = batches.run_batches(
                **self._batch_arguments(
                    through_batch=3,
                    max_new_batches=2,
                )
            )

        self.assertTrue(result["idempotent"])
        self.assertEqual(result["processed_this_invocation"], 0)
        self.assertEqual(result["status"], "eligible_complete")
        self.assertTrue(
            (self.run_root / "completions/000002.completion.json").is_file()
        )
        self.assertTrue(
            (self.run_root / "completions/000003.completion.json").is_file()
        )
        self.assertEqual(self.calls, before_calls)

    def test_completion_suffix_mid_write_sigkill_recovers_remaining_head(
        self,
    ) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        original = batches._write_completion

        def kill_before_third_completion(*args, **kwargs):
            if len(kwargs["completions"]) == 2:
                os.kill(os.getpid(), signal.SIGKILL)
            return original(*args, **kwargs)

        wait_status = self._fork_apply(
            patch.object(
                batches,
                "_write_completion",
                side_effect=kill_before_third_completion,
            ),
            through_batch=3,
            max_new_batches=2,
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        second_completion = self.run_root / "completions/000002.completion.json"
        third_completion = self.run_root / "completions/000003.completion.json"
        self.assertTrue(second_completion.is_file())
        self.assertFalse(third_completion.exists())
        before_calls = dict(self.calls)

        with self._pipeline_patches():
            result = batches.run_batches(
                **self._batch_arguments(
                    through_batch=3,
                    max_new_batches=2,
                )
            )

        third = json.loads(third_completion.read_text())
        self.assertTrue(result["idempotent"])
        self.assertEqual(result["processed_this_invocation"], 0)
        self.assertEqual(result["status"], "eligible_complete")
        self.assertEqual(
            third["previous_completion_sha256"],
            local._sha256_file(second_completion),
        )
        self.assertEqual(self.calls, before_calls)

    def test_controlled_download_failure_defers_second_and_continues_third(self) -> None:
        failure_message = "media download failed: injected timeout"

        def fail_open(request, **_kwargs):
            url = str(request.full_url)
            if url in set(self.source_fixtures[2]["urls"]):
                raise urllib.error.URLError("injected timeout")
            return canary_tests._Response(url, b"video" * 1000)

        def media_effect(content_id: int, **kwargs):
            if content_id == 2:
                request = local.urllib.request.Request(
                    kwargs["download_urls"][0]
                )
                with self.assertRaises(urllib.error.URLError):
                    kwargs["urlopen_fn"](request, timeout=90)
                connection = sqlite3.connect(self.db)
                try:
                    self._insert_retryable_download_slot(
                        connection,
                        content_id=content_id,
                        error_message=f"MediaProcessingError: {failure_message}",
                    )
                    connection.commit()
                finally:
                    connection.close()
                raise media.MediaProcessingError(failure_message)
            return self.fixture._fake_media(content_id, **kwargs)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=fail_open),
        ):
            result = batches.run_batches(**self._batch_arguments())

        receipts = [
            json.loads((self.run_root / f"items/{index:06d}.receipt.json").read_text())
            for index in range(1, 4)
        ]
        self.assertEqual([row["status"] for row in receipts], [
            "succeeded",
            "deferred",
            "succeeded",
        ])
        self.assertEqual(result["status"], "partial")
        self.assertFalse(result["full_history_complete"])
        self.assertFalse(result["publication_allowed"])

    def test_completion_deferred_history_uses_only_current_batch_delta(self) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        deferred_ids = {2, 4, 6}
        failure_message = "media download failed: bounded deferred delta"

        def fail_open(request, **_kwargs):
            url = str(request.full_url)
            if any(
                url in set(self.source_fixtures[content_id]["urls"])
                for content_id in deferred_ids
            ):
                raise urllib.error.URLError("bounded deferred delta")
            return canary_tests._Response(url, b"video" * 1000)

        def media_effect(content_id: int, **kwargs):
            if content_id not in deferred_ids:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with self.assertRaises(urllib.error.URLError):
                kwargs["urlopen_fn"](request, timeout=90)
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=f"MediaProcessingError: {failure_message}",
                )
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(media_side_effect=media_effect), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=fail_open),
        ):
            for through_batch in (1, 2, 3):
                result = batches.run_batches(
                    **self._batch_arguments(through_batch=through_batch)
                )

        completions = [
            json.loads(
                (
                    self.run_root
                    / f"completions/{sequence:06d}.completion.json"
                ).read_text()
            )
            for sequence in range(1, 4)
        ]
        evidence = [row["runtime_deferred"] for row in completions]
        self.assertEqual([row["count"] for row in evidence], [1, 2, 3])
        self.assertEqual(
            [
                [entry["content_id"] for entry in row["batch_delta"]]
                for row in evidence
            ],
            [[2], [4], [6]],
        )
        self.assertEqual(result["eligible"]["runtime_deferred"], 3)
        sizes = [
            len(local._canonical_bytes(row["runtime_deferred"]))
            for row in completions
        ]
        self.assertLess(max(sizes), min(sizes) * 2)

    def test_succeeded_network_event_cannot_forge_controlled_download_failure(
        self,
    ) -> None:
        failure_message = "media download failed: forged after success"

        def media_effect(content_id: int, **kwargs):
            if content_id != 2:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with kwargs["urlopen_fn"](request, timeout=90) as response:
                response.read()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(
            media_side_effect=media_effect
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "retryable_failed download slot"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())

    def test_succeeded_network_invalid_media_with_exact_slot_may_defer(self) -> None:
        failure_message = "media download failed: candidate was not playable"

        def media_effect(content_id: int, **kwargs):
            if content_id != 2:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with kwargs["urlopen_fn"](request, timeout=90) as response:
                response.read()
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=f"MediaProcessingError: {failure_message}",
                )
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(media_side_effect=media_effect):
            result = batches.run_batches(**self._batch_arguments())

        second = json.loads(
            (self.run_root / "items/000002.receipt.json").read_text()
        )
        ledger = json.loads(
            (self.run_root / "network/000002.network.json").read_text()
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(second["status"], "deferred")
        self.assertEqual(ledger["events"][-1]["outcome"], "succeeded")
        self.assertEqual(
            second["result"]["validated"]["failure_binding"][
                "terminal_event_outcome"
            ],
            "succeeded",
        )

    def test_real_image_unsupported_groups_bind_wal_slot_and_clean_empty_tree(
        self,
    ) -> None:
        image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal()
        )

        try:
            with self._pipeline_patches(
                media_side_effect=media_effect
            ), patch.object(
                local.urllib.request,
                "build_opener",
                return_value=SimpleNamespace(open=open_image),
            ):
                result = batches.run_batches(**self._batch_arguments())
        except batches.FullLocalAnalysisError as exc:
            self.assertRegex(str(exc), "retryable_failed download slot")
            image_root = self._image_download_root(2, image_urls[2])
            self.assertTrue(image_root.is_dir())
            self.assertEqual(list(image_root.iterdir()), [])
            self.assertFalse(
                (self.run_root / "items/000002.receipt.json").exists()
            )
            self.assertFalse(
                (self.run_root / "items/000003.intent.json").exists()
            )
            self.fail(f"WAL-visible retryable slot was missed: {exc}")

        receipts = [
            json.loads(
                (
                    self.run_root / f"items/{ordinal:06d}.receipt.json"
                ).read_text()
            )
            for ordinal in range(1, 4)
        ]
        ledger = json.loads(
            (self.run_root / "network/000002.network.json").read_text()
        )
        second_root = self.media_root / str(
            self.source_fixtures[2]["link_id"]
        )
        self.assertEqual(
            [receipt["status"] for receipt in receipts],
            ["deferred", "deferred", "deferred"],
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(
            receipts[1]["failure"]["message"],
            "image download incomplete: logical image group 3 exhausted | "
            "logical image group 7 exhausted | logical image group 8 exhausted",
        )
        self.assertEqual(
            receipts[1]["after"]["outputs"]["media"],
            {
                "files": 0,
                "rows_sha256": batches._json_sha([]),
                "rows": [],
            },
        )
        self.assertEqual(
            receipts[1]["after"]["outputs"]["fingerprints"]["files"], 0
        )
        self.assertEqual(len(ledger["events"]), 10)
        self.assertTrue(
            all(event["outcome"] == "succeeded" for event in ledger["events"])
        )
        self.assertEqual(
            [event["mime"] for event in ledger["events"]].count("image/vvic"),
            3,
        )
        self.assertFalse(second_root.exists())
        self.assertTrue(
            (self.run_root / "items/000003.receipt.json").is_file()
        )
        with closing(local._immutable_connection(self.db)) as connection:
            slots = connection.execute(
                "SELECT * FROM media_processing_slots "
                "WHERE content_id=2 AND processor_type='download'"
            ).fetchall()
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["status"], "retryable_failed")
        self.assertEqual(slots[0]["attempt_count"], 1)
        self.assertIsNone(slots[0]["output_artifact_id"])
        self.assertTrue(
            str(slots[0]["error_message"]).startswith(
                batches.DEFERRED_ATTEMPT_ANCHOR_PREFIX
            )
        )

        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ):
            repeated = batches.run_batches(**self._batch_arguments())
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_pre_anchor_sigkill_stays_manual_and_never_replays_network(
        self,
    ) -> None:
        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal()
        )

        def kill_before_anchor(content_id: int) -> None:
            if content_id == 2:
                os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                media, "process_content_media", side_effect=media_effect
            ),
            patch.object(
                local.urllib.request,
                "build_opener",
                return_value=SimpleNamespace(open=open_image),
            ),
            patch.object(
                batches,
                "_before_deferred_attempt_anchor_commit",
                side_effect=kill_before_anchor,
            ),
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        ledger_path = self.run_root / "network/000002.network.json"
        ledger_before = ledger_path.read_bytes()
        tree_before = self.fixture._tree_state(self.analysis_root)

        def forbidden_media(*_args, **_kwargs):
            self.fail("manual-required recovery replayed the media pipeline")

        def forbidden_open(*_args, **_kwargs):
            self.fail("manual-required recovery replayed a network request")

        with self._pipeline_patches(
            media_side_effect=forbidden_media
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=forbidden_open),
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "manual_required.*attempt anchor",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(ledger_path.read_bytes(), ledger_before)
        self.assertEqual(
            self.fixture._tree_state(self.analysis_root), tree_before
        )
        self.assertFalse(
            (self.run_root / "items/000002.receipt.json").exists()
        )
        self.assertFalse(
            (self.run_root / "items/000003.intent.json").exists()
        )
        self.assertEqual(
            self.calls, {"media": 0, "evaluation": 0, "fingerprint": 0}
        )

    def test_post_anchor_sigkill_recovers_without_network_replay(self) -> None:
        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal()
        )

        def kill_after_anchor(content_id: int) -> None:
            if content_id == 2:
                os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                media, "process_content_media", side_effect=media_effect
            ),
            patch.object(
                local.urllib.request,
                "build_opener",
                return_value=SimpleNamespace(open=open_image),
            ),
            patch.object(
                batches,
                "_after_deferred_attempt_anchor_commit",
                side_effect=kill_after_anchor,
            ),
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        ledger_path = self.run_root / "network/000002.network.json"
        ledger_before = ledger_path.read_bytes()

        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ):
            result = batches.run_batches(**self._batch_arguments())

        second = json.loads(
            (self.run_root / "items/000002.receipt.json").read_text()
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(second["status"], "deferred")
        self.assertTrue(second["recovered_after_commit"])
        self.assertEqual(ledger_path.read_bytes(), ledger_before)
        self.assertTrue(
            (self.run_root / "items/000003.receipt.json").is_file()
        )
        self.assertEqual(
            self.calls, {"media": 1, "evaluation": 0, "fingerprint": 0}
        )

        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ):
            repeated = batches.run_batches(**self._batch_arguments())
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_deferred_anchor_rejects_source_and_version_drift_before_write(
        self,
    ) -> None:
        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal()
        )

        def drift_before_anchor(content_id: int) -> None:
            if content_id != 2:
                return
            with closing(sqlite3.connect(self.db)) as connection:
                connection.execute(
                    "UPDATE media_processing_slots "
                    "SET source_sha256=?,processor_version=? "
                    "WHERE content_id=2 AND processor_type='download'",
                    ("0" * 64, "forged-download-version"),
                )
                connection.commit()

        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), patch.object(
            batches,
            "_before_deferred_attempt_anchor_commit",
            side_effect=drift_before_anchor,
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "未绑定current source download identity",
        ):
            batches.run_batches(**self._batch_arguments())

        ledger_path = self.run_root / "network/000002.network.json"
        ledger_before = ledger_path.read_bytes()
        with closing(sqlite3.connect(self.db)) as connection:
            row = connection.execute(
                "SELECT source_sha256,processor_version,error_message "
                "FROM media_processing_slots "
                "WHERE content_id=2 AND processor_type='download'"
            ).fetchone()
        self.assertEqual(row[0], "0" * 64)
        self.assertEqual(row[1], "forged-download-version")
        self.assertFalse(
            str(row[2]).startswith(batches.DEFERRED_ATTEMPT_ANCHOR_PREFIX)
        )
        self.assertFalse(
            (self.run_root / "items/000002.receipt.json").exists()
        )

        def forbidden_media(*_args, **_kwargs):
            self.fail("drifted deferred slot replayed media")

        def forbidden_open(*_args, **_kwargs):
            self.fail("drifted deferred slot replayed network")

        with self._pipeline_patches(
            media_side_effect=forbidden_media
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=forbidden_open),
        ), self.assertRaisesRegex(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError),
            "download identity|slot|manual_required",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(ledger_path.read_bytes(), ledger_before)

    def test_controlled_image_failure_never_deletes_nonempty_evidence(
        self,
    ) -> None:
        evidence_paths: list[Path] = []

        def leave_nonempty_evidence(image_root: Path) -> None:
            image_root.mkdir(parents=True, exist_ok=True)
            evidence = image_root / "unknown-evidence.bin"
            evidence.write_bytes(b"must-not-be-deleted")
            evidence_paths.append(evidence)

        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal(
                on_unsupported_failure=leave_nonempty_evidence
            )
        )
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), patch.object(
            batches, "_before_deferred_attempt_anchor_commit"
        ) as before_anchor, self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "包含文件/link/非目录证据",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(len(evidence_paths), 1)
        self.assertEqual(
            evidence_paths[0].read_bytes(), b"must-not-be-deleted"
        )
        self.assertNotIn(
            2,
            [entry.args[0] for entry in before_anchor.call_args_list],
        )
        with closing(sqlite3.connect(self.db)) as connection:
            row = connection.execute(
                "SELECT error_message FROM media_processing_slots "
                "WHERE content_id=2 AND processor_type='download'"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertFalse(
            str(row[0]).startswith(batches.DEFERRED_ATTEMPT_ANCHOR_PREFIX)
        )
        self.assertFalse(
            (self.run_root / "items/000002.receipt.json").exists()
        )
        self.assertFalse(
            (self.run_root / "items/000003.intent.json").exists()
        )

    def test_controlled_image_failure_rejects_unknown_empty_directory(
        self,
    ) -> None:
        unknown_paths: list[Path] = []

        def leave_unknown_directory(image_root: Path) -> None:
            unknown = image_root.parent / "unknown-empty"
            unknown.mkdir()
            unknown_paths.append(unknown)

        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal(
                on_unsupported_failure=leave_unknown_directory
            )
        )
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "包含未知目录"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(len(unknown_paths), 1)
        self.assertTrue(unknown_paths[0].is_dir())
        self.assertEqual(list(unknown_paths[0].iterdir()), [])
        self.assertFalse(
            (self.run_root / "items/000002.receipt.json").exists()
        )
        self.assertFalse(
            (self.run_root / "items/000003.intent.json").exists()
        )

    def test_controlled_image_failure_rejects_owned_symlink(self) -> None:
        link_paths: list[Path] = []

        def leave_symlink(image_root: Path) -> None:
            target = self.root / "outside-evidence"
            target.write_bytes(b"outside")
            link = image_root / "unexpected-link"
            link.symlink_to(target)
            link_paths.append(link)

        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal(
                on_unsupported_failure=leave_symlink
            )
        )
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "包含文件/link/非目录证据"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(len(link_paths), 1)
        self.assertTrue(link_paths[0].is_symlink())
        self.assertFalse(
            (self.run_root / "items/000002.receipt.json").exists()
        )
        self.assertFalse(
            (self.run_root / "items/000003.intent.json").exists()
        )

    def test_image_deferred_anchor_blocks_coordinated_terminal_resign(
        self,
    ) -> None:
        _image_urls, open_image, media_effect = (
            self._prepare_real_image_unsupported_wal(
                unsupported_content_id=3
            )
        )
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ):
            result = batches.run_batches(**self._batch_arguments())
        self.assertEqual(result["status"], "partial")
        ledger = json.loads(
            (self.run_root / "network/000003.network.json").read_text()
        )
        self.assertEqual(len(ledger["events"]), 10)
        self.assertTrue(
            all(event["outcome"] == "succeeded" for event in ledger["events"])
        )

        self._resign_last_deferred_terminal_as_failed()
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(
            media_side_effect=media_effect
        ), patch.object(
            local.urllib.request,
            "build_opener",
            return_value=SimpleNamespace(open=open_image),
        ), self.assertRaisesRegex(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError),
            "attempt anchor|network ledger|failure binding|历史item强终态",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_post_open_failed_response_with_exact_db_attempt_may_defer(self) -> None:
        failure_message = "media download failed: response failed after open"

        def media_effect(content_id: int, **kwargs):
            if content_id != 2:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with self.assertRaises(media.MediaProcessingError):
                with kwargs["urlopen_fn"](request, timeout=90) as response:
                    response.read()
                    raise media.MediaProcessingError(
                        "media response URL changed after body read"
                    )
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=f"MediaProcessingError: {failure_message}",
                )
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(media_side_effect=media_effect):
            first = batches.run_batches(**self._batch_arguments())
        receipt = json.loads(
            (self.run_root / "items/000002.receipt.json").read_text()
        )
        ledger = json.loads(
            (self.run_root / "network/000002.network.json").read_text()
        )
        terminal = ledger["events"][-1]
        self.assertEqual(first["status"], "partial")
        self.assertEqual(receipt["status"], "deferred")
        self.assertEqual(terminal["outcome"], "failed")
        self.assertEqual(terminal["status"], 200)
        self.assertGreater(terminal["bytes"], 0)
        self.assertRegex(terminal["response_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(
            receipt["result"]["validated"]["failure_binding"][
                "terminal_evidence_class"
            ],
            "response_failed_after_open",
        )

        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches():
            second = batches.run_batches(**self._batch_arguments())
        self.assertTrue(second["idempotent"])
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_live_deferred_terminal_outcome_cannot_be_coordinated_resigned(
        self,
    ) -> None:
        failure_message = "media download failed: terminal binding live"

        def media_effect(content_id: int, **kwargs):
            if content_id != 3:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with kwargs["urlopen_fn"](request, timeout=90) as response:
                response.read()
            connection = sqlite3.connect(self.db)
            try:
                self._insert_retryable_download_slot(
                    connection,
                    content_id=content_id,
                    error_message=f"MediaProcessingError: {failure_message}",
                )
                connection.commit()
            finally:
                connection.close()
            raise media.MediaProcessingError(failure_message)

        with self._pipeline_patches(media_side_effect=media_effect):
            result = batches.run_batches(**self._batch_arguments())
        self.assertEqual(result["status"], "partial")
        receipt = json.loads(
            (self.run_root / "items/000003.receipt.json").read_text()
        )
        self.assertFalse(receipt["recovered_after_commit"])

        self._resign_last_deferred_terminal_as_failed()
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError),
            "network ledger|terminal|failure binding|历史item强终态",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_terminal_without_db_attempt_cannot_be_resigned_into_deferred(
        self,
    ) -> None:
        def succeed_then_kill(content_id: int, **kwargs):
            if content_id != 3:
                return self.fixture._fake_media(content_id, **kwargs)
            request = local.urllib.request.Request(kwargs["download_urls"][0])
            with kwargs["urlopen_fn"](request, timeout=90) as response:
                response.read()
            os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(
                media, "process_content_media", side_effect=succeed_then_kill
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        ledger_path = self.run_root / "network/000003.network.json"
        ledger = json.loads(ledger_path.read_text())
        terminal = ledger["events"][-1]
        self.assertEqual(terminal["outcome"], "succeeded")
        terminal.update(
            {
                "outcome": "failed",
                "error": "URLError: coordinated terminal rewrite",
                "status": None,
                "mime": None,
                "declared_bytes": None,
                "bytes": 0,
                "charged_bytes": 0,
                "response_sha256": None,
            }
        )
        ledger["total_bytes"] = 0
        ledger["budget_consumed_bytes"] = 0
        ledger["overrun"] = False
        ledger_path.write_bytes(local._canonical_bytes(ledger))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "manual_required|durable processing attempt",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse((self.run_root / "items/000003.receipt.json").exists())

    def test_structural_media_collision_is_global_block_not_deferred(self) -> None:
        def media_effect(content_id: int, **kwargs):
            if content_id == 2:
                raise media.MediaProcessingError(
                    "media source manifest collision: download failed marker"
                )
            return self.fixture._fake_media(content_id, **kwargs)

        with self._pipeline_patches(
            media_side_effect=media_effect
        ), self.assertRaisesRegex(
            media.MediaProcessingError, "manifest collision"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertFalse((self.run_root / "items/000002.receipt.json").exists())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())

    def test_absolute_batch_two_requires_explicit_plus_one_and_same_stop_is_noop(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self.fixture._add_source_content(5)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            image_batch_size=25,
            video_batch_size=2,
        )
        with self._pipeline_patches():
            first = batches.run_batches(**self._batch_arguments())
            same = batches.run_batches(**self._batch_arguments())
        self.assertEqual(first["status"], "pilot_complete")
        self.assertTrue(same["idempotent"])
        self.assertEqual(same["processed_this_invocation"], 0)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "显式\\+1"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=3))
        with self._pipeline_patches():
            second = batches.run_batches(**self._batch_arguments(through_batch=2))
        self.assertEqual(second["status"], "eligible_complete")
        self.assertEqual(second["processed_this_invocation"], 2)
        self.assertEqual(
            json.loads(
                (self.run_root / "batches/000002.intent.json").read_text()
            )["content_ids"],
            [4, 5],
        )

    def test_record_top_level_fields_are_individually_tamper_evident(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        records = (
            self.run_root / "items/000001.receipt.json",
            self.run_root / "progress/000001.progress.json",
            self.run_root / "batches/000001.receipt.json",
            self.run_root / "completions/000001.completion.json",
        )
        for path in records:
            original = path.read_bytes()
            body = json.loads(original)
            for key in tuple(body):
                with self.subTest(record=path.name, field=key):
                    mutated = dict(body)
                    mutated[key] = self._mutated(mutated[key])
                    path.write_bytes(local._canonical_bytes(mutated))
                    with self._pipeline_patches(), self.assertRaises(
                        (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
                    ):
                        batches.run_batches(**self._batch_arguments())
                    path.write_bytes(original)

    def test_batch_checkpoint_blocks_provider_cross_target_sequence_and_unknowns(
        self,
    ) -> None:
        def inject_after_first(content_id: int) -> None:
            if content_id != 1:
                return
            connection = sqlite3.connect(self.db)
            try:
                connection.execute(
                    "UPDATE provider_usage SET amount=amount+1 WHERE id=1"
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches,
            "_after_item_database_commit",
            side_effect=inject_after_first,
        ), self.assertRaisesRegex(batches.FullLocalAnalysisError, "provider"):
            batches.run_batches(**self._batch_arguments())
        self.assertTrue((self.run_root / "batches/000001.receipt.json").is_file())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "provider"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )
        self.assertEqual(self.calls, before_calls)

    def test_wal_provider_drift_blocks_before_complete_receipt_temp_promotion(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        receipt = self.run_root / "items/000003.receipt.json"
        temporary = receipt.with_name(f".{receipt.name}.tmp")
        receipt.rename(temporary)
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA wal_autocheckpoint=0")
            connection.execute(
                "UPDATE provider_usage SET amount=amount+1 WHERE id=1"
            )
            connection.commit()
            self.assertTrue(Path(f"{self.db}-wal").is_file())
            self.assertTrue(Path(f"{self.db}-shm").is_file())
            before = self.fixture._tree_state(self.analysis_root)
            before_calls = dict(self.calls)

            with self._pipeline_patches(), self.assertRaisesRegex(
                batches.FullLocalAnalysisError, "provider"
            ):
                batches.run_batches(**self._batch_arguments())

            self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
            self.assertTrue(temporary.is_file())
            self.assertFalse(receipt.exists())
            self.assertEqual(self.calls, before_calls)
        finally:
            connection.close()

    def test_output_ownership_drift_blocks_before_receipt_temp_promotion(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        receipt = self.run_root / "items/000003.receipt.json"
        temporary = receipt.with_name(f".{receipt.name}.tmp")
        receipt.rename(temporary)
        unowned = self.media_root / "UNOWNED"
        unowned.mkdir()
        (unowned / "evidence.bin").write_bytes(b"unowned")
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "output top-level ownership"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertTrue(temporary.is_file())
        self.assertFalse(receipt.exists())
        self.assertEqual(self.calls, before_calls)

    def test_output_ownership_drift_blocks_before_progress_completion_catchup(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        progress = self.run_root / "progress/000003.progress.json"
        completion = self.run_root / "completions/000001.completion.json"
        progress.unlink()
        completion.unlink()
        unowned = self.media_root / "UNOWNED"
        unowned.mkdir()
        (unowned / "evidence.bin").write_bytes(b"unowned")
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "output top-level ownership"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertFalse(progress.exists())
        self.assertFalse(completion.exists())
        self.assertEqual(self.calls, before_calls)

    def test_cross_target_managed_row_injection_blocks_global_batch(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 1:
                return
            connection = sqlite3.connect(self.db)
            try:
                source = connection.execute(
                    """SELECT * FROM evidence_artifacts
                       WHERE content_id=2 AND artifact_type='media_source'"""
                ).fetchone()
                connection.execute(
                    """INSERT INTO evidence_artifacts(
                           content_id,artifact_type,local_path,status,byte_size,
                           sha256,captured_at,processor_version,metadata_json,created_at
                       ) VALUES (2,'media',?,'available',?,?,?,?,'{}',?)""",
                    (source[2], source[4], source[5], source[6], "injected", source[9]),
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaises(batches.FullLocalAnalysisError):
            batches.run_batches(**self._batch_arguments())

    def test_last_item_direction_pollution_of_first_receipt_blocks_batch(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 3:
                return
            connection = sqlite3.connect(self.db)
            try:
                connection.execute(
                    "UPDATE content_items SET evaluation_content_direction="
                    "'unknown' WHERE id=1"
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaisesRegex(batches.FullLocalAnalysisError, "receipt 1"):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "batches/000001.receipt.json").exists())

    def test_second_item_pollution_blocks_before_third_network(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 2:
                return
            connection = sqlite3.connect(self.db)
            try:
                connection.execute(
                    "UPDATE content_items SET evaluation_content_direction="
                    "'unknown' WHERE id=1"
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaisesRegex(batches.FullLocalAnalysisError, "receipt 1"):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "items/000003.intent.json").exists())
        self.assertEqual(self.calls, {"media": 2, "evaluation": 2, "fingerprint": 2})

    def test_partial_close_rejects_polluted_unstarted_suffix(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 1:
                return
            connection = sqlite3.connect(self.db)
            try:
                source = connection.execute(
                    "SELECT * FROM evidence_artifacts "
                    "WHERE content_id=2 AND artifact_type='media_source'"
                ).fetchone()
                connection.execute(
                    """INSERT INTO evidence_artifacts(
                           content_id,artifact_type,local_path,status,byte_size,
                           sha256,captured_at,processor_version,metadata_json,created_at
                       ) VALUES (2,'media',?,'available',?,?,?,?,'{}',?)""",
                    (source[2], source[4], source[5], source[6], "injected", source[9]),
                )
                connection.commit()
            finally:
                connection.close()

        with patch.object(
            batches, "BATCH_DOWNLOAD_CAP_BYTES", 5_000
        ), self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "unstarted suffix"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "items/000002.intent.json").exists())
        self.assertFalse((self.run_root / "batches/000001.receipt.json").exists())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )

    def test_pending_current_item_revalidates_future_suffix_before_network(
        self,
    ) -> None:
        original = batches._recover_item_receipt

        def poison_future_then_kill(*args, **kwargs):
            if int(kwargs["intent"]["content_id"]) == 1:
                connection = sqlite3.connect(self.db)
                try:
                    connection.execute(
                        "UPDATE content_items SET evaluation_content_direction="
                        "'unknown' WHERE id=2"
                    )
                    connection.commit()
                finally:
                    connection.close()
                os.kill(os.getpid(), signal.SIGKILL)
            return original(*args, **kwargs)

        wait_status = self._fork_apply(
            patch.object(
                batches,
                "_recover_item_receipt",
                side_effect=poison_future_then_kill,
            )
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertEqual(os.WTERMSIG(wait_status), signal.SIGKILL)
        self.assertTrue((self.run_root / "items/000001.intent.json").is_file())
        self.assertFalse((self.run_root / "items/000001.receipt.json").exists())

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "unstarted suffix"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(
            self.calls, {"media": 0, "evaluation": 0, "fingerprint": 0}
        )
        self.assertFalse((self.run_root / "items/000002.intent.json").exists())

    def test_last_item_extra_managed_row_for_first_receipt_blocks_batch(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 3:
                return
            connection = sqlite3.connect(self.db)
            try:
                source = connection.execute(
                    "SELECT * FROM evidence_artifacts "
                    "WHERE content_id=1 AND artifact_type='media'"
                ).fetchone()
                connection.execute(
                    """INSERT INTO evidence_artifacts(
                           content_id,artifact_type,local_path,status,byte_size,
                           sha256,captured_at,processor_version,metadata_json,created_at
                       ) VALUES (1,'injected_extra',?,'available',?,?,?,?,'{}',?)""",
                    (source[2], source[4], source[5], source[6], "injected", source[9]),
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaisesRegex(batches.FullLocalAnalysisError, "receipt 1"):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "batches/000001.receipt.json").exists())

    def test_deferred_fake_slot_is_global_block(self) -> None:
        def mutate(connection: sqlite3.Connection) -> None:
            connection.execute(
                """INSERT INTO media_processing_slots(
                       content_id,source_sha256,processor_type,processor_version,
                       status,output_artifact_id,attempt_count,error_message,
                       created_at,updated_at
                   ) VALUES (2,?,'download','forged-version','retryable_failed',
                             NULL,1,'forged',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                ("0" * 64,),
            )

        self._assert_deferred_mutation_blocks(mutate)

    def test_deferred_fake_artifact_is_global_block(self) -> None:
        def mutate(connection: sqlite3.Connection) -> None:
            source = connection.execute(
                "SELECT * FROM evidence_artifacts "
                "WHERE content_id=2 AND artifact_type='media_source'"
            ).fetchone()
            connection.execute(
                """INSERT INTO evidence_artifacts(
                       content_id,artifact_type,local_path,status,byte_size,
                       sha256,captured_at,processor_version,metadata_json,created_at
                   ) VALUES (2,'media',?,'available',?,?,?,?,'{}',?)""",
                (source[2], source[4], source[5], source[6], "forged", source[9]),
            )

        self._assert_deferred_mutation_blocks(mutate)

    def test_deferred_baseline_media_source_rewrite_is_global_block(self) -> None:
        def mutate(connection: sqlite3.Connection) -> None:
            connection.execute(
                "UPDATE evidence_artifacts SET processor_version='rewritten' "
                "WHERE content_id=2 AND artifact_type='media_source'"
            )

        self._assert_deferred_mutation_blocks(mutate)

    def test_static_deferred_target_pollution_is_globally_protected(self) -> None:
        self.fixture._add_source_content(4)
        self._make_source_non_https(4)
        self.profile = batches.HistoryProfile(
            universe_count=4,
            eligible_count=3,
            static_deferred_count=1,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
        )

        def inject(content_id: int) -> None:
            if content_id != 1:
                return
            connection = sqlite3.connect(self.db)
            try:
                source = connection.execute(
                    "SELECT * FROM evidence_artifacts "
                    "WHERE content_id=4 AND artifact_type='media_source'"
                ).fetchone()
                connection.execute(
                    """INSERT INTO evidence_artifacts(
                           content_id,artifact_type,local_path,status,byte_size,
                           sha256,captured_at,processor_version,metadata_json,created_at
                       ) VALUES (4,'media',?,'available',?,?,?,?,'{}',?)""",
                    (source[2], source[4], source[5], source[6], "injected", source[9]),
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaisesRegex(batches.FullLocalAnalysisError, "protected"):
            batches.run_batches(**self._batch_arguments())

        self.assertFalse((self.run_root / "batches/000001.receipt.json").exists())

    def test_static_deferred_existing_row_update_leaves_only_provisional_batch(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self._make_source_non_https(4)
        self.profile = batches.HistoryProfile(
            universe_count=4,
            eligible_count=3,
            static_deferred_count=1,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
        )

        def inject(content_id: int) -> None:
            if content_id != 1:
                return
            connection = sqlite3.connect(self.db)
            try:
                connection.execute(
                    "UPDATE evidence_artifacts SET processor_version='rewritten' "
                    "WHERE content_id=4 AND artifact_type='media_source'"
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaisesRegex(batches.FullLocalAnalysisError, "protected"):
            batches.run_batches(**self._batch_arguments())

        # The internal batch closure is provisional until the invocation-wide
        # protected scan passes; no externally accepted completion may exist.
        self.assertTrue((self.run_root / "batches/000001.receipt.json").is_file())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )
        before_calls = dict(self.calls)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "protected"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse(
            (self.run_root / "completions/000001.completion.json").exists()
        )
        self.assertEqual(self.calls, before_calls)

    def test_extra_same_target_managed_row_is_not_signable(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 1:
                return
            target = self.media_root / "C4N4RY" / "extra.bin"
            target.write_bytes(b"extra")
            connection = sqlite3.connect(self.db)
            try:
                connection.execute(
                    """INSERT INTO evidence_artifacts(
                           content_id,artifact_type,local_path,status,byte_size,
                           sha256,captured_at,processor_version,metadata_json,created_at
                       ) VALUES (1,'media',?,'available',5,?,?,?,'{}',?)""",
                    (
                        str(target),
                        local._sha256_file(target),
                        "2026-01-01T00:00:00Z",
                        "injected",
                        "2026-01-01T00:00:00Z",
                    ),
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())

    def test_protected_sqlite_sequence_injection_blocks(self) -> None:
        def inject(content_id: int) -> None:
            if content_id != 1:
                return
            connection = sqlite3.connect(self.db)
            try:
                connection.execute(
                    "UPDATE sqlite_sequence SET seq=seq+1 WHERE name='provider_usage'"
                )
                connection.commit()
            finally:
                connection.close()

        with self._pipeline_patches(), patch.object(
            batches, "_after_item_database_commit", side_effect=inject
        ), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())

    def test_copy_complete_contract_gap_and_known_atomic_temp_recover(self) -> None:
        def kill_before_contract(*_args, **_kwargs):
            os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(batches, "_build_contract", side_effect=kill_before_contract)
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        self.assertTrue(self.db.is_file())
        self.assertTrue((self.run_root / "copy-receipt.json").is_file())
        self.assertFalse((self.run_root / "run-contract.json").exists())
        with self._pipeline_patches():
            result = batches.run_batches(**self._batch_arguments())
        self.assertEqual(result["status"], "eligible_complete")

        receipt = self.run_root / "items/000001.receipt.json"
        temporary = receipt.with_name(f".{receipt.name}.tmp")
        receipt.replace(temporary)
        with self._pipeline_patches():
            repeated = batches.run_batches(**self._batch_arguments())
        self.assertTrue(repeated["idempotent"])
        self.assertTrue(receipt.is_file())
        self.assertFalse(temporary.exists())

    def test_existing_plan_unknown_run_file_is_zero_write_block(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        unknown = self.run_root / "unknown-preaction.json"
        unknown.write_text("{}\n", encoding="utf-8")
        before = self.fixture._tree_state(self.analysis_root)

        with patch.object(local, "_local_tools", return_value=self.tools), self.assertRaises(
            batches.FullLocalAnalysisError
        ):
            batches.plan_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)

    def test_byte_identical_output_root_replacement_blocks_before_pipeline(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        moved = self.analysis_root / "original-media-root"
        self.media_root.replace(moved)
        shutil.copytree(moved, self.media_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "inode"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.calls, before_calls)

    def test_contract_code_self_selection_is_rejected_before_first_batch(
        self,
    ) -> None:
        def kill_before_batch(*_args, **_kwargs):
            os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(batches, "_run_batch", side_effect=kill_before_batch)
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        contract_path = self.run_root / "run-contract.json"
        contract = json.loads(contract_path.read_text())
        contract["code"] = {}
        contract_path.write_text(
            json.dumps(contract, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "代码完整集合"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse((self.run_root / "batches/000001.intent.json").exists())
        self.assertEqual(
            self.calls, {"media": 0, "evaluation": 0, "fingerprint": 0}
        )

    def test_v2_contract_is_zero_write_blocked_before_first_batch(self) -> None:
        def kill_before_batch(*_args, **_kwargs):
            os.kill(os.getpid(), signal.SIGKILL)

        wait_status = self._fork_apply(
            patch.object(batches, "_run_batch", side_effect=kill_before_batch)
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        contract_path = self.run_root / "run-contract.json"
        contract = json.loads(contract_path.read_text())
        contract["schema_version"] = "full-local-analysis-batches-v2"
        contract_path.write_bytes(local._canonical_bytes(contract))
        before = self.fixture._tree_state(self.analysis_root)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "contract模式/版本"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertFalse((self.run_root / "batches/000001.intent.json").exists())

    def test_v2_item_cannot_mix_into_v3_history(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        receipt_path = self.run_root / "items/000002.receipt.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["schema_version"] = "full-local-analysis-batches-v2"
        receipt_path.write_bytes(local._canonical_bytes(receipt))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "batch audit重派生|intent/receipt语义链|item receipt",
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_contract_source_summaries_reject_coordinated_row_resigns(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        paths = batches._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        original = json.loads(paths.contract.read_text())

        def validate(contract) -> None:
            with patch.object(
                local, "_local_tools", return_value=self.tools
            ):
                batches._validate_contract(
                    paths,
                    contract,
                    expected_source_db_sha256=self.source_db_sha,
                    expected_source_completion_sha256=(
                        self.source_completion_sha
                    ),
                    profile=self.profile,
                )

        validate(original)

        def resign(contract) -> None:
            summaries = contract["source_summaries"]
            contract["source_summaries_sha256"] = batches._json_sha(
                [
                    [int(key), summaries[key]]
                    for key in sorted(summaries, key=int)
                ]
            )

        mutations = (
            (
                "summary_missing",
                lambda summaries: summaries.pop("1"),
            ),
            (
                "row_not_mapping",
                lambda summaries: summaries.__setitem__("1", True),
            ),
            (
                "row_extra_key",
                lambda summaries: summaries["1"].__setitem__("extra", 1),
            ),
            (
                "row_missing_key",
                lambda summaries: summaries["1"].pop(
                    "download_urls_sha256"
                ),
            ),
            (
                "content_id_mismatch",
                lambda summaries: summaries["1"].__setitem__(
                    "content_id", 2
                ),
            ),
            (
                "content_id_bool",
                lambda summaries: summaries["1"].__setitem__(
                    "content_id", True
                ),
            ),
            (
                "source_sha_bool",
                lambda summaries: summaries["1"].__setitem__(
                    "source_sha256", True
                ),
            ),
            (
                "raw_sha_float",
                lambda summaries: summaries["1"].__setitem__(
                    "raw_response_body_sha256", 1.0
                ),
            ),
            (
                "download_sha_uppercase",
                lambda summaries: summaries["1"].__setitem__(
                    "download_urls_sha256", "A" * 64
                ),
            ),
            (
                "media_kind_bool",
                lambda summaries: summaries["1"].__setitem__(
                    "media_kind", True
                ),
            ),
            (
                "media_kind_unknown",
                lambda summaries: summaries["1"].__setitem__(
                    "media_kind", "audio"
                ),
            ),
            (
                "image_groups_none",
                lambda summaries: summaries["1"].__setitem__(
                    "media_kind", "image"
                ),
            ),
            (
                "image_groups_bool",
                lambda summaries: (
                    summaries["1"].__setitem__("media_kind", "image"),
                    summaries["1"].__setitem__(
                        "image_groups_sha256", True
                    ),
                ),
            ),
            (
                "image_groups_float",
                lambda summaries: (
                    summaries["1"].__setitem__("media_kind", "image"),
                    summaries["1"].__setitem__(
                        "image_groups_sha256", 1.0
                    ),
                ),
            ),
            (
                "image_groups_uppercase",
                lambda summaries: (
                    summaries["1"].__setitem__("media_kind", "image"),
                    summaries["1"].__setitem__(
                        "image_groups_sha256", "A" * 64
                    ),
                ),
            ),
            (
                "video_groups_nonnull",
                lambda summaries: summaries["1"].__setitem__(
                    "image_groups_sha256", "0" * 64
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                contract = json.loads(json.dumps(original))
                mutate(contract["source_summaries"])
                resign(contract)
                with self.assertRaisesRegex(
                    batches.FullLocalAnalysisError,
                    "source summaries.*\u5408\u540c",
                ):
                    validate(contract)

    def test_true_truncated_item_receipt_is_rebuilt_without_replaying_item(
        self,
    ) -> None:
        original = local._write_atomic
        target = self.run_root / "items/000002.receipt.json"

        def truncate_then_kill(path, body, *, immutable):
            if path == target:
                temporary = path.with_name(f".{path.name}.tmp")
                temporary.write_bytes(body[: max(1, len(body) // 2)])
                with temporary.open("rb") as handle:
                    os.fsync(handle.fileno())
                os.kill(os.getpid(), signal.SIGKILL)
            return original(path, body, immutable=immutable)

        wait_status = self._fork_apply(
            patch.object(local, "_write_atomic", side_effect=truncate_then_kill)
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        temporary = target.with_name(f".{target.name}.tmp")
        self.assertTrue(temporary.is_file())
        self.assertFalse(target.exists())

        with self._pipeline_patches():
            result = batches.run_batches(**self._batch_arguments())
        self.assertEqual(result["status"], "eligible_complete")
        self.assertTrue(target.is_file())
        self.assertFalse(temporary.exists())
        self.assertEqual(
            self.calls, {"media": 1, "evaluation": 1, "fingerprint": 1}
        )

    def test_true_truncated_empty_network_ledger_restarts_before_request(
        self,
    ) -> None:
        original = local._write_atomic
        target = self.run_root / "network/000002.network.json"

        def truncate_then_kill(path, body, *, immutable):
            if path == target and not path.exists():
                temporary = path.with_name(f".{path.name}.tmp")
                temporary.write_bytes(body[: max(1, len(body) // 2)])
                with temporary.open("rb") as handle:
                    os.fsync(handle.fileno())
                os.kill(os.getpid(), signal.SIGKILL)
            return original(path, body, immutable=immutable)

        wait_status = self._fork_apply(
            patch.object(local, "_write_atomic", side_effect=truncate_then_kill)
        )
        self.assertTrue(os.WIFSIGNALED(wait_status))
        temporary = target.with_name(f".{target.name}.tmp")
        self.assertTrue(temporary.is_file())
        self.assertFalse(target.exists())

        with self._pipeline_patches():
            result = batches.run_batches(**self._batch_arguments())
        self.assertEqual(result["status"], "eligible_complete")
        self.assertTrue(target.is_file())
        self.assertFalse(temporary.exists())
        self.assertEqual(
            self.calls, {"media": 2, "evaluation": 2, "fingerprint": 2}
        )

    def test_completed_receipt_missing_network_ledger_is_not_recreated(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        ledger = self.run_root / "network/000002.network.json"
        ledger.unlink()
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "缺少network ledger"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertFalse(ledger.exists())
        self.assertEqual(self.calls, before_calls)

    def test_invalid_unowned_receipt_temp_is_preserved_and_not_promoted(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        temporary = self.run_root / "items/.000004.receipt.json.tmp"
        temporary.write_bytes(b'{"schema_version":')
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "状态机前驱"
        ):
            batches.run_batches(**self._batch_arguments())
        self.assertTrue(temporary.is_file())
        self.assertFalse(
            (self.run_root / "items/000004.receipt.json").exists()
        )
        self.assertEqual(self.calls, before_calls)

    def test_future_batch_temp_is_preserved_and_blocks_before_next_batch(self) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        temporary = self.run_root / "batches/.000005.intent.json.tmp"
        body = b'{"schema_version":'
        temporary.write_bytes(body)
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "atomic temp|状态机"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=2))

        self.assertEqual(temporary.read_bytes(), body)
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse(
            (self.run_root / "batches/000002.intent.json").exists()
        )

    def test_future_network_temp_is_preserved_and_blocks_as_not_unique_next(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        temporary = self.run_root / "network/.000005.network.json.tmp"
        body = b'{"schema_version":'
        temporary.write_bytes(body)
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "network.*唯一next|状态机"
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(temporary.read_bytes(), body)
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse(
            (self.run_root / "network/000005.network.json").exists()
        )

    def test_exact_numeric_types_and_coordinated_bool_alias_are_blocked(
        self,
    ) -> None:
        before = self.fixture._tree_state(self.analysis_root)
        invalid_profiles = (
            batches.HistoryProfile(
                universe_count=3,
                eligible_count=3,
                static_deferred_count=0,
                missing_universe_count=0,
                first_batch_ids=(1, 2, 3),
                image_batch_size=True,
            ),
            batches.HistoryProfile(
                universe_count=3,
                eligible_count=3,
                static_deferred_count=0,
                missing_universe_count=0,
                first_batch_ids=(1, 2, True),
            ),
        )
        for overrides in (
            {"workers": True},
            {"through_batch": True},
            {"max_new_batches": True},
            *({"profile": profile} for profile in invalid_profiles),
        ):
            with self.subTest(overrides=overrides), self.assertRaises(
                batches.FullLocalAnalysisError
            ):
                batches.plan_batches(**self._batch_arguments(**overrides))
            self.assertEqual(self.fixture._tree_state(self.analysis_root), before)

        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        completion_path = self.run_root / "completions/000001.completion.json"
        original_completion = completion_path.read_bytes()
        completion = json.loads(original_completion)
        completion["provider_calls"] = False
        completion_path.write_bytes(local._canonical_bytes(completion))
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "completion"
        ):
            batches.run_batches(**self._batch_arguments())
        completion_path.write_bytes(original_completion)

        completion = json.loads(original_completion)
        completion["eligible"] = []
        completion_path.write_bytes(local._canonical_bytes(completion))
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "completion"
        ):
            batches.run_batches(**self._batch_arguments())
        completion_path.write_bytes(original_completion)

        batch_path = self.run_root / "batches/000001.receipt.json"
        original_batch = batch_path.read_bytes()
        for alias in (True, 1.0):
            with self.subTest(audit_ordinal_alias=alias):
                batch = json.loads(original_batch)
                batch["audit"]["batch_delta"][0]["ordinal"] = alias
                batch_path.write_bytes(local._canonical_bytes(batch))
                completion = json.loads(original_completion)
                completion["audit"] = batch["audit"]
                completion_path.write_bytes(local._canonical_bytes(completion))
                with self._pipeline_patches(), self.assertRaisesRegex(
                    batches.FullLocalAnalysisError, "audit"
                ):
                    batches.run_batches(**self._batch_arguments())
                batch_path.write_bytes(original_batch)
                completion_path.write_bytes(original_completion)

        # Replace an exact integer with bool and deliberately repair every
        # downstream immutable hash that can be recomputed from public records.
        # The semantic validator must still reject the alias itself.
        item_path = self.run_root / "items/000003.receipt.json"
        item = json.loads(item_path.read_text())
        item["provider_calls"] = False
        item_path.write_bytes(local._canonical_bytes(item))

        progress_path = self.run_root / "progress/000003.progress.json"
        progress = json.loads(progress_path.read_text())
        progress["item_receipt_sha256"] = local._sha256_file(item_path)
        progress_path.write_bytes(local._canonical_bytes(progress))

        batch = json.loads(batch_path.read_text())
        batch["item_receipts"][-1][1] = local._sha256_file(item_path)
        batch["item_receipts_sha256"] = batches._json_sha(
            batch["item_receipts"]
        )
        batch["audit"]["batch_delta"][-1]["receipt_sha256"] = (
            local._sha256_file(item_path)
        )
        batch["audit"]["batch_delta_sha256"] = batches._json_sha(
            batch["audit"]["batch_delta"]
        )
        batch["audit"]["logical_head_sha256"] = batches._json_sha(
            {
                "previous_logical_head_sha256": batch["audit"][
                    "previous_logical_head_sha256"
                ],
                "batch_index": 1,
                "batch_delta_sha256": batch["audit"]["batch_delta_sha256"],
            }
        )
        batch_path.write_bytes(local._canonical_bytes(batch))

        completion = json.loads(completion_path.read_text())
        completion["progress_head_sha256"] = local._sha256_file(progress_path)
        completion["audit"] = batch["audit"]
        completion_path.write_bytes(local._canonical_bytes(completion))

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "intent/receipt"
        ):
            batches.run_batches(**self._batch_arguments())

    def test_resume_guard_tamper_is_blocked_after_completion_chain_resign(
        self,
    ) -> None:
        for content_id in (4, 5):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(**self._batch_arguments(through_batch=2))

        first_path = self.run_root / "completions/000001.completion.json"
        second_path = self.run_root / "completions/000002.completion.json"
        first_body = first_path.read_bytes()
        second_body = second_path.read_bytes()

        def rewrite_first(mutator) -> None:
            first = json.loads(first_body)
            mutator(first["resume_guard"])
            first_path.write_bytes(local._canonical_bytes(first))
            second = json.loads(second_body)
            second["previous_completion_sha256"] = local._sha256_file(
                first_path
            )
            second_path.write_bytes(local._canonical_bytes(second))

        for alias in (True, "3"):
            with self.subTest(completed_count_alias=alias):
                rewrite_first(
                    lambda guard: guard.__setitem__(
                        "completed_count", alias
                    )
                )
                with self._pipeline_patches(), self.assertRaisesRegex(
                    batches.FullLocalAnalysisError,
                    "resume guard|completion",
                ):
                    batches.run_batches(
                        **self._batch_arguments(through_batch=2)
                    )
                first_path.write_bytes(first_body)
                second_path.write_bytes(second_body)

        rewrite_first(
            lambda guard: guard.__setitem__(
                "target_head_sha256", "0" * 64
            )
        )
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "resume guard"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=2))

    def test_network_type_alias_blocks_after_all_downstream_hashes_are_resigned(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        ledger_path = self.run_root / "network/000003.network.json"
        ledger = json.loads(ledger_path.read_text())
        ledger["maximum_bytes"] = str(ledger["maximum_bytes"])
        ledger_path.write_bytes(local._canonical_bytes(ledger))

        item_path = self.run_root / "items/000003.receipt.json"
        item = json.loads(item_path.read_text())
        item["after"]["network_ledger_sha256"] = local._sha256_file(
            ledger_path
        )
        item_path.write_bytes(local._canonical_bytes(item))

        progress_path = self.run_root / "progress/000003.progress.json"
        progress = json.loads(progress_path.read_text())
        progress["item_receipt_sha256"] = local._sha256_file(item_path)
        progress_path.write_bytes(local._canonical_bytes(progress))

        batch_path = self.run_root / "batches/000001.receipt.json"
        batch = json.loads(batch_path.read_text())
        batch["item_receipts"][-1][1] = local._sha256_file(item_path)
        batch["item_receipts_sha256"] = batches._json_sha(
            batch["item_receipts"]
        )
        batch["audit"]["batch_delta"][-1]["receipt_sha256"] = (
            local._sha256_file(item_path)
        )
        batch["audit"]["batch_delta"][-1]["network_ledger_sha256"] = (
            local._sha256_file(ledger_path)
        )
        batch["audit"]["batch_delta_sha256"] = batches._json_sha(
            batch["audit"]["batch_delta"]
        )
        batch["audit"]["logical_head_sha256"] = batches._json_sha(
            {
                "previous_logical_head_sha256": batch["audit"][
                    "previous_logical_head_sha256"
                ],
                "batch_index": 1,
                "batch_delta_sha256": batch["audit"][
                    "batch_delta_sha256"
                ],
            }
        )
        batch_path.write_bytes(local._canonical_bytes(batch))

        completion_path = self.run_root / "completions/000001.completion.json"
        completion = json.loads(completion_path.read_text())
        completion["progress_head_sha256"] = local._sha256_file(progress_path)
        completion["audit"] = batch["audit"]
        completion_path.write_bytes(local._canonical_bytes(completion))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError),
            "network ledger",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_historical_succeeded_terminal_resign_is_semantically_blocked(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        ledger_path = self.run_root / "network/000003.network.json"
        ledger = json.loads(ledger_path.read_text())
        self.assertEqual(ledger["events"][-1]["outcome"], "succeeded")
        ledger["events"][-1]["outcome"] = "failed"
        ledger_path.write_bytes(local._canonical_bytes(ledger))

        item_path = self.run_root / "items/000003.receipt.json"
        item = json.loads(item_path.read_text())
        transcript = ledger["events"]
        item["result"]["network_transcript"] = transcript
        item["result"]["network_transcript_sha256"] = batches._json_sha(
            transcript
        )
        item["after"]["network_ledger_sha256"] = local._sha256_file(
            ledger_path
        )
        item_path.write_bytes(local._canonical_bytes(item))

        progress_path = self.run_root / "progress/000003.progress.json"
        progress = json.loads(progress_path.read_text())
        progress["item_receipt_sha256"] = local._sha256_file(item_path)
        progress_path.write_bytes(local._canonical_bytes(progress))

        batch_path = self.run_root / "batches/000001.receipt.json"
        batch = json.loads(batch_path.read_text())
        batch["item_receipts"][-1][1] = local._sha256_file(item_path)
        batch["item_receipts_sha256"] = batches._json_sha(
            batch["item_receipts"]
        )
        batch["audit"]["batch_delta"][-1]["receipt_sha256"] = (
            local._sha256_file(item_path)
        )
        batch["audit"]["batch_delta"][-1]["network_ledger_sha256"] = (
            local._sha256_file(ledger_path)
        )
        batch["audit"]["batch_delta_sha256"] = batches._json_sha(
            batch["audit"]["batch_delta"]
        )
        batch["audit"]["logical_head_sha256"] = batches._json_sha(
            {
                "previous_logical_head_sha256": batch["audit"][
                    "previous_logical_head_sha256"
                ],
                "batch_index": 1,
                "batch_delta_sha256": batch["audit"][
                    "batch_delta_sha256"
                ],
            }
        )
        batch_path.write_bytes(local._canonical_bytes(batch))

        completion_path = self.run_root / "completions/000001.completion.json"
        completion = json.loads(completion_path.read_text())
        completion["progress_head_sha256"] = local._sha256_file(
            progress_path
        )
        completion["audit"] = batch["audit"]
        completion_path.write_bytes(local._canonical_bytes(completion))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError),
            "历史item强终态|download provenance|network",
        ):
            batches.run_batches(**self._batch_arguments())

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_runtime_context_and_network_preflight_are_invocation_bounded(
        self,
    ) -> None:
        for content_id in range(4, 26):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=25,
            eligible_count=25,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            image_batch_size=25,
            video_batch_size=25,
        )
        with self._pipeline_patches():
            first = batches.run_batches(**self._batch_arguments())
        self.assertEqual(first["status"], "pilot_complete")

        original_hash = local._sha256_file
        original_output_inventory = batches._output_inventory
        contract_path = self.run_root / "run-contract.json"
        database_path = self.db
        fingerprint_root = self.db.parent / "duplicate-fingerprints"
        pilot_output_bytes = sum(
            path.stat().st_size
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        pilot_output_files = sum(
            1
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        hash_counts = {
            "contract": 0,
            "database": 0,
            "database_bytes": 0,
            "full_output_inventory_calls": 0,
            "full_output_hash_calls": 0,
            "full_output_bytes": 0,
            "item_owned_calls": 0,
            "item_owned_bytes": 0,
        }
        full_output_depth = 0

        def is_owned_output(path: Path) -> bool:
            return any(
                path == root or root in path.parents
                for root in (self.media_root, fingerprint_root)
            )

        def counted_hash(path: Path) -> str:
            resolved = Path(path)
            if resolved == contract_path:
                hash_counts["contract"] += 1
            if resolved == database_path:
                hash_counts["database"] += 1
                hash_counts["database_bytes"] += resolved.stat().st_size
            if resolved.is_file() and is_owned_output(resolved):
                key = "full_output_hash" if full_output_depth else "item_owned"
                hash_counts[f"{key}_calls"] += 1
                byte_key = (
                    "full_output_bytes"
                    if full_output_depth
                    else "item_owned_bytes"
                )
                hash_counts[byte_key] += resolved.stat().st_size
            return original_hash(resolved)

        def counted_output_inventory(paths):
            nonlocal full_output_depth
            hash_counts["full_output_inventory_calls"] += 1
            full_output_depth += 1
            try:
                return original_output_inventory(paths)
            finally:
                full_output_depth -= 1

        with patch.object(
            batches, "_target_row_map", wraps=batches._target_row_map
        ) as row_map, patch.object(
            batches,
            "_recover_validate_network_ledgers",
            wraps=batches._recover_validate_network_ledgers,
        ) as network_preflight, patch.object(
            local, "_sha256_file", side_effect=counted_hash
        ), patch.object(
            batches, "_output_inventory", side_effect=counted_output_inventory
        ), self._pipeline_patches():
            second = batches.run_batches(
                **self._batch_arguments(through_batch=2)
            )
        self.assertEqual(second["status"], "eligible_complete")
        self.assertEqual(second["processed_this_invocation"], 22)
        self.assertEqual(row_map.call_count, 1)
        self.assertEqual(hash_counts["contract"], 1)
        self.assertEqual(network_preflight.call_count, 1)
        self.assertEqual(hash_counts["database"], 1)
        self.assertEqual(
            hash_counts["database_bytes"],
            self.db.stat().st_size,
        )
        self.assertEqual(hash_counts["full_output_inventory_calls"], 0)
        final_output_bytes = sum(
            path.stat().st_size
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        final_output_files = sum(
            1
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        self.assertEqual(hash_counts["full_output_bytes"], 0)
        self.assertEqual(hash_counts["full_output_hash_calls"], 0)
        self.assertLessEqual(
            hash_counts["item_owned_bytes"],
            pilot_output_bytes
            + 16 * (final_output_bytes - pilot_output_bytes),
        )
        self.assertLessEqual(
            hash_counts["item_owned_calls"],
            pilot_output_files
            + 16 * (final_output_files - pilot_output_files),
        )
        self.batch_performance_counters = {
            **hash_counts,
            "target_row_map_calls": row_map.call_count,
            "network_preflight_calls": network_preflight.call_count,
            "pilot_output_files": pilot_output_files,
            "final_output_files": final_output_files,
        }

    def test_multi_batch_authority_and_periodic_logical_checkpoints(self) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            image_batch_size=25,
            video_batch_size=2,
        )
        with self._pipeline_patches():
            pilot = batches.run_batches(**self._batch_arguments())
        fingerprint_root = self.db.parent / "duplicate-fingerprints"
        pilot_output_bytes = sum(
            path.stat().st_size
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        pilot_output_files = sum(
            1
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        original_hash = local._sha256_file
        original_output_inventory = batches._output_inventory
        full_output_depth = 0
        counts = {
            "database": 0,
            "database_bytes": 0,
            "full_output_inventory_calls": 0,
            "full_output_hash_calls": 0,
            "full_output_bytes": 0,
            "item_owned_calls": 0,
            "item_owned_bytes": 0,
        }

        def counted_hash(path: Path) -> str:
            resolved = Path(path)
            if resolved == self.db:
                counts["database"] += 1
                counts["database_bytes"] += resolved.stat().st_size
            if resolved.is_file() and any(
                resolved == root or root in resolved.parents
                for root in (self.media_root, fingerprint_root)
            ):
                if full_output_depth:
                    counts["full_output_hash_calls"] += 1
                    counts["full_output_bytes"] += resolved.stat().st_size
                else:
                    counts["item_owned_calls"] += 1
                    counts["item_owned_bytes"] += resolved.stat().st_size
            return original_hash(resolved)

        def counted_output_inventory(paths):
            nonlocal full_output_depth
            counts["full_output_inventory_calls"] += 1
            full_output_depth += 1
            try:
                return original_output_inventory(paths)
            finally:
                full_output_depth -= 1

        with patch.object(
            local, "_sha256_file", side_effect=counted_hash
        ), patch.object(
            batches, "_output_inventory", side_effect=counted_output_inventory
        ), self._pipeline_patches():
            completed = batches.run_batches(
                **self._batch_arguments(
                    through_batch=3,
                    max_new_batches=2,
                )
            )
        self.assertEqual(pilot["status"], "pilot_complete")
        self.assertEqual(completed["status"], "eligible_complete")
        self.assertEqual(completed["processed_this_invocation"], 4)
        final_output_bytes = sum(
            path.stat().st_size
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        final_output_files = sum(
            1
            for root in (self.media_root, fingerprint_root)
            for path in root.rglob("*")
            if path.is_file()
        )
        self.assertEqual(counts["database"], 1)
        self.assertEqual(
            counts["database_bytes"],
            self.db.stat().st_size,
        )
        self.assertEqual(counts["full_output_inventory_calls"], 0)
        self.assertEqual(counts["full_output_hash_calls"], 0)
        self.assertEqual(counts["full_output_bytes"], 0)
        self.assertLessEqual(
            counts["item_owned_calls"],
            pilot_output_files
            + 16 * (final_output_files - pilot_output_files),
        )
        self.assertLessEqual(
            counts["item_owned_bytes"],
            pilot_output_bytes
            + 16 * (final_output_bytes - pilot_output_bytes),
        )
        receipts = [
            json.loads(
                (self.run_root / f"batches/{index:06d}.receipt.json").read_text()
            )
            for index in range(1, 4)
        ]
        self.assertEqual(
            [row["content_ids"] for row in receipts],
            [[1, 2, 3], [4, 5], [6, 7]],
        )
        self.assertEqual(
            [row["audit"]["coverage"] for row in receipts],
            [
                "logical_database_checkpoint",
                "owned_delta",
                "logical_database_checkpoint",
            ],
        )
        self.assertEqual(
            [
                row["audit"]["latest_logical_checkpoint_batch"]
                for row in receipts
            ],
            [1, 1, 3],
        )
        self.assertIsNone(receipts[1]["audit"]["full_checkpoint"])
        self.assertIsNotNone(receipts[2]["audit"]["full_checkpoint"])
        logical_keys = {
            "contract_sha",
            "batch_index",
            "completed_count",
            "processing_prefix_sha",
            "resume_guard_sha",
            "target_head",
            "output_head",
            "remaining_head",
            "provider",
            "protected",
            "source",
            "schema",
            "derived_sequence",
            "logical_head",
        }
        for index, receipt in enumerate(receipts, 1):
            self.assertEqual(set(receipt["after"]["database"]), logical_keys)
            self.assertNotIn("content_sha256", receipt["after"]["database"])
            self.assertNotIn("byte_size", receipt["after"]["database"])
            checkpoint = receipt["audit"]["full_checkpoint"]
            if checkpoint is not None:
                self.assertEqual(set(checkpoint), logical_keys)
                self.assertEqual(checkpoint, receipt["after"]["database"])
                self.assertEqual(checkpoint["batch_index"], index)
        self.assertEqual(
            receipts[1]["audit"]["previous_logical_head_sha256"],
            receipts[0]["audit"]["logical_head_sha256"],
        )
        self.assertEqual(
            receipts[2]["audit"]["previous_logical_head_sha256"],
            receipts[1]["audit"]["logical_head_sha256"],
        )
        completion = json.loads(
            (self.run_root / "completions/000003.completion.json").read_text()
        )
        self.assertEqual(completion["audit"], receipts[-1]["audit"])
        self.assertEqual(set(completion["database"]), logical_keys)
        for ordinal in range(1, 8):
            item = json.loads(
                (
                    self.run_root
                    / f"items/{ordinal:06d}.receipt.json"
                ).read_text()
            )
            progress = json.loads(
                (
                    self.run_root
                    / f"progress/{ordinal:06d}.progress.json"
                ).read_text()
            )
            self.assertNotIn("database", item["after"])
            self.assertNotIn("database", progress)
        second_intent = json.loads(
            (self.run_root / "batches/000002.intent.json").read_text()
        )
        self.assertEqual(set(second_intent["before"]["database"]), logical_keys)

        before = self.fixture._tree_state(self.analysis_root)
        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "eligible已全部闭合"
        ):
            batches.run_batches(
                **self._batch_arguments(
                    through_batch=4,
                    max_new_batches=1,
                )
            )
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)

    def test_real_video_download_then_process_does_not_advance_sequence_on_upsert(
        self,
    ) -> None:
        database = self.root / "real-video-sequence.sqlite3"
        shutil.copy2(self.source_db, database)
        output_root = self.root / "real-video-output"

        def sequence_state() -> tuple[int, int]:
            connection = sqlite3.connect(database)
            try:
                sequence = connection.execute(
                    "SELECT seq FROM sqlite_sequence "
                    "WHERE name='evidence_artifacts'"
                ).fetchone()
                maximum = connection.execute(
                    "SELECT COALESCE(MAX(id),0) FROM evidence_artifacts"
                ).fetchone()
                return (
                    int(sequence[0]) if sequence is not None else 0,
                    int(maximum[0]),
                )
            finally:
                connection.close()

        def fake_download(_urls, target: Path, **_kwargs) -> Path:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"video" * 1000)
            return target

        before = sequence_state()
        with patch.object(media, "_download_video", side_effect=fake_download):
            downloaded = media.download_video_sources(
                1,
                self.source_fixtures[1]["urls"],
                db_path=database,
                media_root=output_root,
                reuse_existing=False,
            )
        after_download = sequence_state()
        with patch.object(
            media, "_valid_media", return_value=True
        ), patch.object(
            media, "_run_processing_slot", return_value=downloaded
        ):
            media.process_video_evidence(
                1,
                Path(downloaded.local_path),
                db_path=database,
                media_root=output_root,
            )
        after_process = sequence_state()

        self.assertEqual(after_download[0], after_download[1])
        self.assertEqual(
            after_process[0],
            after_process[1],
            msg=(
                "evidence_artifacts UPSERT advanced sequence without a row: "
                f"before={before} after_download={after_download} "
                f"after_process={after_process}"
            ),
        )

    def test_owned_delta_resume_blocks_same_size_old_output_tamper_before_batch3(
        self,
    ) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(**self._batch_arguments(through_batch=2))
        old_output = (
            self.media_root
            / str(self.source_fixtures[1]["link_id"])
            / "asr.json"
        )
        original = old_output.read_bytes()
        replacement = bytes([original[0] ^ 1]) + original[1:]
        self.assertEqual(len(replacement), len(original))
        inode = old_output.stat().st_ino
        old_output.write_bytes(replacement)
        self.assertEqual(old_output.stat().st_ino, inode)
        self.assertEqual(old_output.stat().st_size, len(original))
        before = self.fixture._tree_state(self.analysis_root)
        calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "resume guard"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=3))

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, calls)
        self.assertFalse(
            (self.run_root / "batches/000003.intent.json").exists()
        )

    def test_owned_delta_resume_blocks_old_direction_tamper_before_batch3(
        self,
    ) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(**self._batch_arguments(through_batch=2))
        connection = sqlite3.connect(self.db)
        try:
            connection.execute(
                "UPDATE content_items SET evaluation_content_direction='unknown' "
                "WHERE id=1"
            )
            connection.commit()
        finally:
            connection.close()
        before = self.fixture._tree_state(self.analysis_root)
        calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "resume guard"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=3))

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, calls)
        self.assertFalse(
            (self.run_root / "batches/000003.intent.json").exists()
        )

    def test_future_eligible_managed_row_pollution_blocks_before_batch3(
        self,
    ) -> None:
        for content_id in range(4, 8):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=7,
            eligible_count=7,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(**self._batch_arguments(through_batch=2))
        connection = sqlite3.connect(self.db)
        try:
            source = connection.execute(
                "SELECT * FROM evidence_artifacts "
                "WHERE content_id=6 AND artifact_type='media_source'"
            ).fetchone()
            connection.execute(
                """INSERT INTO evidence_artifacts(
                       content_id,artifact_type,local_path,status,byte_size,
                       sha256,captured_at,processor_version,metadata_json,created_at
                   ) VALUES (6,'media',?,'available',?,?,?,?,'{}',?)""",
                (source[2], source[4], source[5], source[6], "injected", source[9]),
            )
            connection.commit()
        finally:
            connection.close()
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "eligible target.*initial baseline"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=3))

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)
        self.assertFalse(
            (self.run_root / "batches/000003.intent.json").exists()
        )
        self.assertFalse(
            (self.run_root / "completions/000003.completion.json").exists()
        )

    def test_historical_completion_coordinated_resign_is_rederived_and_blocked(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self.fixture._add_source_content(5)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(**self._batch_arguments(through_batch=2))
        first_path = self.run_root / "completions/000001.completion.json"
        second_path = self.run_root / "completions/000002.completion.json"
        first = json.loads(first_path.read_text())
        first["status"] = "partial"
        first_path.write_bytes(local._canonical_bytes(first))
        second = json.loads(second_path.read_text())
        second["previous_completion_sha256"] = local._sha256_file(first_path)
        second_path.write_bytes(local._canonical_bytes(second))
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError, "completion 1.*重派生"
        ):
            batches.run_batches(**self._batch_arguments(through_batch=2))

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_historical_logical_database_checkpoint_cannot_be_coordinated_resigned(
        self,
    ) -> None:
        for content_id in (4, 5):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(**self._batch_arguments(through_batch=2))

        first_batch_path = self.run_root / "batches/000001.receipt.json"
        first_batch = json.loads(first_batch_path.read_text())
        forged_database = dict(first_batch["after"]["database"])
        forged_database["provider"] = "f" * 64
        forged_database["logical_head"] = batches._json_sha(
            {
                key: value
                for key, value in forged_database.items()
                if key != "logical_head"
            }
        )
        first_batch["after"]["database"] = forged_database
        first_batch["audit"]["full_checkpoint"] = forged_database
        first_batch_path.write_bytes(local._canonical_bytes(first_batch))
        first_batch_sha = local._sha256_file(first_batch_path)

        second_intent_path = self.run_root / "batches/000002.intent.json"
        second_intent = json.loads(second_intent_path.read_text())
        second_intent["previous_batch_receipt_sha256"] = first_batch_sha
        second_intent["before"]["database"] = forged_database
        second_intent_path.write_bytes(local._canonical_bytes(second_intent))

        second_batch_path = self.run_root / "batches/000002.receipt.json"
        second_batch = json.loads(second_batch_path.read_text())
        second_batch["previous_batch_receipt_sha256"] = first_batch_sha
        second_batch["intent_sha256"] = local._sha256_file(
            second_intent_path
        )
        second_batch_path.write_bytes(local._canonical_bytes(second_batch))

        first_completion_path = (
            self.run_root / "completions/000001.completion.json"
        )
        first_completion = json.loads(first_completion_path.read_text())
        first_completion["database"] = forged_database
        first_completion["audit"] = first_batch["audit"]
        first_completion_path.write_bytes(
            local._canonical_bytes(first_completion)
        )

        second_completion_path = (
            self.run_root / "completions/000002.completion.json"
        )
        second_completion = json.loads(second_completion_path.read_text())
        second_completion["previous_completion_sha256"] = (
            local._sha256_file(first_completion_path)
        )
        second_completion_path.write_bytes(
            local._canonical_bytes(second_completion)
        )
        before = self.fixture._tree_state(self.analysis_root)
        before_calls = dict(self.calls)

        with patch.object(
            local, "_local_tools", return_value=self.tools
        ), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "logical database checkpoint",
        ):
            batches.plan_batches(**self._batch_arguments(through_batch=2))
        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

        with self._pipeline_patches(), self.assertRaisesRegex(
            batches.FullLocalAnalysisError,
            "logical database checkpoint|历史物理",
        ):
            batches.run_batches(**self._batch_arguments(through_batch=2))

        self.assertEqual(self.fixture._tree_state(self.analysis_root), before)
        self.assertEqual(self.calls, before_calls)

    def test_historical_item_sequence_prefix_rejects_coordinated_type_aliases(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        item_two_path = self.run_root / "items/000002.receipt.json"
        item_three_intent_path = self.run_root / "items/000003.intent.json"
        ledger_three_path = self.run_root / "network/000003.network.json"
        item_three_path = self.run_root / "items/000003.receipt.json"
        progress_two_path = self.run_root / "progress/000002.progress.json"
        progress_three_path = self.run_root / "progress/000003.progress.json"
        batch_path = self.run_root / "batches/000001.receipt.json"
        completion_path = (
            self.run_root / "completions/000001.completion.json"
        )
        paths = (
            item_two_path,
            item_three_intent_path,
            ledger_three_path,
            item_three_path,
            progress_two_path,
            progress_three_path,
            batch_path,
            completion_path,
        )
        original = {path: path.read_bytes() for path in paths}
        original_sequence = json.loads(original[item_two_path])[
            "after"
        ]["sequences"]["content_items"]

        def rewrite(alias) -> None:
            for path, body in original.items():
                path.write_bytes(body)
            item_two = json.loads(original[item_two_path])
            item_two["after"]["sequences"]["content_items"] = alias
            item_two_path.write_bytes(local._canonical_bytes(item_two))

            item_three_intent = json.loads(original[item_three_intent_path])
            item_three_intent["previous_item_receipt_sha256"] = (
                local._sha256_file(item_two_path)
            )
            item_three_intent["before"]["sequences"][
                "content_items"
            ] = alias
            item_three_intent_path.write_bytes(
                local._canonical_bytes(item_three_intent)
            )

            ledger_three = json.loads(original[ledger_three_path])
            ledger_three["intent_sha256"] = local._sha256_file(
                item_three_intent_path
            )
            ledger_three_path.write_bytes(local._canonical_bytes(ledger_three))

            item_three = json.loads(original[item_three_path])
            item_three["intent_sha256"] = local._sha256_file(
                item_three_intent_path
            )
            item_three["previous_item_receipt_sha256"] = (
                local._sha256_file(item_two_path)
            )
            item_three["after"]["network_ledger_sha256"] = (
                local._sha256_file(ledger_three_path)
            )
            item_three_path.write_bytes(local._canonical_bytes(item_three))

            progress_two = json.loads(original[progress_two_path])
            progress_two["item_receipt_sha256"] = local._sha256_file(
                item_two_path
            )
            progress_two_path.write_bytes(local._canonical_bytes(progress_two))
            progress_three = json.loads(original[progress_three_path])
            progress_three["previous_progress_sha256"] = local._sha256_file(
                progress_two_path
            )
            progress_three["item_receipt_sha256"] = local._sha256_file(
                item_three_path
            )
            progress_three_path.write_bytes(
                local._canonical_bytes(progress_three)
            )

            batch = json.loads(original[batch_path])
            batch["item_receipts"][1][1] = local._sha256_file(item_two_path)
            batch["item_receipts"][2][1] = local._sha256_file(item_three_path)
            batch["item_receipts_sha256"] = batches._json_sha(
                batch["item_receipts"]
            )
            batch["audit"]["batch_delta"][1]["receipt_sha256"] = (
                local._sha256_file(item_two_path)
            )
            batch["audit"]["batch_delta"][2]["receipt_sha256"] = (
                local._sha256_file(item_three_path)
            )
            batch["audit"]["batch_delta"][2][
                "network_ledger_sha256"
            ] = local._sha256_file(ledger_three_path)
            batch["audit"]["batch_delta_sha256"] = batches._json_sha(
                batch["audit"]["batch_delta"]
            )
            batch["audit"]["logical_head_sha256"] = batches._json_sha(
                {
                    "previous_logical_head_sha256": batch["audit"][
                        "previous_logical_head_sha256"
                    ],
                    "batch_index": 1,
                    "batch_delta_sha256": batch["audit"][
                        "batch_delta_sha256"
                    ],
                }
            )
            batch_path.write_bytes(local._canonical_bytes(batch))

            completion = json.loads(original[completion_path])
            completion["progress_head_sha256"] = local._sha256_file(
                progress_three_path
            )
            completion["audit"] = batch["audit"]
            completion_path.write_bytes(local._canonical_bytes(completion))

        aliases = (
            bool(original_sequence),
            float(original_sequence),
            str(original_sequence),
        )
        for alias in aliases:
            with self.subTest(alias=repr(alias)):
                rewrite(alias)
                before = self.fixture._tree_state(self.analysis_root)
                before_calls = dict(self.calls)
                with self._pipeline_patches(), self.assertRaisesRegex(
                    batches.FullLocalAnalysisError,
                    "sequence|intent/receipt|历史item强终态",
                ):
                    batches.run_batches(**self._batch_arguments())
                self.assertEqual(
                    self.fixture._tree_state(self.analysis_root), before
                )
                self.assertEqual(self.calls, before_calls)
        for path, body in original.items():
            path.write_bytes(body)

    def test_batch_intent_before_rejects_logical_and_sequence_float_aliases(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        intent_path = self.run_root / "batches/000001.intent.json"
        receipt_path = self.run_root / "batches/000001.receipt.json"
        original_intent = intent_path.read_bytes()
        original_receipt = receipt_path.read_bytes()

        def rewrite(kind: str) -> None:
            intent = json.loads(original_intent)
            if kind == "logical":
                intent["before"]["database"]["completed_count"] = 0.0
            else:
                sequence = intent["before"]["sequences"]["content_items"]
                intent["before"]["sequences"]["content_items"] = float(
                    sequence
                )
            intent_path.write_bytes(local._canonical_bytes(intent))
            receipt = json.loads(original_receipt)
            receipt["intent_sha256"] = local._sha256_file(intent_path)
            receipt_path.write_bytes(local._canonical_bytes(receipt))

        for kind in ("logical", "sequence"):
            for action in ("plan", "apply"):
                with self.subTest(kind=kind, action=action):
                    intent_path.write_bytes(original_intent)
                    receipt_path.write_bytes(original_receipt)
                    rewrite(kind)
                    before = self.fixture._tree_state(self.analysis_root)
                    before_calls = dict(self.calls)
                    if action == "plan":
                        with patch.object(
                            local, "_local_tools", return_value=self.tools
                        ), self.assertRaisesRegex(
                            batches.FullLocalAnalysisError,
                            "logical database checkpoint|sequence|batch intent",
                        ):
                            batches.plan_batches(**self._batch_arguments())
                    else:
                        with self._pipeline_patches(), self.assertRaisesRegex(
                            batches.FullLocalAnalysisError,
                            "logical database checkpoint|sequence|batch intent",
                        ):
                            batches.run_batches(**self._batch_arguments())
                    self.assertEqual(
                        self.fixture._tree_state(self.analysis_root), before
                    )
                    self.assertEqual(self.calls, before_calls)
        intent_path.write_bytes(original_intent)
        receipt_path.write_bytes(original_receipt)

    def test_batch_nested_contract_aliases_block_plan_and_apply_zero_write(
        self,
    ) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        intent_path = self.run_root / "batches/000001.intent.json"
        receipt_path = self.run_root / "batches/000001.receipt.json"
        original_intent = intent_path.read_bytes()
        original_receipt = receipt_path.read_bytes()

        def rewrite(kind: str) -> None:
            intent = json.loads(original_intent)
            receipt = json.loads(original_receipt)
            intent_changed = True
            if kind == "before_provider":
                rows = intent["before"]["provider"]["provider_usage"]["rows"]
                intent["before"]["provider"]["provider_usage"]["rows"] = float(
                    rows
                )
            elif kind == "before_outputs":
                intent["before"]["outputs"]["media"]["files"] = 99
            elif kind == "before_protected":
                protected = intent["before"]["protected"]
                protected["content_ids"][0] = float(protected["content_ids"][0])
                intent["before"]["protected_sha256"] = batches._json_sha(
                    protected
                )
            elif kind == "planned_baseline":
                planned = intent["planned_baselines"]
                planned[0]["content_id"] = float(planned[0]["content_id"])
                planned_sha = batches._json_sha(planned)
                intent["planned_baselines_sha256"] = planned_sha
                intent["before"]["protected"][
                    "planned_baselines_sha256"
                ] = planned_sha
                intent["before"]["protected_sha256"] = batches._json_sha(
                    intent["before"]["protected"]
                )
            elif kind == "after_provider":
                intent_changed = False
                rows = receipt["after"]["provider"]["provider_usage"]["rows"]
                receipt["after"]["provider"]["provider_usage"]["rows"] = float(
                    rows
                )
            else:
                raise AssertionError(kind)
            if intent_changed:
                intent_path.write_bytes(local._canonical_bytes(intent))
                receipt["intent_sha256"] = local._sha256_file(intent_path)
            receipt_path.write_bytes(local._canonical_bytes(receipt))

        kinds = (
            "before_provider",
            "before_outputs",
            "before_protected",
            "planned_baseline",
            "after_provider",
        )
        for kind in kinds:
            for action in ("plan", "apply"):
                with self.subTest(kind=kind, action=action):
                    intent_path.write_bytes(original_intent)
                    receipt_path.write_bytes(original_receipt)
                    rewrite(kind)
                    before = self.fixture._tree_state(self.analysis_root)
                    before_calls = dict(self.calls)
                    if action == "plan":
                        with patch.object(
                            local, "_local_tools", return_value=self.tools
                        ), self.assertRaises(
                            batches.FullLocalAnalysisError
                        ):
                            batches.plan_batches(**self._batch_arguments())
                    else:
                        with self._pipeline_patches(), self.assertRaises(
                            batches.FullLocalAnalysisError
                        ):
                            batches.run_batches(**self._batch_arguments())
                    self.assertEqual(
                        self.fixture._tree_state(self.analysis_root), before
                    )
                    self.assertEqual(self.calls, before_calls)
        intent_path.write_bytes(original_intent)
        receipt_path.write_bytes(original_receipt)

    def test_historical_batch_output_prefix_rejects_coordinated_resign(
        self,
    ) -> None:
        self.fixture._add_source_content(4)
        self.fixture._add_source_content(5)
        self.profile = batches.HistoryProfile(
            universe_count=5,
            eligible_count=5,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            video_batch_size=2,
        )
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            batches.run_batches(
                **self._batch_arguments(through_batch=2)
            )

        paths = {
            "batch1": self.run_root / "batches/000001.receipt.json",
            "intent2": self.run_root / "batches/000002.intent.json",
            "batch2": self.run_root / "batches/000002.receipt.json",
            "completion1": (
                self.run_root / "completions/000001.completion.json"
            ),
            "completion2": (
                self.run_root / "completions/000002.completion.json"
            ),
        }
        original = {name: path.read_bytes() for name, path in paths.items()}

        def rewrite() -> None:
            batch1 = json.loads(original["batch1"])
            media_ownership = batch1["after"]["outputs"]["ownership"][
                "media"
            ]
            media_ownership["rows"][0]["device"] += 1
            media_ownership["rows_sha256"] = batches._json_sha(
                media_ownership["rows"]
            )
            paths["batch1"].write_bytes(local._canonical_bytes(batch1))

            intent2 = json.loads(original["intent2"])
            intent2["previous_batch_receipt_sha256"] = local._sha256_file(
                paths["batch1"]
            )
            intent2["before"]["outputs"] = batch1["after"]["outputs"]
            paths["intent2"].write_bytes(local._canonical_bytes(intent2))

            batch2 = json.loads(original["batch2"])
            batch2["previous_batch_receipt_sha256"] = local._sha256_file(
                paths["batch1"]
            )
            batch2["intent_sha256"] = local._sha256_file(paths["intent2"])
            paths["batch2"].write_bytes(local._canonical_bytes(batch2))

            completion1 = json.loads(original["completion1"])
            completion1["outputs"] = batch1["after"]["outputs"]
            paths["completion1"].write_bytes(
                local._canonical_bytes(completion1)
            )
            completion2 = json.loads(original["completion2"])
            completion2["previous_completion_sha256"] = local._sha256_file(
                paths["completion1"]
            )
            paths["completion2"].write_bytes(
                local._canonical_bytes(completion2)
            )

        for action in ("plan", "apply"):
            with self.subTest(action=action):
                for name, path in paths.items():
                    path.write_bytes(original[name])
                rewrite()
                before = self.fixture._tree_state(self.analysis_root)
                before_calls = dict(self.calls)
                if action == "plan":
                    with patch.object(
                        local, "_local_tools", return_value=self.tools
                    ), self.assertRaises(
                        batches.FullLocalAnalysisError
                    ):
                        batches.plan_batches(
                            **self._batch_arguments(through_batch=2)
                        )
                else:
                    with self._pipeline_patches(), self.assertRaises(
                        batches.FullLocalAnalysisError
                    ):
                        batches.run_batches(
                            **self._batch_arguments(through_batch=2)
                        )
                self.assertEqual(
                    self.fixture._tree_state(self.analysis_root), before
                )
                self.assertEqual(self.calls, before_calls)
        for name, path in paths.items():
            path.write_bytes(original[name])

    def test_shared_batch_cap_closes_partial_and_next_stop_starts_suffix(self) -> None:
        with patch.object(
            batches, "BATCH_DOWNLOAD_CAP_BYTES", 5_000
        ), self._pipeline_patches():
            first = batches.run_batches(**self._batch_arguments())
            same = batches.run_batches(**self._batch_arguments())
            second = batches.run_batches(
                **self._batch_arguments(through_batch=2)
            )
            third = batches.run_batches(
                **self._batch_arguments(through_batch=3)
            )
        first_batch = json.loads(
            (self.run_root / "batches/000001.receipt.json").read_text()
        )
        second_batch = json.loads(
            (self.run_root / "batches/000002.receipt.json").read_text()
        )
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["processed_this_invocation"], 1)
        self.assertEqual(first_batch["status"], "budget_exhausted_partial")
        self.assertEqual(first_batch["content_ids"], [1, 2, 3])
        self.assertEqual(first_batch["completed_content_ids"], [1])
        self.assertEqual(first_batch["unstarted_content_ids"], [2, 3])
        self.assertTrue(same["idempotent"])
        self.assertEqual(same["processed_this_invocation"], 0)
        self.assertEqual(second["status"], "partial")
        self.assertEqual(second_batch["content_ids"], [2, 3])
        self.assertEqual(second_batch["completed_content_ids"], [2])
        self.assertEqual(second_batch["unstarted_content_ids"], [3])
        self.assertEqual(third["status"], "eligible_complete")
        self.assertEqual(self.calls, {"media": 3, "evaluation": 3, "fingerprint": 3})
        self.assertFalse((self.run_root / "items/000004.intent.json").exists())

    def test_batch_completed_and_unstarted_ids_require_exact_ints(self) -> None:
        with patch.object(
            batches, "BATCH_DOWNLOAD_CAP_BYTES", 5_000
        ), self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        batch_path = self.run_root / "batches/000001.receipt.json"
        original = batch_path.read_bytes()
        mutations = {
            "completed_bool": lambda value: value[
                "completed_content_ids"
            ].__setitem__(0, True),
            "unstarted_float": lambda value: value[
                "unstarted_content_ids"
            ].__setitem__(0, 2.0),
        }
        for label, mutate in mutations.items():
            with self.subTest(field=label):
                value = json.loads(original)
                mutate(value)
                batch_path.write_bytes(local._canonical_bytes(value))
                with (
                    patch.object(
                        batches, "BATCH_DOWNLOAD_CAP_BYTES", 5_000
                    ),
                    self._pipeline_patches(),
                    self.assertRaisesRegex(
                        batches.FullLocalAnalysisError, "batch"
                    ),
                ):
                    batches.run_batches(**self._batch_arguments())
                batch_path.write_bytes(original)

    def test_symlink_hardlink_unknown_output_and_root_replacement_block(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())

        symlink = self.media_root / "symlink"
        symlink.symlink_to(self.run_root / "run-contract.json")
        with self._pipeline_patches(), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        symlink.unlink()

        receipt = self.run_root / "items/000001.receipt.json"
        hardlink = self.run_root / "receipt-hardlink.json"
        os.link(receipt, hardlink)
        with self._pipeline_patches(), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        hardlink.unlink()

        unknown_output = self.media_root / "unknown.bin"
        unknown_output.write_bytes(b"unknown")
        with self._pipeline_patches(), self.assertRaises(
            batches.FullLocalAnalysisError
        ):
            batches.run_batches(**self._batch_arguments())
        unknown_output.unlink()

        moved = self.analysis_root / "moved-media"
        self.media_root.replace(moved)
        self.media_root.mkdir()
        with self._pipeline_patches(), self.assertRaises(
            batches.FullLocalAnalysisError
        ):
            batches.run_batches(**self._batch_arguments())

    def test_post_completion_wal_unknown_link_and_output_root_changes_block(self) -> None:
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        unknown = self.run_root / "unknown.json"
        unknown.write_text("{}\n")
        with self._pipeline_patches(), self.assertRaises(
            batches.FullLocalAnalysisError
        ):
            batches.run_batches(**self._batch_arguments())
        unknown.unlink()

        empty = self.media_root / "unexpected-empty"
        empty.mkdir()
        with self._pipeline_patches(), self.assertRaises(
            (batches.FullLocalAnalysisError, local.LocalAnalysisCanaryError)
        ):
            batches.run_batches(**self._batch_arguments())
        empty.rmdir()

        wal = Path(f"{self.db}-wal")
        wal.write_bytes(b"unknown-wal")
        with self._pipeline_patches(), self.assertRaises(
            batches.FullLocalAnalysisError
        ):
            batches.run_batches(**self._batch_arguments())
        wal.unlink()

        connection = sqlite3.connect(self.db)
        try:
            connection.execute("UPDATE content_items SET link_id='ZZZZZZ' WHERE id=1")
            connection.commit()
        finally:
            connection.close()
        with self._pipeline_patches(), self.assertRaises(
            batches.FullLocalAnalysisError
        ):
            batches.run_batches(**self._batch_arguments())

    def test_heavy_checkpoint_calls_are_batch_bounded_and_receipts_are_linear(self) -> None:
        with patch.object(
            batches,
            "_database_identity",
            wraps=batches._database_identity,
        ) as database_identity, patch.object(
            batches,
            "_protected_snapshot",
            wraps=batches._protected_snapshot,
        ) as protected, patch.object(
            batches,
            "_output_inventory",
            wraps=batches._output_inventory,
        ) as output_inventory, self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
        self.assertLessEqual(database_identity.call_count, 8)
        self.assertLessEqual(protected.call_count, 4)
        self.assertLessEqual(output_inventory.call_count, 8)
        sizes = [
            (self.run_root / f"items/{index:06d}.receipt.json").stat().st_size
            for index in range(1, 4)
        ]
        self.assertLess(max(sizes), min(sizes) * 2)
        self.assertLess(max(sizes), 64 * 1024)

    def test_101_batch_history_is_read_once_and_new_heads_append_linearly(
        self,
    ) -> None:
        for content_id in range(4, 129):
            self.fixture._add_source_content(content_id)
        self.profile = batches.HistoryProfile(
            universe_count=128,
            eligible_count=128,
            static_deferred_count=0,
            missing_universe_count=0,
            first_batch_ids=(1, 2, 3),
            image_batch_size=1,
            video_batch_size=1,
        )
        complexity = {
            "source_completion_canonical_calls": 0,
            "source_completion_canonical_bytes": 0,
            "eligible_baseline_map_calls": 0,
            "eligible_baseline_rows": 0,
        }
        source_completion_keys = {
            "batch_evidence",
            "byte_size",
            "contract",
            "database",
            "explicit_ids_membership_sha256",
            "path",
            "receipts_total",
            "sha256",
            "target_count",
        }
        original_canonical = local._canonical_bytes
        original_eligible_baseline_map = batches._eligible_baseline_map

        def counted_canonical(value):
            body = original_canonical(value)
            if isinstance(value, dict) and set(value) == source_completion_keys:
                complexity["source_completion_canonical_calls"] += 1
                complexity["source_completion_canonical_bytes"] += len(body)
            return body

        def counted_eligible_baseline_map(contract):
            complexity["eligible_baseline_map_calls"] += 1
            complexity["eligible_baseline_rows"] += len(
                contract["eligible_target_baseline"]
            )
            return original_eligible_baseline_map(contract)

        logical_heads_patcher = patch.object(
            batches,
            "_logical_global_heads",
            wraps=batches._logical_global_heads,
        )
        batch_cursor_patcher = patch.object(
            batches,
            "_batch_at_cursor",
            wraps=batches._batch_at_cursor,
        )
        logical_checkpoint_patcher = patch.object(
            batches,
            "_logical_database_checkpoint",
            wraps=batches._logical_database_checkpoint,
        )
        canonical_patcher = patch.object(
            local, "_canonical_bytes", side_effect=counted_canonical
        )
        eligible_baseline_patcher = patch.object(
            batches,
            "_eligible_baseline_map",
            side_effect=counted_eligible_baseline_map,
        )
        logical_heads = logical_heads_patcher.start()
        batch_cursor = batch_cursor_patcher.start()
        logical_checkpoints = logical_checkpoint_patcher.start()
        canonical_patcher.start()
        eligible_baseline_patcher.start()
        self.addCleanup(logical_heads_patcher.stop)
        self.addCleanup(batch_cursor_patcher.stop)
        self.addCleanup(logical_checkpoint_patcher.stop)
        self.addCleanup(canonical_patcher.stop)
        self.addCleanup(eligible_baseline_patcher.stop)
        with self._pipeline_patches():
            batches.run_batches(**self._batch_arguments())
            for through_batch in (26, 51, 76, 101):
                batches.run_batches(
                    **self._batch_arguments(
                        through_batch=through_batch,
                        max_new_batches=25,
                    )
                )

        history_paths = {
            path.resolve()
            for root in (
                self.run_root / "items",
                self.run_root / "batches",
                self.run_root / "progress",
                self.run_root / "completions",
                self.run_root / "network",
            )
            for path in root.iterdir()
            if path.is_file()
        }
        history_bytes = sum(path.stat().st_size for path in history_paths)
        original_read = batches._read_json
        original_protected = batches._protected_snapshot
        original_provider = batches._provider_snapshot
        original_source_groups = batches._source_groups_snapshot
        original_target_rows = local._target_rows
        counters = {
            "history_reads": 0,
            "history_bytes": 0,
            "protected_rows": [],
            "provider_rows": [],
            "source_rows": [],
            "bulk_target_calls": 0,
            "bulk_target_ids": 0,
            "bulk_target_rows": 0,
            "ownership_item_builds": 0,
        }

        def counted_read(path: Path, *, label: str):
            resolved = Path(path).resolve()
            if resolved in history_paths:
                counters["history_reads"] += 1
                counters["history_bytes"] += resolved.stat().st_size
            return original_read(path, label=label)

        def counted_protected(connection, content_ids):
            value = original_protected(connection, content_ids)
            counters["protected_rows"].append(
                sum(int(row.get("rows", 0)) for row in value.values())
            )
            return value

        def counted_provider(connection):
            value = original_provider(connection)
            counters["provider_rows"].append(
                sum(int(row.get("rows", 0)) for row in value.values())
            )
            return value

        def counted_source_groups(connection, content_ids):
            value = original_source_groups(connection, content_ids)
            counters["source_rows"].append(int(value["rows"]))
            return value

        def counted_target_rows(connection, content_ids):
            value = original_target_rows(connection, content_ids)
            if len(content_ids) > 1:
                counters["bulk_target_calls"] += 1
                counters["bulk_target_ids"] += len(content_ids)
                counters["bulk_target_rows"] += sum(
                    len(rows) for rows in value.values()
                )
            return value

        original_item_ownership = batches._item_top_level_ownership_rows

        def counted_item_ownership(*args, **kwargs):
            counters["ownership_item_builds"] += 1
            return original_item_ownership(*args, **kwargs)

        with (
            patch.object(batches, "_read_json", side_effect=counted_read),
            patch.object(
                batches, "_protected_snapshot", side_effect=counted_protected
            ),
            patch.object(
                batches, "_provider_snapshot", side_effect=counted_provider
            ),
            patch.object(
                batches,
                "_source_groups_snapshot",
                side_effect=counted_source_groups,
            ),
            patch.object(local, "_target_rows", side_effect=counted_target_rows),
            patch.object(
                batches,
                "_item_top_level_ownership_rows",
                side_effect=counted_item_ownership,
            ),
            self._pipeline_patches(),
        ):
            result = batches.run_batches(
                **self._batch_arguments(
                    through_batch=126,
                    max_new_batches=25,
                )
            )

        self.performance_counters = {
            **counters,
            "history_total_bytes": history_bytes,
            "history_read_ratio": counters["history_bytes"] / history_bytes,
            "resume_guard_output_hashed_files": result["resume_guard"][
                "output_hashed_files"
            ],
            "resume_guard_output_hashed_bytes": result["resume_guard"][
                "output_hashed_bytes"
            ],
        }
        self.assertEqual(result["processed_this_invocation"], 25)
        self.assertLessEqual(counters["history_bytes"], 6 * history_bytes)
        self.assertEqual(len(counters["protected_rows"]), 2)
        self.assertEqual(len(counters["provider_rows"]), 2)
        self.assertEqual(counters["source_rows"], [128, 128])
        self.assertLessEqual(counters["bulk_target_calls"], 2)
        self.assertEqual(counters["bulk_target_ids"], 128)
        self.assertGreater(counters["bulk_target_rows"], 0)
        self.assertLessEqual(counters["ownership_item_builds"], 128)

        paths = batches._paths(
            source_db_path=self.source_db,
            source_completion_path=self.source_completion,
            db_path=self.db,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        contract = batches._read_json(
            paths.contract, label="completion complexity contract"
        )
        runtime = batches._runtime_context(paths, contract)
        batch_values = batches._validate_batch_chain(paths, runtime)
        receipt_values = batches._validate_item_chain(
            paths, contract, runtime
        )
        completion_values = batches._validate_completion_chain(paths, runtime)
        batches._validate_resume_guard_history(
            paths,
            contract,
            receipt_values,
            batch_values,
            runtime,
            completions=completion_values,
        )

        class NoSliceProbe:
            def __init__(self, values):
                self.values = values
                self.index_reads = 0

            def __len__(self):
                return len(self.values)

            def __getitem__(self, index):
                if isinstance(index, slice):
                    raise AssertionError("completion history不得复制prefix slice")
                self.index_reads += 1
                return self.values[index]

        batch_probe = NoSliceProbe(batch_values)
        receipt_probe = NoSliceProbe(receipt_values)
        batches._validate_completion_history_exact(
            paths,
            contract,
            batch_receipts=batch_probe,
            receipts=receipt_probe,
            completions=completion_values,
            runtime=runtime,
        )
        self.assertEqual(batch_probe.index_reads, len(completion_values))
        self.assertEqual(receipt_probe.index_reads, len(receipt_values))
        self.performance_counters.update(
            {
                "completion_batch_index_reads": batch_probe.index_reads,
                "completion_receipt_index_reads": receipt_probe.index_reads,
                "completion_prefix_slice_attempts": 0,
                "logical_global_heads_calls": logical_heads.call_count,
                "batch_at_cursor_calls": batch_cursor.call_count,
                "initial_logical_checkpoint_calls": sum(
                    call.kwargs.get("completed_count") == 0
                    for call in logical_checkpoints.call_args_list
                ),
                **complexity,
            }
        )
        self.assertLessEqual(logical_heads.call_count, 7)
        self.assertLessEqual(batch_cursor.call_count, 20)
        self.assertLessEqual(
            sum(
                call.kwargs.get("completed_count") == 0
                for call in logical_checkpoints.call_args_list
            ),
            7,
        )
        self.assertLessEqual(
            complexity["source_completion_canonical_calls"], 16
        )
        self.assertLessEqual(complexity["eligible_baseline_map_calls"], 7)
        self.assertLessEqual(complexity["eligible_baseline_rows"], 7 * 128)


@unittest.skipUnless(
    Path("/private/tmp/dcar-step3-canary-v2.xVXN0Y/clone.sqlite3").is_file()
    and Path(
        "/private/tmp/dcar-step3-canary-v2.xVXN0Y/run/completion.json"
    ).is_file(),
    "frozen real Step3 proof is not present",
)
class FullLocalAnalysisRealStep3PlanTest(unittest.TestCase):
    def test_real_51749_default_plan_is_zero_write_and_exactly_classified(self) -> None:
        source_db = Path(
            "/private/tmp/dcar-step3-canary-v2.xVXN0Y/clone.sqlite3"
        )
        source_completion = Path(
            "/private/tmp/dcar-step3-canary-v2.xVXN0Y/run/completion.json"
        )
        with tempfile.TemporaryDirectory() as value:
            root = Path(value).resolve()
            before = list(root.iterdir())
            result = batches.plan_batches(
                source_db_path=source_db,
                source_completion_path=source_completion,
                expected_source_db_sha256=(
                    "8e2d7ca81b241918bb9e3d8fc7eae25320dc8d83dbfd9387ed9e10b0c197e183"
                ),
                expected_source_completion_sha256=(
                    "6306617ca2ba597d78c8f4fb97755f05fbe5c60f7cd2ab49cc8de43d20f52af3"
                ),
                db_path=root / "work.sqlite3",
                media_root=root / "media",
                run_root=root / "run",
                through_batch=1,
            )
            self.assertEqual(result["universe_count"], 51_749)
            self.assertEqual(result["eligible_count"], 17_147)
            self.assertEqual(result["static_deferred_count"], 34_602)
            self.assertEqual(result["missing_universe"]["missing_media_urls"], 39)
            self.assertEqual(
                result["batch_action"]["content_ids"], [15809, 15810, 17182]
            )
            self.assertEqual(list(root.iterdir()), before)
