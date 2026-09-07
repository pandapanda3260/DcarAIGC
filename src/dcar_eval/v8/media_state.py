"""Read-only terminal-state projection for the current media evidence DAG."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from .media import (
    IMAGE_DOWNLOAD_VERSION,
    MAX_MEDIA_DOWNLOAD_ATTEMPTS,
    MAX_MEDIA_PROCESSING_ATTEMPTS,
    MEDIA_SOURCE_VERSION,
    VIDEO_DOWNLOAD_VERSION,
    managed_evidence_metadata,
    managed_slot_source,
    processor_versions,
)


_QUERY_CHUNK_SIZE = 400


MediaTerminalState = Literal[
    "complete",
    "terminal_insufficient",
    "terminal_failed",
    "pending",
]
MediaTerminalReason = Literal[
    "complete",
    "terminal_insufficient",
    "source_missing",
    "download_pending",
    "frames_pending",
    "asr_pending",
    "ocr_pending",
    "evaluation_pending",
    "download_terminal_failed",
    "frames_terminal_failed",
    "asr_terminal_failed",
    "ocr_terminal_failed",
    "managed_source_pending",
    "restore_required",
    "expired_non_replayable",
    "original_unavailable",
]


class MediaStateError(RuntimeError):
    """Raised when the requested release or content identity is invalid."""


@dataclass(frozen=True)
class MediaTerminalDetail:
    """One derived state plus the exact stage that determined it."""

    state: MediaTerminalState
    reason: MediaTerminalReason


@dataclass(frozen=True)
class _ArtifactClosure:
    artifact_id: int
    sha256: str


def _id_chunks(values: Sequence[int]) -> list[list[int]]:
    return [
        list(values[offset : offset + _QUERY_CHUNK_SIZE])
        for offset in range(0, len(values), _QUERY_CHUNK_SIZE)
    ]


def _valid_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _metadata_object(value: Any) -> dict[str, Any] | None:
    if type(value) is not str:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _slot_artifact(
    row: sqlite3.Row | None,
    *,
    content_id: int,
    artifact_type: str,
) -> tuple[str, _ArtifactClosure | None]:
    if row is None:
        return "pending", None
    status = str(row["status"] or "")
    attempt_limit = (
        MAX_MEDIA_DOWNLOAD_ATTEMPTS
        if row["processor_type"] == "download"
        else MAX_MEDIA_PROCESSING_ATTEMPTS
    )
    if status == "terminal_failed" or (
        status == "retryable_failed"
        and type(row["attempt_count"]) is int
        and int(row["attempt_count"]) >= attempt_limit
    ):
        return "terminal_failed", None
    if status != "succeeded":
        return "pending", None
    if (
        type(row["attempt_count"]) is not int
        or int(row["attempt_count"]) <= 0
        or type(row["output_artifact_id"]) is not int
        or row["artifact_id"] != row["output_artifact_id"]
        or row["artifact_content_id"] != content_id
        or row["artifact_type"] != artifact_type
        or row["artifact_status"] != "available"
        or row["artifact_processor_version"] != row["processor_version"]
        or type(row["artifact_local_path"]) is not str
        or not str(row["artifact_local_path"])
        or type(row["artifact_byte_size"]) is not int
        or int(row["artifact_byte_size"]) <= 0
        or not _valid_sha256(row["artifact_sha256"])
        or _metadata_object(row["artifact_metadata_json"]) is None
    ):
        return "pending", None
    return "succeeded", _ArtifactClosure(
        artifact_id=int(row["artifact_id"]),
        sha256=str(row["artifact_sha256"]),
    )


def _douyin_image_download_identity(
    rows: Sequence[sqlite3.Row],
    *,
    flat_source_sha256: str,
    content_id: int,
) -> tuple[str, str] | None:
    """Recover a proven Douyin image binding from a closed current download.

    Douyin grouping comes from frozen discovery candidates and is not stored on
    the media-source artifact.  A failed slot therefore cannot be attributed to
    the latest source without guessing.  A succeeded manifest does carry both
    hashes, so that exact closure remains classifiable.
    """

    candidates: list[tuple[int, str, str]] = []
    for row in rows:
        if (
            row["processor_type"] != "download"
            or row["processor_version"] != IMAGE_DOWNLOAD_VERSION
        ):
            continue
        outcome, _artifact = _slot_artifact(
            row, content_id=content_id, artifact_type="media_manifest"
        )
        if outcome != "succeeded":
            continue
        metadata = _metadata_object(row["artifact_metadata_json"])
        if (
            metadata is None
            or metadata.get("source_sha256") != flat_source_sha256
            or metadata.get("download_binding_sha256") != row["source_sha256"]
            or not _valid_sha256(row["source_sha256"])
        ):
            continue
        candidates.append(
            (
                int(row["id"]),
                str(row["source_sha256"]),
                str(row["processor_version"]),
            )
        )
    if not candidates:
        return None
    _slot_id, source_sha256, processor_version = max(candidates)
    return source_sha256, processor_version


def _envelope_matches(
    evaluation: sqlite3.Row | None,
    *,
    content_id: int,
    media_sha256: str,
    asr_sha256: str | None,
    ocr_sha256: str,
) -> bool:
    if evaluation is None:
        return False
    components = _metadata_object(evaluation["components_json"])
    return bool(
        type(evaluation["evidence_envelope_id"]) is int
        and evaluation["envelope_id"] == evaluation["evidence_envelope_id"]
        and evaluation["envelope_content_id"] == content_id
        and evaluation["evaluation_evidence_sha256"]
        == evaluation["envelope_evidence_sha256"]
        and evaluation["media_sha256"] == media_sha256
        and evaluation["asr_sha256"] == asr_sha256
        and evaluation["ocr_sha256"] == ocr_sha256
        and components is not None
        and components.get("media_sha256") == media_sha256
        and components.get("asr_sha256") == asr_sha256
        and components.get("ocr_sha256") == ocr_sha256
    )


def _complete_evaluation_envelope_valid(
    evaluation: sqlite3.Row | None,
    *,
    content_id: int,
    content_type: str,
) -> bool:
    """Validate the immutable media hashes already sealed by a V2/V3 result."""

    if (
        evaluation is None
        or evaluation["evaluation_status"] != "evaluated"
        or evaluation["evidence_level"] not in {"V2", "V3"}
    ):
        return False
    components = _metadata_object(evaluation["components_json"])
    media_sha256 = evaluation["media_sha256"]
    asr_sha256 = evaluation["asr_sha256"]
    ocr_sha256 = evaluation["ocr_sha256"]
    return bool(
        type(evaluation["evidence_envelope_id"]) is int
        and evaluation["envelope_id"] == evaluation["evidence_envelope_id"]
        and evaluation["envelope_content_id"] == content_id
        and _valid_sha256(evaluation["evaluation_evidence_sha256"])
        and evaluation["evaluation_evidence_sha256"]
        == evaluation["envelope_evidence_sha256"]
        and _valid_sha256(media_sha256)
        and _valid_sha256(ocr_sha256)
        and (
            _valid_sha256(asr_sha256)
            if content_type == "video"
            else asr_sha256 is None
        )
        and components is not None
        and components.get("media_sha256") == media_sha256
        and components.get("asr_sha256") == asr_sha256
        and components.get("ocr_sha256") == ocr_sha256
    )


def _unavailable_evaluation_envelope_valid(
    evaluation: sqlite3.Row | None,
    *,
    content_id: int,
) -> bool:
    """Validate a weak automatic envelope with no invented media evidence."""

    if (
        evaluation is None
        or evaluation["evaluation_status"] != "insufficient_evidence"
        or evaluation["evidence_level"] not in {"V0", "V1"}
        or evaluation["evaluation_source"] != "automatic"
    ):
        return False
    components = _metadata_object(evaluation["components_json"])
    return bool(
        type(evaluation["evidence_envelope_id"]) is int
        and evaluation["envelope_id"] == evaluation["evidence_envelope_id"]
        and evaluation["envelope_content_id"] == content_id
        and _valid_sha256(evaluation["evaluation_evidence_sha256"])
        and evaluation["evaluation_evidence_sha256"]
        == evaluation["envelope_evidence_sha256"]
        and evaluation["media_sha256"] is None
        and evaluation["asr_sha256"] is None
        and evaluation["ocr_sha256"] is None
        and components is not None
        and components.get("media_sha256") is None
        and components.get("asr_sha256") is None
        and components.get("ocr_sha256") is None
    )


def provider_terminal_unavailable_content_ids(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
) -> set[int]:
    """Return exact provider-confirmed permanent media-source misses."""

    result: set[int] = set()
    for chunk in _id_chunks(list(dict.fromkeys(content_ids))):
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        result.update(
            int(row["content_id"])
            for row in connection.execute(
                f"""
                SELECT DISTINCT content_id
                FROM fetch_slots
                WHERE content_id IN ({placeholders})
                  AND stage IN ('detail','media_source_refresh')
                  AND window_key IN ('lifetime','xhs-type-probe-v1')
                  AND status='terminal_failed'
                  AND last_error_code='content_unavailable'
                """,
                chunk,
            ).fetchall()
        )
    return result


def _managed_terminal_detail(
    connection: sqlite3.Connection, *, content_id: int, content_type: str,
    bundle: Mapping[str, Any], evaluation: sqlite3.Row | None,
    slots: Mapping[tuple[int, str, str, str], sqlite3.Row],
    slots_by_id: Mapping[int, sqlite3.Row], versions: Mapping[str, str],
) -> MediaTerminalDetail:
    from .media_lifecycle import original_artifact
    from .media_retention import original_availability

    original = original_artifact(connection, bundle)

    def retained_hash_bound(artifact_type: str, sha256: str | None) -> bool:
        if sha256 is None:
            return False
        for row in connection.execute(
            """SELECT * FROM evidence_artifacts WHERE content_id=?
               AND artifact_type=? AND sha256=? AND status='available'""",
            (content_id, artifact_type, sha256),
        ):
            metadata = _metadata_object(row["metadata_json"])
            if metadata is None:
                continue
            namespace = metadata.get("media_lifecycle")
            expected = managed_evidence_metadata(
                bundle, source_sha256=str((namespace or {}).get("input_sha256") or ""),
                processor_version=str(row["processor_version"]),
            )["media_lifecycle"] if isinstance(namespace, dict) else None
            if expected is not None and namespace == expected:
                return True
        return False

    if (
        _complete_evaluation_envelope_valid(
            evaluation, content_id=content_id, content_type=content_type
        )
        and evaluation is not None
        and evaluation["media_sha256"] == original["sha256"]
        and retained_hash_bound("ocr", evaluation["ocr_sha256"])
        and (content_type != "video" or retained_hash_bound("asr", evaluation["asr_sha256"]))
    ):
        return MediaTerminalDetail("complete", "complete")

    availability = original_availability(connection, content_id)
    reason = availability["reason"]
    if reason in {"original_archived", "original_restoring"}:
        return MediaTerminalDetail("pending", "restore_required")
    if reason in {"original_expired", "original_expiry_pending", "original_purge_in_progress"}:
        return MediaTerminalDetail("pending", "expired_non_replayable")
    if reason != "original_available":
        return MediaTerminalDetail("pending", "original_unavailable")
    download = slots_by_id.get(int(bundle["manifest"]["download_slot"]["id"]))
    outcome, media_artifact = _slot_artifact(
        download, content_id=content_id,
        artifact_type="media" if content_type == "video" else "media_manifest",
    )
    if (outcome != "succeeded" or media_artifact is None
            or media_artifact.artifact_id != original["id"]
            or media_artifact.sha256 != original["sha256"]):
        return MediaTerminalDetail("pending", "download_pending")

    def stage(
        processor: str, input_sha256: str, artifact_type: str,
    ) -> tuple[str, _ArtifactClosure | None]:
        row = slots.get((content_id, managed_slot_source(bundle, input_sha256),
                         processor, versions[processor]))
        result, artifact = _slot_artifact(row, content_id=content_id, artifact_type=artifact_type)
        if result == "succeeded" and row is not None:
            metadata = _metadata_object(row["artifact_metadata_json"])
            if metadata != managed_evidence_metadata(
                bundle, source_sha256=input_sha256, processor_version=versions[processor]
            ):
                return "pending", None
        return result, artifact

    def waiting(processor: str, outcome: str) -> MediaTerminalDetail:
        reasons: dict[str, tuple[MediaTerminalReason, MediaTerminalReason]] = {
            "frames": ("frames_pending", "frames_terminal_failed"),
            "asr": ("asr_pending", "asr_terminal_failed"),
            "ocr": ("ocr_pending", "ocr_terminal_failed"),
        }
        pending, failed = reasons[processor]
        return MediaTerminalDetail(
            "terminal_failed" if outcome == "terminal_failed" else "pending",
            failed if outcome == "terminal_failed" else pending,
        )

    asr_sha256 = None
    ocr_input = media_artifact.sha256
    if content_type == "video":
        for processor, artifact_type in (("frames", "frames_manifest"), ("asr", "asr")):
            outcome, artifact = stage(processor, media_artifact.sha256, artifact_type)
            if outcome != "succeeded" or artifact is None:
                return waiting(processor, outcome)
            if processor == "frames":
                ocr_input = artifact.sha256
            else:
                asr_sha256 = artifact.sha256
    outcome, ocr = stage("ocr", ocr_input, "ocr")
    if outcome != "succeeded" or ocr is None:
        return waiting("ocr", outcome)
    if not _envelope_matches(
        evaluation, content_id=content_id, media_sha256=media_artifact.sha256,
        asr_sha256=asr_sha256, ocr_sha256=ocr.sha256,
    ):
        return MediaTerminalDetail("pending", "evaluation_pending")
    if evaluation is not None and evaluation["evaluation_status"] == "insufficient_evidence":
        return MediaTerminalDetail("terminal_insufficient", "terminal_insufficient")
    return MediaTerminalDetail("pending", "evaluation_pending")


def media_terminal_state_details(
    connection: sqlite3.Connection,
    release_id: str,
    content_ids: Sequence[int],
    *, cutoff_at: str | None = None,
) -> dict[int, MediaTerminalDetail]:
    """Project each content onto its current media DAG state and stage reason.

    A structurally valid latest automatic V2/V3 envelope is already immutable
    evidence and is complete without replaying today's media slots.  Weak V0/V1
    results become terminal-insufficient only after the current source and every
    required current processor slot close over exact available output artifacts.
    """

    if type(release_id) is not str or not release_id:
        raise MediaStateError("release_id must be a non-empty string")
    if connection.execute(
        "SELECT 1 FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone() is None:
        raise MediaStateError(f"evaluation release does not exist: {release_id}")

    ids: list[int] = []
    seen: set[int] = set()
    for value in content_ids:
        if type(value) is not int or value <= 0:
            raise MediaStateError("content_ids must contain positive integers")
        if value not in seen:
            ids.append(value)
            seen.add(value)
    if not ids:
        return {}

    # Six cutoff/release binds are added to the largest query; respect the
    # connection's actual SQLite limit (including small regression fixtures).
    chunk_size = min(_QUERY_CHUNK_SIZE, connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) - 6)
    if chunk_size < 1:
        raise MediaStateError("SQLite variable limit is too small for media state")
    chunks = [ids[offset:offset + chunk_size] for offset in range(0, len(ids), chunk_size)]
    contents: dict[int, sqlite3.Row] = {}
    for chunk in chunks:
        placeholders = ",".join("?" for _ in chunk)
        contents.update(
            {
                int(row["id"]): row
                for row in connection.execute(
                    f"""
                    SELECT id,platform,content_type
                    FROM content_items
                    WHERE id IN ({placeholders})
                    """,
                    chunk,
                ).fetchall()
            }
        )
    missing_ids = [content_id for content_id in ids if content_id not in contents]
    if missing_ids:
        raise MediaStateError(f"content items do not exist: {missing_ids}")
    slot_rows: list[sqlite3.Row] = []
    for chunk in chunks:
        placeholders = ",".join("?" for _ in chunk)
        slot_rows.extend(
            connection.execute(
                f"""
                SELECT m.*,
                       e.id AS artifact_id,
                       e.content_id AS artifact_content_id,
                       e.artifact_type AS artifact_type,
                       e.status AS artifact_status,
                       e.local_path AS artifact_local_path,
                       e.byte_size AS artifact_byte_size,
                       e.sha256 AS artifact_sha256,
                       e.processor_version AS artifact_processor_version,
                       e.metadata_json AS artifact_metadata_json
                FROM media_processing_slots m
                LEFT JOIN evidence_artifacts e ON e.id=m.output_artifact_id
                WHERE m.content_id IN ({placeholders})
                  AND (? IS NULL OR (julianday(m.updated_at)<=julianday(?)
                       AND (e.id IS NULL OR (julianday(e.created_at)<=julianday(?) AND julianday(e.captured_at)<=julianday(?)))))
                ORDER BY m.id
                """,
                [*chunk, cutoff_at, cutoff_at, cutoff_at, cutoff_at],
            ).fetchall()
        )
    slots_by_content: dict[int, list[sqlite3.Row]] = {value: [] for value in ids}
    slots_by_id: dict[int, sqlite3.Row] = {}
    slots: dict[tuple[int, str, str, str], sqlite3.Row] = {}
    for row in slot_rows:
        content_id = int(row["content_id"])
        slots_by_content.setdefault(content_id, []).append(row)
        slots_by_id[int(row["id"])] = row
        slots[
            (
                content_id,
                str(row["source_sha256"]),
                str(row["processor_type"]),
                str(row["processor_version"]),
            )
        ] = row

    latest_sources: dict[int, sqlite3.Row] = {}
    for chunk in chunks:
        placeholders = ",".join("?" for _ in chunk)
        for row in connection.execute(
            f"""
            SELECT * FROM (
                SELECT id,content_id,sha256,processor_version,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_id ORDER BY id DESC
                       ) AS selector_rank
                FROM evidence_artifacts
                WHERE artifact_type='media_source' AND status='available'
                  AND content_id IN ({placeholders})
                  AND (? IS NULL OR (julianday(created_at)<=julianday(?) AND julianday(captured_at)<=julianday(?)))
            ) WHERE selector_rank=1
            """,
            [*chunk, cutoff_at, cutoff_at, cutoff_at],
        ).fetchall():
            latest_sources[int(row["content_id"])] = row

    evaluations: dict[int, sqlite3.Row] = {}
    for chunk in chunks:
        placeholders = ",".join("?" for _ in chunk)
        evaluations.update(
            {
                int(row["content_id"]): row
                for row in connection.execute(
                    f"""
                    SELECT * FROM (
                        SELECT ev.content_id,
                               ev.evidence_envelope_id,
                               ev.evidence_sha256 AS evaluation_evidence_sha256,
                               ev.evaluation_status,
                               ev.evidence_level,
                               ev.evaluation_source,
                               envelope.id AS envelope_id,
                               envelope.content_id AS envelope_content_id,
                               envelope.evidence_sha256
                                   AS envelope_evidence_sha256,
                               envelope.media_sha256,
                               envelope.asr_sha256,
                               envelope.ocr_sha256,
                               envelope.components_json,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ev.content_id
                                   ORDER BY ev.evaluated_at DESC,ev.id DESC
                               ) AS selector_rank
                        FROM evaluation_versions ev
                        LEFT JOIN evidence_envelopes envelope
                          ON envelope.id=ev.evidence_envelope_id
                        WHERE ev.release_id=?
                          AND (ev.invalidated_at IS NULL OR (? IS NOT NULL AND julianday(ev.invalidated_at)>julianday(?)))
                          AND ev.evaluation_source='automatic'
                          AND ev.content_id IN ({placeholders})
                          AND (? IS NULL OR (julianday(ev.evaluated_at)<=julianday(?) AND julianday(envelope.created_at)<=julianday(?)))
                    ) WHERE selector_rank=1
                    """,
                    [release_id, cutoff_at, cutoff_at, *chunk, cutoff_at, cutoff_at, cutoff_at],
                ).fetchall()
            }
        )
    versions = processor_versions()
    provider_unavailable = provider_terminal_unavailable_content_ids(
        connection, ids
    )
    result: dict[int, MediaTerminalDetail] = {
        content_id: MediaTerminalDetail("pending", "source_missing")
        for content_id in ids
    }

    def slot_for(
        content_id: int,
        source_sha256: str,
        processor_type: str,
        processor_version: str,
    ) -> sqlite3.Row | None:
        return slots.get(
            (content_id, source_sha256, processor_type, processor_version)
        )

    for content_id in ids:
        content = contents.get(content_id)
        if content is None:
            continue
        evaluation = evaluations.get(content_id)
        if content["content_type"] not in {"video", "image"}:
            if (
                content_id in provider_unavailable
                and _unavailable_evaluation_envelope_valid(
                    evaluation, content_id=content_id
                )
            ):
                result[content_id] = MediaTerminalDetail(
                    "terminal_insufficient", "terminal_insufficient"
                )
            continue
        from .media import _has_managed_history
        from .media_lifecycle import current_bundle

        bundle = current_bundle(connection, content_id)
        if bundle is not None:
            if cutoff_at is not None and (latest_sources.get(content_id) is None or
                    bundle["manifest"]["source"]["artifact_id"] != latest_sources[content_id]["id"]):
                # A later source cannot retroactively close an old report's
                # media gate. No temporal bundle state is invented here.
                result[content_id] = MediaTerminalDetail("pending", "managed_source_pending")
                continue
            result[content_id] = _managed_terminal_detail(
                connection, content_id=content_id, content_type=str(content["content_type"]),
                bundle=bundle, evaluation=evaluation, slots=slots,
                slots_by_id=slots_by_id, versions=versions,
            )
            continue
        if _has_managed_history(connection, content_id):
            result[content_id] = MediaTerminalDetail("pending", "managed_source_pending")
            continue
        if _complete_evaluation_envelope_valid(
            evaluation,
            content_id=content_id,
            content_type=str(content["content_type"]),
        ):
            result[content_id] = MediaTerminalDetail("complete", "complete")
            continue
        source_row = latest_sources.get(content_id)
        if (
            source_row is None
            or source_row["processor_version"] != MEDIA_SOURCE_VERSION
            or not _valid_sha256(source_row["sha256"])
        ):
            if (
                content_id in provider_unavailable
                and _unavailable_evaluation_envelope_valid(
                    evaluation, content_id=content_id
                )
            ):
                result[content_id] = MediaTerminalDetail(
                    "terminal_insufficient", "terminal_insufficient"
                )
            continue
        download_row = slot_for(
            content_id,
            str(source_row["sha256"]),
            "download",
            (
                VIDEO_DOWNLOAD_VERSION
                if content["content_type"] == "video"
                else IMAGE_DOWNLOAD_VERSION
            ),
        )
        download_outcome, media_artifact = _slot_artifact(
            download_row,
            content_id=content_id,
            artifact_type=(
                "media" if content["content_type"] == "video" else "media_manifest"
            ),
        )
        if download_outcome == "terminal_failed":
            result[content_id] = MediaTerminalDetail(
                "terminal_failed", "download_terminal_failed"
            )
            continue
        if download_outcome != "succeeded" or media_artifact is None:
            result[content_id] = MediaTerminalDetail("pending", "download_pending")
            continue

        asr_sha256: str | None = None
        if content["content_type"] == "video":
            frames_outcome, frames_artifact = _slot_artifact(
                slot_for(
                    content_id,
                    media_artifact.sha256,
                    "frames",
                    versions["frames"],
                ),
                content_id=content_id,
                artifact_type="frames_manifest",
            )
            asr_outcome, asr_artifact = _slot_artifact(
                slot_for(
                    content_id,
                    media_artifact.sha256,
                    "asr",
                    versions["asr"],
                ),
                content_id=content_id,
                artifact_type="asr",
            )
            if frames_outcome == "terminal_failed":
                result[content_id] = MediaTerminalDetail(
                    "terminal_failed", "frames_terminal_failed"
                )
                continue
            if asr_outcome == "terminal_failed":
                result[content_id] = MediaTerminalDetail(
                    "terminal_failed", "asr_terminal_failed"
                )
                continue
            if frames_outcome != "succeeded" or frames_artifact is None:
                result[content_id] = MediaTerminalDetail("pending", "frames_pending")
                continue
            ocr_outcome, ocr_artifact = _slot_artifact(
                slot_for(
                    content_id,
                    frames_artifact.sha256,
                    "ocr",
                    versions["ocr"],
                ),
                content_id=content_id,
                artifact_type="ocr",
            )
            if ocr_outcome == "terminal_failed":
                result[content_id] = MediaTerminalDetail(
                    "terminal_failed", "ocr_terminal_failed"
                )
                continue
            if asr_outcome != "succeeded" or asr_artifact is None:
                result[content_id] = MediaTerminalDetail("pending", "asr_pending")
                continue
            if ocr_outcome != "succeeded" or ocr_artifact is None:
                result[content_id] = MediaTerminalDetail("pending", "ocr_pending")
                continue
            asr_sha256 = asr_artifact.sha256
        else:
            ocr_outcome, ocr_artifact = _slot_artifact(
                slot_for(
                    content_id,
                    media_artifact.sha256,
                    "ocr",
                    versions["ocr"],
                ),
                content_id=content_id,
                artifact_type="ocr",
            )
            if ocr_outcome == "terminal_failed":
                result[content_id] = MediaTerminalDetail(
                    "terminal_failed", "ocr_terminal_failed"
                )
                continue
            if ocr_outcome != "succeeded" or ocr_artifact is None:
                result[content_id] = MediaTerminalDetail("pending", "ocr_pending")
                continue

        if evaluation is None or not _envelope_matches(
            evaluation,
            content_id=content_id,
            media_sha256=media_artifact.sha256,
            asr_sha256=asr_sha256,
            ocr_sha256=ocr_artifact.sha256,
        ):
            result[content_id] = MediaTerminalDetail(
                "pending", "evaluation_pending"
            )
            continue
        evaluation_status = str(evaluation["evaluation_status"])
        evidence_level = str(evaluation["evidence_level"])
        if evaluation_status == "evaluated" and evidence_level in {"V2", "V3"}:
            result[content_id] = MediaTerminalDetail("complete", "complete")
        elif (
            evaluation_status == "insufficient_evidence"
            and evidence_level in {"V0", "V1"}
        ):
            result[content_id] = MediaTerminalDetail(
                "terminal_insufficient", "terminal_insufficient"
            )
        else:
            result[content_id] = MediaTerminalDetail(
                "pending", "evaluation_pending"
            )
    return result


def media_terminal_states(
    connection: sqlite3.Connection,
    release_id: str,
    content_ids: Sequence[int],
    *, cutoff_at: str | None = None,
) -> dict[int, MediaTerminalState]:
    """Return the compact state projection used by report coverage."""

    return {
        content_id: detail.state
        for content_id, detail in media_terminal_state_details(
            connection, release_id, content_ids, cutoff_at=cutoff_at
        ).items()
    }
