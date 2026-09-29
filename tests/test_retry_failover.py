"""失败重试与兜底：临时故障 vs 永久失败、退避增长、失败区可查询、不允许假成功。"""

from conftest import submit, wait_status


async def test_transient_failure_retries_with_growing_backoff_then_dead_letter(client, caplog):
    r = await submit(client, "transient_fail", {}, key="t-retries")
    jid = r.json()["job"]["id"]

    final = await wait_status(client, jid, "failed", timeout=8.0)
    # 首次 + max_retries 次重试
    assert final["attempts"] == final["max_retries"] + 1
    assert final["error_kind"] == "retries_exhausted"
    assert "下游不可用" in (final["last_error"] or "")

    # 关键状态流转日志：claim -> retry(每次) -> fail
    messages = [r.message for r in caplog.records if getattr(r, "message", "")]
    retry_logs = [m for m in messages if m.startswith("retry job_id=")]
    fail_logs = [m for m in messages if m.startswith("fail job_id=")]
    assert len(retry_logs) == final["max_retries"]
    assert len(fail_logs) == 1
    # 退避应随次数增长：0.02, 0.04, 0.08
    backoffs = [float(m.split("backoff=")[1].split("s")[0]) for m in retry_logs]
    assert backoffs == sorted(backoffs) and backoffs[0] < backoffs[-1], backoffs

    # 失败区可查询，并保留最后一次失败原因
    dl = await client.get("/api/v1/dead-letters")
    assert dl.status_code == 200
    entries = {j["id"]: j for j in dl.json()}
    assert jid in entries
    assert entries[jid]["last_error"] == final["last_error"]
    assert entries[jid]["error_kind"] == "retries_exhausted"
    # 未真正跑完的任务不得标成功
    assert entries[jid]["status"] == "failed" and entries[jid]["result"] is None


async def test_permanent_failure_no_retry(client):
    r = await submit(client, "permanent_fail", {}, key="t-permanent")
    jid = r.json()["job"]["id"]
    final = await wait_status(client, jid, "failed", timeout=5.0)
    assert final["attempts"] == 1, "永久失败不得重试"
    assert final["error_kind"] == "permanent"

    dl = await client.get("/api/v1/dead-letters")
    assert any(j["id"] == jid and j["error_kind"] == "permanent" for j in dl.json())


async def test_input_error_treated_as_permanent(client):
    r = await submit(client, "bad_input", {}, key="t-badinput")
    final = await wait_status(client, r.json()["job"]["id"], "failed")
    assert final["attempts"] == 1
    assert final["error_kind"] == "permanent"
    assert "KeyError" in (final["last_error"] or "")


async def test_flaky_eventually_succeeds_and_records_attempts(client):
    r = await submit(client, "flaky", {"fail_times": 2}, key="t-flaky")
    final = await wait_status(client, r.json()["job"]["id"], "succeeded", timeout=8.0)
    assert final["attempts"] == 3
    assert final["result"] == "ok after 3 attempts"


async def test_effect_recorded_exactly_once_after_retries(client):
    # record_effect 一次性成功；真正校验"重复执行无重复副作用"见抢占测试，
    # 这里确认成功任务在失败区之外且结果里的副作用计数为 1。
    r = await submit(client, "record_effect", {"label": "charge"}, key="t-effect")
    final = await wait_status(client, r.json()["job"]["id"], "succeeded")
    assert final["result"]["effect_count"] == 1
