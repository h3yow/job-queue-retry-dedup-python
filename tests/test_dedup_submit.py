"""重复提交去重 + 非法/超限提交拒绝。"""


from conftest import submit, wait_status


async def test_duplicate_idempotency_key_returns_same_job(client):
    r1 = await submit(client, "echo", {"order": "A-1001"}, key="order-A-1001")
    assert r1.status_code == 201, r1.text
    body1 = r1.json()
    assert body1["deduplicated"] is False

    r2 = await submit(client, "echo", {"order": "A-1001"}, key="order-A-1001")
    assert r2.status_code == 201
    body2 = r2.json()
    assert body2["deduplicated"] is True
    assert body2["job"]["id"] == body1["job"]["id"], "重复请求必须返回同一任务"

    # 不同幂等键 => 不同任务
    r3 = await submit(client, "echo", {"order": "A-1001"}, key="order-A-1002")
    assert r3.json()["job"]["id"] != body1["job"]["id"]

    # 等待首个任务成功，确认只执行了一次
    done = await wait_status(client, body1["job"]["id"], "succeeded")
    assert done["result"]["echoed"] == {"order": "A-1001"}


async def test_idempotency_key_with_different_content_conflicts(client):
    r1 = await submit(client, "echo", {"a": 1}, key="k-conflict")
    assert r1.status_code == 201
    r2 = await submit(client, "flaky", {"a": 1}, key="k-conflict")
    assert r2.status_code == 409
    assert r2.json()["detail"]["error_code"] == "idempotency_key_conflict"


async def test_unknown_type_rejected(client):
    r = await submit(client, "does_not_exist", {})
    assert r.status_code == 400
    assert r.json()["detail"]["error_code"] == "unknown_job_type"


async def test_invalid_body_rejected(client):
    # 缺 type 字段
    r = await client.post("/api/v1/jobs", json={"payload": {}})
    assert r.status_code == 422
    assert r.json()["detail"]["error_code"] == "invalid_submission"
    # 非法 JSON
    r = await client.post(
        "/api/v1/jobs", content=b"{not-json", headers={"Content-Type": "application/json"}
    )
    assert r.status_code == 422
    assert r.json()["detail"]["error_code"] == "invalid_submission"
    # 空 body / 类型错误
    r = await client.post("/api/v1/jobs", content=b"", headers={"Content-Type": "application/json"})
    assert r.status_code == 422
    assert r.json()["detail"]["error_code"] == "invalid_submission"
    r = await submit(client, 123, {})
    assert r.status_code == 422
    assert r.json()["detail"]["error_code"] == "invalid_submission"


async def test_payload_too_large_rejected(client):
    big = {"blob": "x" * 3000}  # 上限 2048
    r = await submit(client, "echo", big)
    assert r.status_code == 413
    assert r.json()["detail"]["error_code"] == "payload_too_large"


async def test_no_idempotency_key_creates_distinct_jobs(client):
    r1 = await submit(client, "echo", {"x": 1})
    r2 = await submit(client, "echo", {"x": 1})
    assert r1.json()["job"]["id"] != r2.json()["job"]["id"]
