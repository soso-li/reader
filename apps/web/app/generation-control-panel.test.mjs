import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

import GenerationControlPanel, { canApproveGenerationTask, canReapplyGenerationTask, generationSafetyState, parseGenerationTokenInput } from "./generation-control-panel.tsx";


test("generation safety banner only reflects confirmed server state", () => {
  assert.deepEqual(
    generationSafetyState({
      global_pause: true,
      auto_run: false,
      daily_budget_tokens: null
    }),
    {
      paused: true,
      tone: "paused",
      title: "全局暂停",
      detail: "不会创建新执行尝试，也不会调用模型服务。"
    }
  );
  assert.deepEqual(
    generationSafetyState(null),
    {
      paused: null,
      tone: "unknown",
      title: "控制状态未知",
      detail: "无法确认是否已暂停，请刷新后再操作。"
    }
  );
  assert.deepEqual(
    generationSafetyState({
      global_pause: false,
      auto_run: false,
      daily_budget_tokens: null
    }),
    {
      paused: false,
      tone: "blocked",
      title: "预算未配置",
      detail: "新任务会在创建执行尝试前被阻断。"
    }
  );
  assert.deepEqual(
    generationSafetyState({
      global_pause: false,
      auto_run: true,
      daily_budget_tokens: 1000
    }),
    {
      paused: false,
      tone: "active",
      title: "允许自动任务准入",
      detail: "自动任务仍需预算充足；不会绕过预算检查。"
    }
  );
});

test("generation token inputs reject values that would trigger browser validation", () => {
  assert.equal(parseGenerationTokenInput("", null), null);
  assert.equal(parseGenerationTokenInput("", 0), 0);
  assert.equal(parseGenerationTokenInput("1200", null), 1200);
  assert.equal(parseGenerationTokenInput("-1", null), undefined);
  assert.equal(parseGenerationTokenInput("1.5", null), undefined);
  assert.equal(parseGenerationTokenInput("2147483648", null), undefined);
});


test("generation result cannot expose another approval action", () => {
  assert.equal(
    canApproveGenerationTask({ status: "complete", approval_status: "awaiting", privacy_status: "local", result_uid: "result-1" }),
    false
  );
  assert.equal(
    canApproveGenerationTask({ status: "blocked", approval_status: "awaiting", privacy_status: "blocked", result_uid: null }),
    false
  );
  assert.equal(
    canApproveGenerationTask({ status: "canceled", approval_status: "awaiting", privacy_status: "eligible", result_uid: null }),
    false
  );
  assert.equal(
    canReapplyGenerationTask({ can_reapply: true }),
    true
  );
  assert.equal(
    canReapplyGenerationTask({ can_reapply: false }),
    false
  );
});


test("retention cleanup is visible without exposing purged payload or execution audit", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialRetention: {
        status: "succeeded",
        last_run_at: "2026-07-18T12:00:00Z",
        finished_at: "2026-07-18T12:00:00Z",
        scanned_count: 2,
        deleted_count: 2,
        failure_reason: ""
      },
      initialTasks: [{
        request_uid: "request-purged",
        task_type: "event-synthesis",
        reason: "explicit-user-request",
        target_type: "event",
        target_uid: "event-purged",
        provider: "local",
        model: "gpt-5",
        payload_retention: "purged",
        payload_purged_at: "2026-07-18T12:00:00Z",
        status: "failed",
        privacy_status: "eligible",
        approval_status: "consumed",
        admission_status: "admitted",
        admission_reason: "",
        application_status: "not_started",
        result_uid: null,
        attempts: [{
          attempt_uid: "attempt-purged",
          attempt_no: 1,
          status: "failed",
          input_tokens: 10,
          output_tokens: 2,
          started_at: "2026-06-01T12:00:00Z",
          finished_at: "2026-06-01T12:01:00Z",
          error: "transport failed",
          runner_events_retention: "purged",
          runner_events_purged_at: "2026-07-18T12:00:00Z"
        }],
        retry_count: 0,
        failure_class: "transport",
        cancel_requested: false,
        created_at: "2026-06-01T12:00:00Z",
        error: "transport failed"
      }]
    })
  );

  assert.match(html, /保留清理：已完成/);
  assert.match(html, /07\/18 20:00/);
  assert.match(html, /扫描 2 项 · 清理 2 项/);
  assert.match(html, /请求 Payload.*已按保留策略清理/);
  assert.match(html, /历史执行审计.*已按保留策略清理/);
  assert.match(html, /请求正文已清理，请从原业务重新发起/);
  assert.doesNotMatch(html, />明确重试</);
  assert.doesNotMatch(html, /private_input|must-not-leak/);
});


test("missing generation control cannot be edited or shown as zero usage", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialTasks: []
    })
  );

  assert.match(html, /控制状态未知/);
  assert.match(html, /今日实际<\/dt><dd>未知/);
  assert.match(html, /活跃预留<\/dt><dd>未知/);
  assert.doesNotMatch(html, /今日实际<\/dt><dd>0/);
  assert.match(html, /保存生成控制<\/button>/);
  assert.match(html, /<button type="submit" disabled=""/);
});


test("generation task history exposes pagination after the first hundred", () => {
  const tasks = Array.from({ length: 100 }, (_, index) => ({
    request_uid: `request-${index}`,
    task_type: "event-synthesis",
    reason: "explicit-user-request",
    target_type: "event",
    target_uid: `event-${index}`,
    provider: "local-chat",
    model: "test-model",
    payload_retention: "retained",
    payload_purged_at: null,
    status: "complete",
    privacy_status: "local",
    approval_status: "consumed",
    admission_status: "admitted",
    admission_reason: "",
    input_tokens_estimated: 1,
    output_tokens_reserved: 1,
    application_status: "applied",
    result_currency: "current",
    can_reapply: false,
    result_uid: `result-${index}`,
    result_fingerprint: null,
    result_schema_version: null,
    apply_attempt_count: 1,
    last_apply_error: "",
    artifact_type: null,
    artifact_uid: null,
    attempts: [],
    input_tokens: 1,
    output_tokens: 1,
    retry_count: 0,
    failure_class: null,
    cancel_requested: false,
    created_at: "2026-07-17T10:00:00Z",
    started_at: "2026-07-17T10:00:00Z",
    finished_at: "2026-07-17T10:01:00Z",
    error: ""
  }));
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialTasks: tasks
    })
  );
  const pageSource = readFileSync(new URL("./page.tsx", import.meta.url), "utf8");
  const panelSource = readFileSync(new URL("./generation-control-panel.tsx", import.meta.url), "utf8");

  assert.match(html, /加载更早请求/);
  assert.match(pageSource, /\/generation\/tasks\?limit=100/);
  assert.match(panelSource, /before_request_uid=\$\{encodeURIComponent\(cursor\)\}/);
  assert.doesNotMatch(panelSource, /offset=\$\{tasks\.length\}/);
});


test("a stale result directs the user back to the event without offering reapply", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialTasks: [{
        request_uid: "request-stale-result",
        task_type: "event-synthesis",
        reason: "explicit-user-request",
        target_type: "event",
        target_uid: "event-1",
        provider: "local",
        model: "gpt-5",
        status: "stale_result",
        privacy_status: "eligible",
        approval_status: "consumed",
        admission_status: "admitted",
        admission_reason: "",
        application_status: "failed",
        result_currency: "stale",
        can_reapply: false,
        result_uid: "result-1",
        attempts: [],
        retry_count: 0,
        failure_class: null,
        cancel_requested: false,
        created_at: "2026-07-17T10:00:00Z",
        error: "生成结果已过期，不能应用"
      }]
    })
  );

  assert.match(html, /结果已过期/);
  assert.match(html, /下一步：结果已过期，不再重新应用/);
  assert.doesNotMatch(html, /重新应用已有结果/);
});


test("generation control renders fail-closed state and auditable task costs", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: {
        global_pause: true,
        auto_run: false,
        daily_budget_tokens: null,
        input_estimator: "unicode-codepoints-v1",
        output_reserve_tokens: 200,
        day_timezone: "Asia/Shanghai",
        used_tokens: 0,
        reserved_tokens: 0,
        remaining_tokens: null,
        requires_usage_review: true
      },
      initialTasks: [
        {
          request_uid: "request-1",
          task_type: "event-synthesis",
          reason: "automatic",
          target_type: "event",
          target_uid: "event-1",
          provider: "local",
          model: "gpt-5",
          status: "blocked",
          privacy_status: "eligible",
          approval_status: "awaiting",
          admission_status: "blocked_budget_unconfigured",
          admission_reason: "每日 Token 预算尚未配置",
          input_tokens_estimated: 4200,
          output_tokens_reserved: 200,
          application_status: "not_started",
          result_uid: null,
          result_fingerprint: null,
          result_schema_version: null,
          apply_attempt_count: 0,
          last_apply_error: "",
          artifact_type: null,
          artifact_uid: null,
          attempts: [],
          input_tokens: null,
          output_tokens: null,
          created_at: "2026-07-17T10:00:00Z",
          started_at: null,
          finished_at: null,
          error: "RuntimeError: secret database path /private/reader"
        }
      ]
    })
  );

  assert.match(html, /全局暂停/);
  assert.match(html, /用量待处理/);
  assert.match(html, /预算未配置/);
  assert.match(html, /本地估算/);
  assert.match(html, /Unicode 字符数（规则 v1）/);
  assert.match(html, /UTF-8 字节数（规则 v1）/);
  assert.match(html, /每日 Token 预算尚未配置/);
  assert.match(html, /触发：自动生成/);
  assert.match(html, /4,200.*估算/);
  assert.match(html, /实际输入.*未知/);
  assert.match(html, /批准一次执行/);
  assert.match(html, /生成任务失败，请重试/);
  assert.doesNotMatch(html, /secret database path/);
  assert.doesNotMatch(html, /Attempt|provider|Generation Requests|admission budget|reserve/);
});


test("generation task shows failure class retry count and next action", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialTasks: [
        {
          request_uid: "request-retry",
          task_type: "event-synthesis",
          reason: "explicit-user-request",
          target_type: "event",
          target_uid: "event-retry",
          provider: "local",
          model: "gpt-5",
          status: "failed",
          privacy_status: "eligible",
          approval_status: "consumed",
          admission_status: "admitted",
          admission_reason: "",
          input_tokens_estimated: 100,
          output_tokens_reserved: 20,
          application_status: "not_started",
          result_uid: null,
          input_tokens: 88,
          output_tokens: 12,
          retry_count: 1,
          failure_class: "transport",
          cancel_requested: false,
          created_at: "2026-07-17T10:00:00Z",
          started_at: "2026-07-17T10:01:00Z",
          finished_at: "2026-07-17T10:02:00Z",
          error: "transport failed"
        }
      ]
    })
  );

  assert.match(html, /失败分类：传输故障/);
  assert.match(html, /已重试 1 次/);
  assert.match(html, /下一步：可在原操作处重试/);
  assert.doesNotMatch(html, />明确重试</);
});


test("automatic retry awaiting approval shows the only valid next action", () => {
  const task = {
    request_uid: "request-auto-retry",
    task_type: "event-synthesis",
    reason: "explicit-user-request",
    target_type: "event",
    target_uid: "event-auto-retry",
    provider: "local",
    model: "gpt-5",
    status: "pending",
    privacy_status: "eligible",
    approval_status: "awaiting",
    admission_status: "awaiting",
    admission_reason: "",
    input_tokens_estimated: 100,
    output_tokens_reserved: 20,
    application_status: "not_started",
    result_uid: null,
    input_tokens: 88,
    output_tokens: 12,
    retry_count: 0,
    failure_class: "transport",
    cancel_requested: false,
    created_at: "2026-07-17T10:00:00Z",
    started_at: "2026-07-17T10:01:00Z",
    finished_at: "2026-07-17T10:02:00Z",
    error: "transport failed"
  };
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialTasks: [task]
    })
  );

  assert.match(html, /下一步：请先批准自动重试/);
  assert.match(html, /批准一次执行/);
  assert.equal(canApproveGenerationTask(task), true);
});

test("apply failed task exposes replay-only detail without private payload", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: {
        global_pause: false,
        auto_run: false,
        daily_budget_tokens: 1000,
        input_estimator: "unicode-codepoints-v1",
        output_reserve_tokens: 100,
        day_timezone: "Asia/Shanghai",
        used_tokens: 57,
        reserved_tokens: 0,
        remaining_tokens: 943,
        requires_usage_review: false
      },
      initialTasks: [{
        request_uid: "request-apply-failed",
        task_type: "event-synthesis",
        reason: "explicit-user-request",
        target_type: "event",
        target_uid: "event-1",
        provider: "local",
        model: "gpt-5",
        status: "apply_failed",
        privacy_status: "eligible",
        approval_status: "consumed",
        admission_status: "admitted",
        admission_reason: "",
        input_tokens_estimated: 40,
        output_tokens_reserved: 100,
        application_status: "failed",
        result_currency: "unverified",
        can_reapply: false,
        result_uid: "result-1",
        result_fingerprint: "a".repeat(64),
        result_schema_version: "event-synthesis-schema-v1",
        apply_attempt_count: 1,
        last_apply_error: "生成结果保存失败，请重试",
        artifact_type: null,
        artifact_uid: null,
        attempts: [{
          attempt_uid: "attempt-1",
          attempt_no: 1,
          status: "complete",
          input_tokens: 45,
          output_tokens: 12,
          started_at: "2026-07-17T10:00:00Z",
          finished_at: "2026-07-17T10:01:00Z",
          error: ""
        }],
        input_tokens: 45,
        output_tokens: 12,
        created_at: "2026-07-17T10:00:00Z",
        started_at: "2026-07-17T10:00:00Z",
        finished_at: "2026-07-17T10:01:00Z",
        error: "生成结果保存失败，请重试"
      }]
    })
  );

  assert.match(html, /应用失败/);
  assert.match(html, /校验并重新应用/);
  assert.match(html, /重新应用前会校验结果是否仍有效/);
  assert.match(html, new RegExp("a{64}"));
  assert.match(html, /event-synthesis-schema-v1/);
  assert.match(html, /应用次数：1/);
  assert.match(html, /Attempt 1 · 已完成/);
  assert.match(html, /当前产物：尚未应用/);
  assert.doesNotMatch(html, /payload|private evidence/);
});

test("a purged committed result with pending apply exposes the same replay-only action", () => {
  const html = renderToStaticMarkup(
    React.createElement(GenerationControlPanel, {
      apiUrl: "http://reader.test",
      initialControl: null,
      initialTasks: [{
        request_uid: "request-apply-pending",
        task_type: "event-synthesis",
        reason: "explicit-user-request",
        target_type: "event",
        target_uid: "event-1",
        provider: "local",
        model: "gpt-5",
        payload_retention: "purged",
        payload_purged_at: "2026-07-18T12:00:00Z",
        status: "apply_pending",
        privacy_status: "eligible",
        approval_status: "consumed",
        admission_status: "admitted",
        admission_reason: "",
        application_status: "pending",
        result_currency: "current",
        can_reapply: true,
        result_uid: "result-1",
        result_fingerprint: "b".repeat(64),
        result_schema_version: "event-synthesis-schema-v1",
        apply_attempt_count: 0,
        last_apply_error: "",
        artifact_type: null,
        artifact_uid: null,
        attempts: [],
        retry_count: 0,
        failure_class: null,
        cancel_requested: false,
        created_at: "2026-07-17T10:00:00Z",
        error: ""
      }]
    })
  );

  assert.match(html, /等待应用/);
  assert.match(html, /下一步：可重新应用已有结果/);
  assert.match(html, /重新应用已有结果/);
  assert.match(html, /请求 Payload.*已按保留策略清理/);
  assert.doesNotMatch(html, /下一步：无需操作/);
});
