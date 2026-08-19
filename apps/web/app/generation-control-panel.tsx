"use client";

import { type FormEvent, useMemo, useState } from "react";

import { userFacingErrorMessage } from "./lib/api";
import type { GenerationTaskStatus } from "./generation-task-status";

const MAX_GENERATION_TOKENS = 2_147_483_647;
const GENERATION_TASK_PAGE_SIZE = 100;

export type GenerationControl = {
  global_pause: boolean;
  auto_run: boolean;
  daily_budget_tokens: number | null;
  input_estimator: "unicode-codepoints-v1" | "utf8-bytes-v1";
  output_reserve_tokens: number;
  day_timezone: string;
  used_tokens: number;
  reserved_tokens: number;
  remaining_tokens: number | null;
  requires_usage_review: boolean;
};

export type GenerationTask = {
  request_uid: string;
  task_type: string;
  reason: string;
  target_type: string;
  target_uid: string;
  provider: string;
  model: string;
  payload_retention: "not_stored" | "retained" | "purged";
  payload_purged_at: string | null;
  status: GenerationTaskStatus;
  privacy_status: "local" | "eligible" | "blocked";
  approval_status: "awaiting" | "approved" | "consumed";
  admission_status: "awaiting" | "blocked_paused" | "blocked_budget_unconfigured" | "blocked_budget" | "blocked_concurrency" | "admitted" | "canceled";
  admission_reason: string;
  input_tokens_estimated: number | null;
  output_tokens_reserved: number | null;
  application_status: "not_started" | "pending" | "applied" | "failed";
  result_currency: "none" | "current" | "stale" | "unverified";
  can_reapply: boolean;
  result_uid: string | null;
  result_fingerprint: string | null;
  result_schema_version: string | null;
  apply_attempt_count: number;
  last_apply_error: string;
  artifact_type: string | null;
  artifact_uid: string | null;
  attempts: Array<{
    attempt_uid: string;
    attempt_no: number;
    status: "pending" | "running" | "failed" | "complete" | "expired" | "canceled";
    input_tokens: number | null;
    output_tokens: number | null;
    started_at: string | null;
    finished_at: string | null;
    error: string;
    runner_events_retention: "not_recorded" | "retained" | "purged";
    runner_events_purged_at: string | null;
  }>;
  input_tokens: number | null;
  output_tokens: number | null;
  retry_count: number;
  failure_class: "transport" | "validation" | "canceled" | null;
  cancel_requested: boolean;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string;
};

export type GenerationRetention = {
  status: "never" | "running" | "succeeded" | "failed";
  last_run_at: string | null;
  finished_at: string | null;
  scanned_count: number;
  deleted_count: number;
  failure_reason: string;
};

export function generationSafetyState(
  control: Pick<GenerationControl, "global_pause" | "auto_run" | "daily_budget_tokens"> | null
) {
  if (control === null) {
    return {
      paused: null,
      tone: "unknown",
      title: "控制状态未知",
      detail: "无法确认是否已暂停，请刷新后再操作。"
    };
  }
  if (control.global_pause) {
    return {
      paused: true,
      tone: "paused",
      title: "全局暂停",
      detail: "不会创建新执行尝试，也不会调用模型服务。"
    };
  }
  if (control.daily_budget_tokens === null) {
    return {
      paused: false,
      tone: "blocked",
      title: "预算未配置",
      detail: "新任务会在创建执行尝试前被阻断。"
    };
  }
  return control.auto_run
    ? {
        paused: false,
        tone: "active",
        title: "允许自动任务准入",
        detail: "自动任务仍需预算充足；不会绕过预算检查。"
      }
    : {
        paused: false,
        tone: "active",
        title: "允许已批准任务准入",
        detail: "任务仍需一次批准且预算充足。"
      };
}

export function canApproveGenerationTask(
  task: Pick<GenerationTask, "status" | "approval_status" | "privacy_status" | "result_uid" | "payload_retention">
) {
  return ["pending", "blocked"].includes(task.status) && task.privacy_status !== "blocked" && task.approval_status === "awaiting" && task.result_uid === null && task.payload_retention !== "purged";
}

export function canReapplyGenerationTask(
  task: Pick<GenerationTask, "can_reapply"> & Partial<Pick<GenerationTask, "result_currency" | "application_status">>
) {
  return task.can_reapply || (
    task.result_currency === "unverified"
    && ["pending", "failed"].includes(task.application_status ?? "")
  );
}

export function parseGenerationTokenInput(value: string, emptyValue: number | null) {
  const trimmed = value.trim();
  if (!trimmed) return emptyValue;
  const parsed = Number(trimmed);
  return Number.isInteger(parsed) && parsed >= 0 && parsed <= MAX_GENERATION_TOKENS ? parsed : undefined;
}

export default function GenerationControlPanel({
  apiUrl,
  initialControl,
  initialTasks,
  initialRetention = null,
  initialError = ""
}: {
  apiUrl: string;
  initialControl: GenerationControl | null;
  initialTasks: GenerationTask[];
  initialRetention?: GenerationRetention | null;
  initialError?: string;
}) {
  const baseUrl = useMemo(() => apiUrl.replace(/\/$/, ""), [apiUrl]);
  const [control, setControl] = useState(initialControl);
  const [tasks, setTasks] = useState(initialTasks);
  const [hasMoreTasks, setHasMoreTasks] = useState(
    initialTasks.length === GENERATION_TASK_PAGE_SIZE
  );
  const [retention, setRetention] = useState(initialRetention);
  const [globalPause, setGlobalPause] = useState(initialControl?.global_pause ?? true);
  const [autoRun, setAutoRun] = useState(initialControl?.auto_run ?? false);
  const [budget, setBudget] = useState(initialControl?.daily_budget_tokens?.toString() ?? "");
  const [inputEstimator, setInputEstimator] = useState<GenerationControl["input_estimator"]>(initialControl?.input_estimator ?? "unicode-codepoints-v1");
  const [reserve, setReserve] = useState((initialControl?.output_reserve_tokens ?? 0).toString());
  const [dayTimezone, setDayTimezone] = useState(initialControl?.day_timezone ?? "Asia/Shanghai");
  const [error, setError] = useState(initialError);
  const [message, setMessage] = useState("");
  const [working, setWorking] = useState("");
  const safetyState = generationSafetyState(control);

  async function load() {
    setWorking("refresh");
    try {
      const [controlResponse, tasksResponse, retentionResponse] = await Promise.all([
        fetch(`${baseUrl}/generation/control`, { cache: "no-store" }),
        fetch(`${baseUrl}/generation/tasks?limit=${GENERATION_TASK_PAGE_SIZE}`, { cache: "no-store" }),
        fetch(`${baseUrl}/generation/retention`, { cache: "no-store" })
      ]);
      if (!controlResponse.ok || !tasksResponse.ok || !retentionResponse.ok) throw new Error("生成控制状态加载失败");
      const nextControl = (await controlResponse.json()) as GenerationControl;
      setControl(nextControl);
      const nextTasks = (await tasksResponse.json()) as GenerationTask[];
      setTasks(nextTasks);
      setHasMoreTasks(nextTasks.length === GENERATION_TASK_PAGE_SIZE);
      setRetention((await retentionResponse.json()) as GenerationRetention);
      syncForm(nextControl);
      setError("");
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "生成控制状态加载失败"));
    } finally {
      setWorking("");
    }
  }

  async function loadOlderTasks() {
    const cursor = tasks[tasks.length - 1]?.request_uid;
    if (!cursor) return;
    setWorking("load-older");
    setError("");
    try {
      const response = await fetch(
        `${baseUrl}/generation/tasks?limit=${GENERATION_TASK_PAGE_SIZE}&before_request_uid=${encodeURIComponent(cursor)}`,
        { cache: "no-store" }
      );
      if (!response.ok) throw new Error("更早生成请求加载失败");
      const olderTasks = (await response.json()) as GenerationTask[];
      setTasks((current) => {
        const seen = new Set(current.map((task) => task.request_uid));
        return [...current, ...olderTasks.filter((task) => !seen.has(task.request_uid))];
      });
      setHasMoreTasks(olderTasks.length === GENERATION_TASK_PAGE_SIZE);
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "更早生成请求加载失败"));
    } finally {
      setWorking("");
    }
  }

  function syncForm(value: GenerationControl) {
    setGlobalPause(value.global_pause);
    setAutoRun(value.auto_run);
    setBudget(value.daily_budget_tokens?.toString() ?? "");
    setInputEstimator(value.input_estimator);
    setReserve(value.output_reserve_tokens.toString());
    setDayTimezone(value.day_timezone);
  }

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (control === null) {
      setError("请先刷新并确认当前生成控制状态");
      return;
    }
    const dailyBudgetTokens = parseGenerationTokenInput(budget, null);
    const outputReserveTokens = parseGenerationTokenInput(reserve, 0);
    if (dailyBudgetTokens === undefined || outputReserveTokens === undefined) {
      setMessage("");
      setError(`预算和输出预留必须是 0–${MAX_GENERATION_TOKENS} 的整数`);
      return;
    }
    setWorking("save");
    setMessage("");
    setError("");
    try {
      const response = await fetch(`${baseUrl}/generation/control`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          global_pause: globalPause,
          auto_run: autoRun,
          daily_budget_tokens: dailyBudgetTokens,
          input_estimator: inputEstimator,
          output_reserve_tokens: outputReserveTokens,
          day_timezone: dayTimezone.trim()
        })
      });
      const body = (await response.json()) as GenerationControl & { detail?: unknown };
      if (!response.ok) throw new Error(typeof body.detail === "string" ? body.detail : `保存失败（${response.status}）`);
      setControl(body);
      syncForm(body);
      setMessage("生成控制已保存");
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "生成控制保存失败"));
    } finally {
      setWorking("");
    }
  }

  async function approve(requestUid: string) {
    setWorking(requestUid);
    setMessage("");
    setError("");
    try {
      const response = await fetch(`${baseUrl}/generation/requests/${requestUid}/approve`, { method: "POST" });
      const task = (await response.json()) as GenerationTask & { detail?: unknown };
      if (!response.ok) throw new Error(typeof task.detail === "string" ? task.detail : `批准失败（${response.status}）`);
      setTasks((current) => current.map((row) => (row.request_uid === requestUid ? task : row)));
      setMessage("已批准一次执行；仍需通过暂停与预算检查");
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "任务批准失败"));
    } finally {
      setWorking("");
    }
  }

  async function reapply(task: GenerationTask) {
    const requestUid = task.request_uid;
    setWorking(`reapply-${requestUid}`);
    setMessage("");
    setError("");
    try {
      if (task.result_currency === "unverified") {
        const validationResponse = await fetch(`${baseUrl}/generation/requests/${requestUid}`, { cache: "no-store" });
        const validated = (await validationResponse.json()) as GenerationTask & { detail?: unknown };
        if (!validationResponse.ok) throw new Error(typeof validated.detail === "string" ? validated.detail : `结果校验失败（${validationResponse.status}）`);
        setTasks((current) => current.map((candidate) => candidate.request_uid === requestUid ? validated : candidate));
        if (!validated.can_reapply) {
          setMessage(validated.result_currency === "stale" ? "已有结果已过期，未执行应用" : "已有结果当前不能重新应用");
          return;
        }
      }
      const response = await fetch(`${baseUrl}/generation/requests/${requestUid}/reapply`, { method: "POST" });
      const updatedTask = (await response.json()) as GenerationTask & { detail?: unknown };
      if (!response.ok) throw new Error(typeof updatedTask.detail === "string" ? updatedTask.detail : `重新应用失败（${response.status}）`);
      setTasks((current) => current.map((row) => (row.request_uid === requestUid ? updatedTask : row)));
      setMessage("已重新应用已有结果；未创建新的模型执行");
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "重新应用已有结果失败"));
    } finally {
      setWorking("");
    }
  }

  return (
    <section id="settings-generation-control" className="settings-block generation-control">
      <div className="settings-block-heading">
        <div>
          <h3>生成控制</h3>
          <p className="source-meta">所有新执行尝试都先经过批准、暂停和本地用量预算检查。</p>
        </div>
        <button type="button" onClick={() => void load()} disabled={Boolean(working)}>{working === "refresh" ? "刷新中" : "刷新"}</button>
      </div>
      {error ? <p className="error-line" role="alert">{error}</p> : null}
      {message ? <p className="status-line success-line" role="status">{message}</p> : null}
      <div className={`generation-safety-state ${safetyState.tone}`}>
        <strong>{safetyState.title}</strong>
        <span>{safetyState.detail}</span>
      </div>
      <div className={`generation-safety-state ${retentionTone(retention?.status)}`}>
        <strong>保留清理：{retentionStatusLabel(retention?.status)}</strong>
        <span>{retentionDetail(retention)}</span>
      </div>
      {control?.requires_usage_review ? (
        <div className="generation-safety-state blocked">
          <strong>用量待处理</strong>
          <span>存在实际 Token 用量未知的结果，自动运行已关闭；核对后可重新启用。</span>
        </div>
      ) : null}
      <form className="form-stack" noValidate onSubmit={save}>
        <label className="editor-toggle-row">
          <span>全局暂停</span>
          <input name="global_pause" type="checkbox" checked={globalPause} onChange={(event) => setGlobalPause(event.target.checked)} />
        </label>
        <label className="editor-toggle-row">
          <span>自动运行策略</span>
          <input name="auto_run" type="checkbox" checked={autoRun} onChange={(event) => setAutoRun(event.target.checked)} />
        </label>
        <p className="source-meta">关闭时，自动产生的生成请求会等待批准；开启也不会绕过暂停或预算。</p>
        <label>
          每日准入预算（Token）
          <input name="daily_budget_tokens" type="number" min="0" max={MAX_GENERATION_TOKENS} inputMode="numeric" value={budget} onChange={(event) => setBudget(event.target.value)} placeholder="预算未配置" />
        </label>
        <label>
          输入估算规则
          <select name="input_estimator" value={inputEstimator} onChange={(event) => setInputEstimator(event.target.value as GenerationControl["input_estimator"])}>
            <option value="unicode-codepoints-v1">Unicode 字符数（规则 v1）</option>
            <option value="utf8-bytes-v1">UTF-8 字节数（规则 v1）</option>
          </select>
        </label>
        <label>
          每次输出预留（Token）
          <input name="output_token_allowance" type="number" min="0" max={MAX_GENERATION_TOKENS} inputMode="numeric" value={reserve} onChange={(event) => setReserve(event.target.value)} />
        </label>
        <label>
          日界时区
          <input name="day_timezone" value={dayTimezone} onChange={(event) => setDayTimezone(event.target.value)} placeholder="Asia/Shanghai" />
        </label>
        <div className="source-meta">输入规则：本地估算 · {estimatorLabel(control?.input_estimator)}。估算只用于准入，不会冒充模型服务的实际用量。</div>
        <button type="submit" disabled={Boolean(working) || control === null}>{working === "save" ? "保存中..." : "保存生成控制"}</button>
      </form>
      <dl className="generation-ledger">
        <Ledger label="今日实际" value={control?.used_tokens ?? null} />
        <Ledger label="活跃预留" value={control?.reserved_tokens ?? null} />
        <Ledger label="今日可用" value={control?.remaining_tokens ?? null} unconfigured={control !== null && control.daily_budget_tokens === null} />
      </dl>
      <div className="generation-task-list">
        <h4>生成请求</h4>
        {tasks.length ? tasks.map((task) => (
          <article className="generation-task" key={task.request_uid}>
            <div className="generation-task-heading">
              <div>
                <strong>{taskLabel(task.task_type)}</strong>
                <span className={`generation-task-status ${task.status}`}>{statusLabel(task.status)}</span>
              </div>
              <span className="source-meta">{formatDateTime(task.created_at)}</span>
            </div>
            {task.admission_reason ? <p className="generation-block-reason">{task.admission_reason}</p> : null}
            <p className="source-meta">触发：{reasonLabel(task.reason)}</p>
            <p className="source-meta">失败分类：{failureClassLabel(task.failure_class)} · 已重试 {task.retry_count} 次 · 下一步：{nextActionLabel(task)}</p>
            {task.error ? <p className="error-line">{userFacingErrorMessage(task.error, "生成任务失败，请重试")}</p> : null}
            <dl className="generation-task-costs">
              <Cost label="输入" value={task.input_tokens_estimated} suffix="（估算）" />
              <Cost label="输出预留" value={task.output_tokens_reserved} />
              <Cost label="实际输入" value={task.input_tokens} />
              <Cost label="实际输出" value={task.output_tokens} />
            </dl>
            <div className="generation-task-footer">
              <span className="source-meta">批准：{task.result_uid ? "已有结果，无需批准" : approvalLabel(task.approval_status)} · 模型：{providerLabel(task.provider)} / {task.model}</span>
              {canApproveGenerationTask(task) ? (
                <button type="button" onClick={() => void approve(task.request_uid)} disabled={Boolean(working)}>{working === task.request_uid ? "批准中..." : "批准一次执行"}</button>
              ) : canReapplyGenerationTask(task) ? (
                <button type="button" onClick={() => void reapply(task)} disabled={Boolean(working)}>{working === `reapply-${task.request_uid}` ? "校验并应用中..." : task.result_currency === "unverified" ? "校验并重新应用" : "重新应用已有结果"}</button>
              ) : null}
            </div>
            <details className="generation-task-detail">
              <summary>任务详情</summary>
              <dl>
                <div><dt>请求 Payload</dt><dd>{retentionItemLabel(task.payload_retention, task.payload_purged_at)}</dd></div>
                <div><dt>Result 指纹</dt><dd>{task.result_fingerprint ?? "尚无结果"}</dd></div>
                <div><dt>结果 Schema</dt><dd>{task.result_schema_version ?? "尚无结果"}</dd></div>
                <div><dt>应用次数</dt><dd>应用次数：{task.apply_attempt_count ?? 0}</dd></div>
                <div><dt>当前产物</dt><dd>当前产物：{task.artifact_type && task.artifact_uid ? `${task.artifact_type} · ${task.artifact_uid}` : "尚未应用"}</dd></div>
              </dl>
              {task.last_apply_error ? <p className="error-line">最后应用错误：{userFacingErrorMessage(task.last_apply_error, "生成结果应用失败，请重试")}</p> : null}
              <div className="generation-attempt-list">
                {(task.attempts ?? []).map((attempt) => (
                  <div key={attempt.attempt_uid}>
                    <strong>Attempt {attempt.attempt_no} · {attemptStatusLabel(attempt.status)}</strong>
                    <span>{attempt.started_at ? formatDateTime(attempt.started_at) : "尚未开始"} · 输入 {formatTokens(attempt.input_tokens)} / 输出 {formatTokens(attempt.output_tokens)}</span>
                    <span>历史执行审计：{retentionItemLabel(attempt.runner_events_retention, attempt.runner_events_purged_at)}</span>
                    {attempt.error ? <span>{userFacingErrorMessage(attempt.error, "生成执行失败")}</span> : null}
                  </div>
                ))}
              </div>
            </details>
          </article>
        )) : <p className="source-meta">暂无生成请求。</p>}
        {hasMoreTasks ? (
          <button type="button" onClick={() => void loadOlderTasks()} disabled={Boolean(working)}>
            {working === "load-older" ? "加载中..." : "加载更早请求"}
          </button>
        ) : null}
      </div>
    </section>
  );
}

function Ledger({ label, value, unconfigured = false }: { label: string; value: number | null; unconfigured?: boolean }) {
  return <div><dt>{label}</dt><dd>{unconfigured ? "预算未配置" : formatTokens(value)}</dd></div>;
}

function Cost({ label, value, suffix = "" }: { label: string; value: number | null; suffix?: string }) {
  return <div><dt>{label}</dt><dd>{value == null ? "未知" : `${formatTokens(value)}${suffix}`}</dd></div>;
}

function formatTokens(value: number | null) {
  return value == null ? "未知" : value.toLocaleString("zh-CN");
}

const generationDateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  day: "2-digit",
  hour: "2-digit",
  hour12: false,
  minute: "2-digit",
  month: "2-digit",
  timeZone: "Asia/Shanghai"
});

function formatDateTime(value: string) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : generationDateTimeFormatter.format(date);
}

function taskLabel(taskType: string) {
  return taskType === "event-synthesis" || taskType === "event_synthesis" ? "事件合成" : "其他生成任务";
}

function reasonLabel(reason: string) {
  return {
    automatic: "自动生成",
    "explicit-user-request": "用户主动请求",
    "material-evidence-change": "证据变化"
  }[reason] ?? "系统登记";
}

function estimatorLabel(estimator: GenerationControl["input_estimator"] | undefined) {
  if (estimator === "utf8-bytes-v1") return "UTF-8 字节数（规则 v1）";
  return "Unicode 字符数（规则 v1）";
}

function providerLabel(provider: string) {
  return { local: "本地", legacy: "历史任务", openai_compatible: "远端兼容接口", remote: "远端" }[provider] ?? "已配置服务";
}

function statusLabel(status: GenerationTask["status"]) {
  return { pending: "等待中", blocked: "已阻断", running: "运行中", failed: "执行失败", apply_pending: "等待应用", apply_failed: "应用失败", stale_result: "结果已过期", complete: "已完成", canceled: "已取消" }[status];
}

function failureClassLabel(value: GenerationTask["failure_class"]) {
  if (value === null) return "无";
  return { transport: "传输故障", validation: "结构或内容校验失败", canceled: "用户取消" }[value];
}

function nextActionLabel(task: GenerationTask) {
  if (task.payload_retention === "purged" && task.result_uid === null) return "请求正文已清理，请从原业务重新发起";
  if (task.cancel_requested) return "历史任务已请求取消";
  if (task.status === "failed" || task.status === "canceled") return "可在原操作处重试";
  if (task.status === "running") return task.provider === "legacy" ? "历史任务不会继续执行" : "等待完成";
  if (task.status === "pending" && task.approval_status === "awaiting" && task.failure_class === "transport") return "请先批准自动重试";
  if (task.status === "pending" || task.status === "blocked") return task.provider === "legacy" ? "历史任务不会继续执行" : "处理阻断原因";
  if (task.status === "apply_pending" || task.status === "apply_failed") return task.result_currency === "unverified" ? "重新应用前会校验结果是否仍有效" : "可重新应用已有结果";
  if (task.status === "stale_result") return "结果已过期，不再重新应用";
  return "无需操作";
}

function attemptStatusLabel(status: GenerationTask["attempts"][number]["status"]) {
  return { pending: "等待中", running: "运行中", failed: "执行失败", complete: "已完成", expired: "租约过期", canceled: "已取消" }[status];
}

function approvalLabel(status: GenerationTask["approval_status"]) {
  return { awaiting: "等待批准", approved: "已批准一次", consumed: "批准已消费" }[status];
}

function retentionTone(status: GenerationRetention["status"] | undefined) {
  if (status === "succeeded") return "active";
  if (status === "failed") return "blocked";
  return "unknown";
}

function retentionStatusLabel(status: GenerationRetention["status"] | undefined) {
  return { never: "尚未运行", running: "运行中", succeeded: "已完成", failed: "最近失败" }[status ?? "never"];
}

function retentionDetail(retention: GenerationRetention | null) {
  if (retention === null || retention.status === "never") return "尚无清理记录；正文最多保留 30 天。";
  if (retention.status === "running") return `开始于 ${formatDateTime(retention.last_run_at ?? "")}`;
  const summary = `${retention.finished_at ? formatDateTime(retention.finished_at) : "时间未知"} · 扫描 ${retention.scanned_count} 项 · 清理 ${retention.deleted_count} 项`;
  return retention.status === "failed"
    ? `${summary} · ${userFacingErrorMessage(retention.failure_reason, "清理失败，请稍后重试")}`
    : summary;
}

function retentionItemLabel(status: "not_stored" | "not_recorded" | "retained" | "purged", purgedAt: string | null) {
  if (status === "purged") return `已按保留策略清理${purgedAt ? ` · ${formatDateTime(purgedAt)}` : ""}`;
  if (status === "retained") return "保留中（最多 30 天）";
  return "未保存";
}
