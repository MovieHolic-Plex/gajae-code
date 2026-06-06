import { describe, expect, it } from "bun:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { create } from "@bufbuild/protobuf";
import { streamAcpAgent } from "../src/providers/acp-agent";
import {
	type ReadArgs,
	ReadResultSchema,
	ReadSuccessSchema,
	type ShellArgs,
	ShellResultSchema,
	ShellSuccessSchema,
	type WriteArgs,
	WriteErrorSchema,
	WriteResultSchema,
	WriteSuccessSchema,
} from "../src/providers/cursor/gen/agent_pb";
import type { Context, CursorExecHandlers, Model, ToolResultMessage } from "../src/types";

const fixturePath = join(fileURLToPath(new URL(".", import.meta.url)), "fixtures/fake-acp-agent.ts");

const model: Model<"acp-agent"> = {
	id: "composer-2.5",
	name: "Cursor Composer 2.5 ACP",
	api: "acp-agent",
	provider: "cursor-acp",
	baseUrl: "acp://cursor",
	reasoning: false,
	input: ["text"],
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	contextWindow: 200_000,
	maxTokens: 64_000,
};

const context: Context = {
	systemPrompt: ["You are a helpful coding agent."],
	messages: [{ role: "user", content: "Use tools", timestamp: 0 }],
};

function textToolResult(toolCallId: string, toolName: string, text: string): ToolResultMessage {
	return {
		role: "toolResult",
		toolCallId,
		toolName,
		content: [{ type: "text", text }],
		isError: false,
		timestamp: Date.now(),
	};
}

function defaultExecHandlers(): CursorExecHandlers {
	return {
		async read(args: ReadArgs) {
			return textToolResult(args.toolCallId ?? "read", "read", "fake-file-content");
		},
		async write(args: WriteArgs) {
			return {
				result: create(WriteResultSchema, {
					result: {
						case: "success",
						value: create(WriteSuccessSchema, {
							path: args.path,
						}),
					},
				}),
				toolResult: textToolResult(args.toolCallId ?? "write", "write", args.fileText),
			};
		},
		async shell(args: ShellArgs) {
			return textToolResult(args.toolCallId ?? "shell", "bash", args.command);
		},
	};
}

describe("ACP agent provider", () => {
	it("initializes Cursor-style ACP agents with parameterized model picker and applies default config options", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			clientCapabilities: {
				_meta: {
					parameterizedModelPicker: true,
				},
			},
			execHandlers: {
				async read(args) {
					return {
						result: create(ReadResultSchema, {
							result: {
								case: "success",
								value: create(ReadSuccessSchema, {
									path: args.path,
									totalLines: 1,
									fileSize: BigInt("fake-file-content".length),
									truncated: false,
									output: { case: "content", value: "fake-file-content" },
								}),
							},
						}),
						toolResult: textToolResult(args.toolCallId ?? "read", "read", "fake-file-content"),
					};
				},
				async shell(args) {
					return {
						result: create(ShellResultSchema, {
							result: {
								case: "success",
								value: create(ShellSuccessSchema, {
									command: args.command,
									workingDirectory: args.workingDirectory,
									exitCode: 0,
									signal: "",
									stdout: "fake-shell-output",
									stderr: "",
									executionTime: 0,
								}),
							},
						}),
						toolResult: textToolResult(args.toolCallId ?? "shell", "bash", "fake-shell-output"),
					};
				},
			},
		}).result();

		const text = result.content.find(item => item.type === "text")?.text;
		expect(text).toBeDefined();
		const payload = JSON.parse(text ?? "{}");
		expect(payload.initialize.clientCapabilities._meta.parameterizedModelPicker).toBe(true);
		expect(payload.configSets).toContainEqual({
			sessionId: "fake-session",
			configId: "model",
			value: "composer-2.5",
		});
		expect(payload.configSets).toContainEqual({
			sessionId: "fake-session",
			configId: "fast",
			value: "false",
		});
		expect(payload.promptText).toContain("System:\nYou are a helpful coding agent.");
		expect(payload.promptText).toContain("user:\nUse tools");
		expect(payload.readContent).toBe("fake-file-content");
		expect(payload.shellOutput).toBe("fake-shell-output");
		expect(payload.releasedTerminalRejected).toBe(true);
	});

	it("accepts bare tool result handler returns for ACP file and terminal requests", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: {
				async read(args) {
					return textToolResult(args.toolCallId ?? "read", "read", "bare-read-content");
				},
				async shell(args) {
					return textToolResult(args.toolCallId ?? "shell", "bash", "bare-shell-output");
				},
			},
		}).result();

		const text = result.content.find(item => item.type === "text")?.text;
		expect(text).toBeDefined();
		const payload = JSON.parse(text ?? "{}");
		expect(payload.readContent).toBe("bare-read-content");
		expect(payload.shellOutput).toBe("bare-shell-output");
		expect(payload.releasedTerminalRejected).toBe(true);
	});

	it("preserves ACP terminal argv boundaries when dispatching through shell handlers", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: {
				async read(args) {
					return textToolResult(args.toolCallId ?? "read", "read", "fake-file-content");
				},
				async shell(args) {
					return textToolResult(args.toolCallId ?? "shell", "bash", args.command);
				},
			},
		}).result();

		const text = result.content.find(item => item.type === "text")?.text;
		expect(text).toBeDefined();
		const payload = JSON.parse(text ?? "{}");
		expect(payload.shellOutput).toBe("printf %s 'fake shell'");
	});

	it("declares the ACP SDK as an ai package runtime dependency", async () => {
		const packageJson = await Bun.file(join(fileURLToPath(new URL("../", import.meta.url)), "package.json")).json();
		expect(packageJson.dependencies["@agentclientprotocol/sdk"]).toBe("catalog:");
	});

	it("cancels the ACP session and terminates the child process when the stream is aborted", async () => {
		const controller = new AbortController();
		const promptMarkerPath = join(tmpdir(), `gjc-acp-prompt-${crypto.randomUUID()}`);
		const markerPath = join(tmpdir(), `gjc-acp-cancel-${crypto.randomUUID()}`);
		const stream = streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_WAIT_FOR_CANCEL: "1",
				FAKE_ACP_PROMPT_MARKER: promptMarkerPath,
				FAKE_ACP_CANCEL_MARKER: markerPath,
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			signal: controller.signal,
		});

		for (let attempts = 0; attempts < 100 && !(await Bun.file(promptMarkerPath).exists()); attempts++) {
			await Bun.sleep(5);
		}
		expect(await Bun.file(promptMarkerPath).text()).toBe("fake-session");
		controller.abort(new Error("user aborted"));
		await expect(stream.result()).rejects.toThrow(/aborted/);
		expect(await Bun.file(markerPath).text()).toBe("fake-session");
	});

	it("accepts Cursor ACP todo extension updates without failing the turn", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_UPDATE_TODOS: "1",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: {
				async read(args) {
					return textToolResult(args.toolCallId ?? "read", "read", "fake-file-content");
				},
				async shell(args) {
					return textToolResult(args.toolCallId ?? "shell", "bash", args.command);
				},
			},
		}).result();

		const text = result.content.find(item => item.type === "text")?.text;
		expect(text).toBeDefined();
		const payload = JSON.parse(text ?? "{}");
		expect(payload.updateTodosResult).toEqual({});
	});

	it("handles ACP write and permission requests through existing execution handlers", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_WRITE_AND_PERMISSION: "1",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: defaultExecHandlers(),
		}).result();

		const text = result.content.find(item => item.type === "text")?.text;
		expect(text).toBeDefined();
		const payload = JSON.parse(text ?? "{}");
		expect(payload.writePermission).toBe("allow");
		expect(payload.writeContent).toBe("fake-write-content");
	});

	it("fails ACP write requests when handlers return structured write errors", async () => {
		const records: Array<{ direction: string; method?: string; errorCode?: number }> = [];
		const stream = streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_WRITE_AND_PERMISSION: "1",
				FAKE_ACP_CHECK_RELEASE_REJECTED: "0",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: {
				...defaultExecHandlers(),
				async write(args) {
					return {
						result: create(WriteResultSchema, {
							result: {
								case: "error",
								value: create(WriteErrorSchema, {
									path: args.path,
									error: "disk is read-only",
								}),
							},
						}),
						toolResult: textToolResult(args.toolCallId ?? "write", "write", "handler returned structured error"),
					};
				},
			},
			onProtocolMessage(record) {
				records.push({
					direction: record.direction,
					method: record.method,
					errorCode: record.errorCode,
				});
			},
		});

		await expect(stream.result()).rejects.toThrow(/ACP agent provider failed/);
		expect(records).toContainEqual({ direction: "agent_to_client", method: "fs/write_text_file" });
		expect(records).toContainEqual({
			direction: "client_to_agent",
			method: "fs/write_text_file",
			errorCode: -32603,
		});
	});

	it("records ACP protocol methods and response errors for compatibility smoke diagnostics", async () => {
		const records: Array<{ direction: string; method?: string; errorCode?: number }> = [];
		await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_UPDATE_TODOS: "1",
				FAKE_ACP_WRITE_AND_PERMISSION: "1",
				FAKE_ACP_CHECK_RELEASE_REJECTED: "0",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: defaultExecHandlers(),
			onProtocolMessage(record) {
				records.push({
					direction: record.direction,
					method: record.method,
					errorCode: record.errorCode,
				});
			},
		}).result();

		expect(records).toContainEqual({ direction: "client_to_agent", method: "session/prompt" });
		expect(records).toContainEqual({ direction: "agent_to_client", method: "cursor/update_todos" });
		expect(records).toContainEqual({ direction: "agent_to_client", method: "fs/write_text_file" });
		expect(records).toContainEqual({ direction: "agent_to_client", method: "session/request_permission" });
		expect(records.filter(record => record.errorCode === -32601 || record.errorCode === -32603)).toEqual([]);
	});

	it("records unsupported Cursor extension failures with the originating method", async () => {
		const records: Array<{ direction: string; method?: string; errorCode?: number }> = [];
		const stream = streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_UNKNOWN_EXTENSION: "1",
				FAKE_ACP_CHECK_RELEASE_REJECTED: "0",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: defaultExecHandlers(),
			onProtocolMessage(record) {
				records.push({
					direction: record.direction,
					method: record.method,
					errorCode: record.errorCode,
				});
			},
		});

		await expect(stream.result()).rejects.toThrow(/ACP agent provider failed/);
		expect(records).toContainEqual({
			direction: "agent_to_client",
			method: "cursor/unknown_future_method",
		});
		expect(records).toContainEqual({
			direction: "client_to_agent",
			method: "cursor/unknown_future_method",
			errorCode: -32603,
		});
	});

	it("keeps killed ACP terminals valid for output and wait status until release", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_KILL_TERMINAL: "1",
				FAKE_ACP_CHECK_RELEASE_REJECTED: "0",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
			execHandlers: {
				async read(args) {
					return textToolResult(args.toolCallId ?? "read", "read", "fake-file-content");
				},
				async shell(args) {
					if (args.command.startsWith("sleep ")) {
						await Bun.sleep(100);
						return textToolResult(args.toolCallId ?? "shell", "bash", "late-output");
					}
					return textToolResult(args.toolCallId ?? "shell", "bash", args.command);
				},
			},
		}).result();

		const text = result.content.find(item => item.type === "text")?.text;
		expect(text).toBeDefined();
		const payload = JSON.parse(text ?? "{}");
		expect(payload.killExitCode).toBe(143);
		expect(payload.killOutputExitCode).toBe(143);
	});

	it("keeps partial ACP assistant output when the transport closes after a message", async () => {
		const result = await streamAcpAgent(model, context, {
			command: "bun",
			args: [fixturePath],
			env: {
				FAKE_ACP_CLOSE_AFTER_MESSAGE: "1",
			},
			defaultConfigOptions: {
				model: "composer-2.5",
				fast: "false",
			},
		}).result();

		expect(result.stopReason).toBe("stop");
		expect(result.content).toEqual([{ type: "text", text: "partial before closed transport" }]);
	});
});
