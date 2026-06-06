import { Readable, Writable } from "node:stream";
import * as acp from "@agentclientprotocol/sdk";

type CapturedState = {
	initialize?: acp.InitializeRequest;
	configSets: acp.SetSessionConfigOptionRequest[];
	promptText?: string;
	readContent?: string;
	writePermission?: string;
	writeContent?: string;
	shellOutput?: string;
	killExitCode?: number | null;
	killOutputExitCode?: number | null;
	releasedTerminalRejected?: boolean;
	cancelledSessionId?: string;
	updateTodosResult?: Record<string, unknown>;
};

const state: CapturedState = {
	configSets: [],
};

class FakeAgent implements acp.Agent {
	async authenticate(_params: acp.AuthenticateRequest): Promise<acp.AuthenticateResponse> {
		return {};
	}

	async initialize(params: acp.InitializeRequest): Promise<acp.InitializeResponse> {
		state.initialize = params;
		return {
			protocolVersion: acp.PROTOCOL_VERSION,
			agentCapabilities: {},
			agentInfo: { name: "fake-acp-agent", version: "0.0.0" },
		};
	}

	async newSession(_params: acp.NewSessionRequest): Promise<acp.NewSessionResponse> {
		return {
			sessionId: "fake-session",
			configOptions: [
				{
					id: "model",
					name: "Model",
					type: "select",
					category: "model",
					currentValue: "composer-2.5",
					options: [{ value: "composer-2.5", name: "Composer 2.5" }],
				},
				{
					id: "fast",
					name: "Fast",
					type: "select",
					currentValue: "true",
					options: [
						{ value: "false", name: "Off" },
						{ value: "true", name: "Fast" },
					],
				},
			],
		};
	}

	async setSessionConfigOption(
		params: acp.SetSessionConfigOptionRequest,
	): Promise<acp.SetSessionConfigOptionResponse> {
		state.configSets.push(params);
		return { configOptions: [] };
	}

	async prompt(params: acp.PromptRequest): Promise<acp.PromptResponse> {
		if (process.env.FAKE_ACP_WAIT_FOR_CANCEL === "1") {
			if (process.env.FAKE_ACP_PROMPT_MARKER) {
				await Bun.write(process.env.FAKE_ACP_PROMPT_MARKER, params.sessionId);
			}
			while (!state.cancelledSessionId) {
				await Bun.sleep(5);
			}
			await connection.sessionUpdate({
				sessionId: params.sessionId,
				update: {
					sessionUpdate: "agent_message_chunk",
					content: {
						type: "text",
						text: JSON.stringify({
							cancelledSessionId: state.cancelledSessionId,
						}),
					},
				},
			});
			return { stopReason: "cancelled" };
		}

		state.promptText = params.prompt.map(item => (item.type === "text" ? item.text : "")).join("\n");
		if (process.env.FAKE_ACP_CLOSE_AFTER_MESSAGE === "1") {
			await connection.sessionUpdate({
				sessionId: params.sessionId,
				update: {
					sessionUpdate: "agent_message_chunk",
					content: {
						type: "text",
						text: "partial before closed transport",
					},
				},
			});
			process.exit(0);
		}
		if (process.env.FAKE_ACP_UPDATE_TODOS === "1") {
			state.updateTodosResult = await connection.extMethod("cursor/update_todos", {
				toolCallId: "fake-update-todos",
				todos: [
					{
						id: "todo-1",
						content: "Implement ACP extension method handling",
						status: "in_progress",
					},
				],
				merge: true,
			});
		}
		if (process.env.FAKE_ACP_UNKNOWN_EXTENSION === "1") {
			await connection.extMethod("cursor/unknown_future_method", {
				toolCallId: "fake-unknown-extension",
			});
		}
		const read = await connection.readTextFile({
			sessionId: params.sessionId,
			path: "/tmp/fake-acp.txt",
		});
		state.readContent = read.content;
		if (process.env.FAKE_ACP_WRITE_AND_PERMISSION === "1") {
			const permission = await connection.requestPermission({
				sessionId: params.sessionId,
				toolCall: {
					toolCallId: "fake-write-permission",
					title: "Write fake file",
					kind: "edit",
				},
				options: [
					{ optionId: "reject", name: "Reject", kind: "reject_once" },
					{ optionId: "allow", name: "Allow", kind: "allow_always" },
				],
			});
			state.writePermission = permission.outcome.outcome === "selected" ? permission.outcome.optionId : "cancelled";
			await connection.writeTextFile({
				sessionId: params.sessionId,
				path: "/tmp/fake-acp-write.txt",
				content: "fake-write-content",
			});
			state.writeContent = "fake-write-content";
		}
		if (process.env.FAKE_ACP_KILL_TERMINAL === "1") {
			const killTerminal = await connection.createTerminal({
				sessionId: params.sessionId,
				command: "sleep",
				args: ["10"],
				cwd: "/tmp",
			});
			await killTerminal.kill();
			const exit = await killTerminal.waitForExit();
			const output = await killTerminal.currentOutput();
			state.killExitCode = exit.exitCode;
			state.killOutputExitCode = output.exitStatus?.exitCode ?? null;
			await killTerminal.release();
		}

		const terminal = await connection.createTerminal({
			sessionId: params.sessionId,
			command: "printf",
			args: ["%s", "fake shell"],
			cwd: "/tmp",
		});
		await terminal.waitForExit();
		const terminalOutput = await terminal.currentOutput();
		state.shellOutput = terminalOutput.output;
		await terminal.release();
		if (process.env.FAKE_ACP_CHECK_RELEASE_REJECTED !== "0") {
			try {
				await terminal.currentOutput();
				state.releasedTerminalRejected = false;
			} catch {
				state.releasedTerminalRejected = true;
			}
		}

		await connection.sessionUpdate({
			sessionId: params.sessionId,
			update: {
				sessionUpdate: "agent_message_chunk",
				content: {
					type: "text",
					text: JSON.stringify({
						initialize: state.initialize,
						configSets: state.configSets,
						promptText: state.promptText,
						readContent: state.readContent,
						writePermission: state.writePermission,
						writeContent: state.writeContent,
						shellOutput: state.shellOutput,
						killExitCode: state.killExitCode,
						killOutputExitCode: state.killOutputExitCode,
						releasedTerminalRejected: state.releasedTerminalRejected,
						updateTodosResult: state.updateTodosResult,
					}),
				},
			},
		});
		return { stopReason: "end_turn" };
	}

	async cancel(params: acp.CancelNotification): Promise<void> {
		state.cancelledSessionId = params.sessionId;
		if (process.env.FAKE_ACP_CANCEL_MARKER) {
			await Bun.write(process.env.FAKE_ACP_CANCEL_MARKER, params.sessionId);
		}
	}
}

let connection: acp.AgentSideConnection;
const stream = acp.ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin));
connection = new acp.AgentSideConnection(() => new FakeAgent(), stream);
