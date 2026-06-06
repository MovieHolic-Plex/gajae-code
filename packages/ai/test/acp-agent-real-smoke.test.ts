import { describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { create } from "@bufbuild/protobuf";
import { type AcpProtocolRecord, streamAcpAgent } from "../src/providers/acp-agent";
import {
	ReadResultSchema,
	ReadSuccessSchema,
	type ShellArgs,
	ShellFailureSchema,
	ShellResultSchema,
	ShellSuccessSchema,
	type WriteArgs,
	WriteResultSchema,
	WriteSuccessSchema,
} from "../src/providers/cursor/gen/agent_pb";
import type { Context, CursorExecHandlers, Model } from "../src/types";

const smoke = process.env.GJC_CURSOR_ACP_SMOKE === "1" ? it : it.skip;

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

function workspacePath(workspace: string, target: string): string {
	return path.isAbsolute(target) ? target : path.join(workspace, target);
}

function smokeExecHandlers(workspace: string): CursorExecHandlers {
	return {
		async read(args) {
			const target = workspacePath(workspace, args.path);
			const content = await Bun.file(target).text();
			return create(ReadResultSchema, {
				result: {
					case: "success",
					value: create(ReadSuccessSchema, {
						path: args.path,
						totalLines: content.split(/\r\n|\r|\n/).length,
						fileSize: BigInt(content.length),
						truncated: false,
						output: { case: "content", value: content },
					}),
				},
			});
		},
		async write(args: WriteArgs) {
			const target = workspacePath(workspace, args.path);
			await fs.mkdir(path.dirname(target), { recursive: true });
			await Bun.write(target, args.fileText);
			return create(WriteResultSchema, {
				result: {
					case: "success",
					value: create(WriteSuccessSchema, { path: args.path }),
				},
			});
		},
		async shell(args: ShellArgs) {
			const proc = Bun.spawn(["bash", "-lc", args.command], {
				cwd: args.workingDirectory || workspace,
				stdout: "pipe",
				stderr: "pipe",
			});
			const [stdout, stderr, exitCode] = await Promise.all([
				new Response(proc.stdout).text(),
				new Response(proc.stderr).text(),
				proc.exited,
			]);
			const shellValue = {
				command: args.command,
				workingDirectory: args.workingDirectory || workspace,
				exitCode,
				signal: "",
				stdout,
				stderr,
				executionTime: 0,
			};
			return create(ShellResultSchema, {
				result:
					exitCode === 0
						? { case: "success", value: create(ShellSuccessSchema, shellValue) }
						: { case: "failure", value: create(ShellFailureSchema, shellValue) },
			});
		},
	};
}

describe("ACP agent provider real Cursor smoke", () => {
	smoke(
		"runs real cursor-agent ACP through file, terminal, todo, and recorder paths",
		async () => {
			const workspace = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-cursor-acp-smoke-"));
			await Bun.write(
				path.join(workspace, "README.md"),
				"Cursor ACP smoke workspace. Create smoke-result.txt with the requested content.\n",
			);
			const records: AcpProtocolRecord[] = [];
			const context: Context = {
				systemPrompt: [
					"You are running in an automated smoke test. Keep the response short and use tools when needed.",
				],
				messages: [
					{
						role: "user",
						content:
							"Create a todo, write smoke-result.txt containing exactly acp-smoke-ok, read it back, run `printf acp-terminal-ok`, and finish.",
						timestamp: Date.now(),
					},
				],
			};

			await streamAcpAgent(model, context, {
				cwd: workspace,
				execHandlers: smokeExecHandlers(workspace),
				onProtocolMessage(record) {
					records.push(record);
				},
			}).result();
			await Bun.write(path.join(workspace, "protocol-records.json"), JSON.stringify(records, null, 2));
			const methods = new Set(records.map(record => record.method).filter(method => method !== undefined));

			expect(await Bun.file(path.join(workspace, "smoke-result.txt")).text()).toBe("acp-smoke-ok");
			expect(records.filter(record => record.errorCode === -32601 || record.errorCode === -32603)).toEqual([]);
			expect(methods.has("session/prompt")).toBe(true);
			expect(methods.has("cursor/update_todos")).toBe(true);
		},
		120_000,
	);
});
