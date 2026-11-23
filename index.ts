import { appendFileSync } from "node:fs"
import { homedir } from "node:os"
import { join } from "node:path"
import { Plugin } from "@opencode/plugin"

/**
 * Abort an SSE response when no real SSE field line arrives for `stallMs`.
 * Pure comment heartbeats (`: keepalive`) are ignored, so they cannot keep a
 * dead generation alive. The stream error is surfaced to OpenCode's provider
 * layer as a transport failure, which triggers its normal retry.
 */
function watchdog(
  body: ReadableStream<Uint8Array>,
  stallMs: number,
  url: string,
  logPath: string,
): ReadableStream<Uint8Array> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let timer: ReturnType<typeof setTimeout> | undefined
  let done = false

  const report = (reason: string) => {
    try {
      appendFileSync(logPath, `${new Date().toISOString()} ${reason} url=${url} stallMs=${stallMs}\n`)
    } catch {
      // Logging must never affect the request.
    }
  }

  const arm = (controller: ReadableStreamDefaultController<Uint8Array>) => {
    if (timer) clearTimeout(timer)
    timer = setTimeout(() => {
      if (done) return
      done = true
      report("ABORT no-data-event")
      console.log(`[sse-watchdog] aborted after ${stallMs}ms without a data event: ${url}`)
      controller.error(new Error(`SSE data stalled for ${stallMs}ms (no data: event)`))
      void reader.cancel().catch(() => {})
    }, stallMs)
  }

  return new ReadableStream<Uint8Array>({
    start(controller) {
      arm(controller)
    },
    async pull(controller) {
      let result: ReadableStreamReadResult<Uint8Array>
      try {
        result = await reader.read()
      } catch (error) {
        if (!done) {
          done = true
          if (timer) clearTimeout(timer)
          controller.error(error)
        }
        return
      }
      if (done) return
      if (result.done) {
        done = true
        if (timer) clearTimeout(timer)
        controller.close()
        return
      }
      const text = decoder.decode(result.value, { stream: true })
      const progress = text.split(/\r?\n/).some((line) => {
        const value = line.trim()
        return value.length > 0 && !value.startsWith(":")
      })
      if (progress) arm(controller)
      controller.enqueue(result.value)
    },
    cancel(reason) {
      done = true
      if (timer) clearTimeout(timer)
      return reader.cancel(reason)
    },
  })
}

export default Plugin.define({
  id: "sse-watchdog",
  async setup(ctx) {
    const options = (ctx.options ?? {}) as { enabled?: boolean; stallMs?: number; log?: string }
    if (options.enabled === false) return
    const stallMs = Math.max(1000, Number(options.stallMs ?? 90_000))
    const logPath = options.log ?? join(homedir(), ".opencode2", "share", "opencode", "sse-watchdog.log")

    await ctx.session.hook("http.response", (event) => {
      const response = event.response
      if (!response.body) return
      const contentType = (response.headers.get("content-type") ?? "").toLowerCase()
      if (!contentType.includes("text/event-stream")) return
      event.response = new Response(watchdog(response.body, stallMs, event.request.url, logPath), response)
    })
  },
})
