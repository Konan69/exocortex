import { Database } from "bun:sqlite"
import { homedir } from "node:os"
import { join } from "node:path"
import { Effect, Option, Schema } from "effect"
import { io } from "./extraction.ts"
import { canonicalJSON, digest, type Source } from "./knowledge.ts"

const Links = Schema.Array(Schema.String)
const Post = Schema.Struct({
  url: Schema.String,
  text: Schema.String,
  authorHandle: Schema.optional(Schema.String),
  authorName: Schema.optional(Schema.String),
  links: Schema.optional(Links),
})
const Bookmark = Schema.Struct({ ...Post.fields, id: Schema.String, quotedTweet: Schema.optional(Post) })
const Label = Schema.Struct({ id: Schema.String, category: Schema.NullOr(Schema.String), signal: Schema.NullOr(Schema.Finite) })
const FtRow = Schema.Struct({
  id: Schema.String,
  links_json: Schema.NullOr(Schema.fromJsonString(Links)),
  quoted_tweet_json: Schema.NullOr(Schema.fromJsonString(Post)),
  article_title: Schema.NullOr(Schema.String),
  article_text: Schema.NullOr(Schema.String),
})

const pathOf = (value: Option.Option<string>, fallback: string) => {
  const path = Option.getOrElse(value, () => fallback)
  return path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path
}
const byline = ({ authorHandle, authorName }: typeof Post.Type) =>
  authorHandle ? `${authorName || authorHandle} (@${authorHandle})` : authorName || "unknown"
const emptyIfMissing = async (path: string) => await Bun.file(path).exists() ? Bun.file(path).text() : ""

export const readBookmarks = Effect.fn("readBookmarks")(function*(options: {
  jsonl: Option.Option<string>; labels: Option.Option<string>; skip: Option.Option<string>; ftdb: Option.Option<string>
}) {
  const sourcePath = pathOf(options.jsonl, "~/.fieldtheory/bookmarks/bookmarks.jsonl")
  const labelsPath = pathOf(options.labels, "~/.local/share/bookmark-jev/jev-labels.jsonl")
  const skipPath = pathOf(options.skip, "~/.local/share/bookmark-jev/tombstoned.json")
  const dbPath = pathOf(options.ftdb, "~/.fieldtheory/bookmarks/bookmarks.db")
  const SOURCE = Schema.fromJsonString(Bookmark)
  const LABEL = Schema.fromJsonString(Label)
  const sourceLines = (yield* io(() => Bun.file(sourcePath).text())).split("\n")
  const labelLines = (yield* io(() => emptyIfMissing(labelsPath))).split("\n").filter(line => line.trim())
  const labels = new Map((yield* Schema.decodeUnknownEffect(Schema.Array(LABEL))(labelLines)).map(row => [row.id, row]))
  const tombstones = yield* io(() => emptyIfMissing(skipPath))
  const retired = new Set((yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Array(Schema.String)))(tombstones || "[]")).map(id => `x:${id}`))
  const extras = yield* io(async () => {
    if (!(await Bun.file(dbPath).exists())) return []
    using db = new Database(dbPath, { readonly: true })
    return Schema.decodeUnknownSync(Schema.Array(FtRow))(db.query(
      "SELECT id, links_json, quoted_tweet_json, article_title, article_text FROM bookmarks",
    ).all())
  })
  const byId = new Map(extras.map(row => [row.id, row]))
  const rows = new Map<string, { source: Source; article: boolean }>()
  let skipped = 0
  for (const [index, line] of sourceLines.entries()) {
    if (!line.trim()) continue
    const post = yield* Schema.decodeUnknownEffect(SOURCE)(line)
    const id = `x:${post.id}`
    if (retired.has(id)) { skipped++; continue }
    const extra = byId.get(post.id)
    const label = labels.get(post.id)
    const quote = post.quotedTweet ?? extra?.quoted_tweet_json
    const links = [...new Set([...(post.links ?? []), ...(quote?.links ?? []), ...(extra?.links_json ?? []), ...(quote ? [quote.url] : [])])]
      .filter(url => URL.parse(url)?.hostname !== "t.co")
    const articleText = extra?.article_text ?? ""
    const parts = [{ path: sourcePath, locator: `line ${index + 1}`, hash: digest(line) }]
    if (extra) parts.push({ path: dbPath, locator: `bookmarks:${post.id}`, hash: digest(canonicalJSON(extra)) })
    const content = [
      `Author: ${byline(post)}`,
      post.text,
      quote?.text ? `Quoted post by ${byline(quote)}:\n${quote.text}` : "",
      links.length ? `Links:\n${links.join("\n")}` : "",
      extra?.article_title ? `Article: ${extra.article_title}` : "",
      articleText ? `Article text:\n${articleText}` : "",
    ].filter(Boolean).join("\n\n").toWellFormed()
    rows.set(id, {
      article: articleText.trim().length > 200,
      source: {
        id, kind: "x_bookmark", source: "x", url: post.url.toWellFormed(), content,
        title: `@${post.authorHandle || "unknown"}: ${post.text.trim().replace(/\s+/g, " ").slice(0, 80)}`.toWellFormed(),
        ...(label?.category == null ? {} : { category: label.category.toWellFormed() }),
        ...(label?.signal == null ? {} : { salience: Math.max(0, Math.min(1, label.signal / 3)) }),
        provenance: { kind: "assembled", parts },
      },
    })
  }
  return { rows, retired, skipped }
})
