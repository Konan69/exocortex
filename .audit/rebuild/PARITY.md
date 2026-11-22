# Rebuild CLI parity with main

This table covers main's CLI. Shared flags apply to `search`, `docs`, `entities`, and `categories` unless a row names a smaller scope.

| Main command or flag | Rebuild status | Detail |
| --- | --- | --- |
| `search <query...>` | Ported | Searches a generation and prints matching text, claim evidence, and supported entities. |
| `search --mode` | Kept | Accepts `hybrid`, `text`, `vector`, and `graph`. Vector retrieval uses claim embeddings. |
| `search --k` | Dropped | The generation search selects candidates for each channel. |
| `search --top` | Kept | Limits results. The rebuild default is 10. |
| `search --page` | Dropped | The rebuild returns the top documents in one response. |
| `search --chunks` | Dropped | Results are ranked by source document. |
| `search --expand` | Replaced by `show <document>` | Reads the complete stored source. |
| `search --full` | Replaced by `search --json` or `show <document>` | JSON carries the matched segment or quote. |
| `search --rerank` | Dropped | The rebuild uses rank fusion over its retrieval channels. |
| `search --no-graph` | Dropped | The old flag controlled expansion after retrieval. Results now list supported entities. |
| `search --json` | Kept | Returns a JSON object with `hits`. Hit fields describe sources. |
| `walk <name...>` | Ported | Takes a quoted name and returns supported incoming and outgoing claims. |
| `walk --to` | Dropped | The old path traversed `ASSOCIATED_WITH`. |
| `walk --hops` | Dropped | The generation walk reports direct claims with evidence. |
| `walk --top` | Dropped | There is no ranked neighborhood in the generation walk. |
| `walk --docs` | Dropped | Entity records include source IDs. |
| `walk --json` | Kept | The rebuild walk always prints JSON. |
| `query` | Dropped | The rebuild exposes generation readers. The Helix SDK remains available for ad hoc queries. |
| `query --cypher` | Dropped | The CLI no longer runs arbitrary queries. |
| `query --ts` | Dropped | The CLI no longer evaluates SDK expressions. |
| `query --file` | Dropped | The CLI no longer submits raw query requests. |
| `query --param` | Dropped | The query command was removed. |
| `query --explain` | Dropped | The query command was removed. |
| `query --write` | Dropped | Generations publish through `activate`. |
| `schema` | Replaced by `ontology` and `stats` | These show mapping rules and generation counts. |
| `docs` | Kept | Lists source documents from the generation. |
| `docs --limit` | Dropped | The rebuild lists all matching documents. |
| `docs --json` | Kept | The rebuild always prints JSON. |
| `show <documentId>` | Kept | Prints the complete original source. |
| `entities` | Kept | Lists supported mapped entities. |
| `entities --entity-kind` | Dropped | Rows report ontology types in place of main's fixed entity kinds. |
| `entities --match` | Kept | Resolves an exact name or alias. Main used a substring filter. |
| `entities --top` | Dropped | The rebuild prints matching entities without frequency ranking. |
| `entities --json` | Kept | The rebuild always prints JSON. |
| `entities --index-names` | Replaced by `map` | Generation construction writes the alias index. |
| `entities --resolve` | Replaced by `map` or `ingest` | Generation construction reviews candidate identities and replays supported decisions. |
| `entities --limit` | Dropped | The standalone identity review command was removed. |
| `entities --yes` | Replaced by `activate` or `ingest` | Publish a generation to make its identity decisions active. |
| `categories` | Dropped | The rebuild source model has no legacy passage labels. |
| `categories --top` | Dropped | The category tally was removed. |
| `categories --json` | Dropped | The category tally was removed. |
| `--kind` | Kept on `search` and `docs` | Filters by source kind. |
| `--source` | Kept on `search` and `docs` | Filters by source origin. |
| `--doc` | Kept on `search` and `docs` | Filters by document ID. |
| `--category` | Dropped | The Jev export carries bookmark labels. The current `Source` schema does not store them. |
| `--status` | Replaced by `stats --generation` | Status applies to a generation. |
| `--mine` | Dropped | The rebuild has no owner-passage labels. |
| `--with-boilerplate` | Dropped | The rebuild stores and indexes the source text. |
| `--min-salience` | Dropped | The Jev export carries bookmark signal. The current `Source` schema does not store it. |
| `--where` | Dropped | The rebuild accepts named source filters. |
| `--entity` | Kept on `search` | Filters by sources with supported occurrences of one resolved name or alias. Main allowed repeated flags. |
| `--all-entities` | Dropped | The rebuild does not expose an AND filter over entities. |
| `ingest <dir>` | Ported | Extracts and maps source JSON, then activates the merged corpus. |
| `ingest --kind` | Replaced by source JSON `kind` | Kind is a property of each source. |
| `ingest --replace` | Kept | Allows updates to existing document IDs with changed text. |
| `link` | Replaced by `extract` and `map` | Extraction retains observations and mapping applies ontology rules. |
| `link --kind` | Replaced by the input directory | The caller selects source files before extraction. |
| `link --limit` | Replaced by the input directory | The caller selects the number of source files. |
| `link --dry-run` | Dropped | The legacy linking stage was removed. |
| `link --redo` | Replaced by `rebalance` | Builds a new generation from a complete corpus. |
| `name` | Replaced by `extract` and `map` | Extraction observes entities and aliases. Mapping assigns supported types. |
| `name --kind` | Replaced by the input directory | The caller selects source files before extraction. |
| `name --limit` | Replaced by the input directory | The caller selects the number of source files. |
| `name --dry-run` | Dropped | The legacy naming stage was removed. |
| `bookmarks [jsonl]` | Pending | Needs a source-only publication design. |
| `bookmarks --labels` | Pending | The export has Jev labels. The source schema needs fields to retain them. |
| `bookmarks --skip` | Pending | The tombstone file excludes deleted bookmarks. Activation currently rejects any missing active ID. |
| `bookmarks --ftdb` | Pending | The Field Theory database supplies full article text and other post data. |
| `bookmarks --replace` | Pending | Needs a source-only publication design. |
| `bookmarks --graph` | Pending | Article extraction and review require model access or saved artifacts. |
| `bookmarks --redo` | Pending | Needs a decision on article revisions and saved extractions. |
| `bookmarks --limit` | Pending | Main limits the article graph pass. |
| `bookmarks --dry-run` | Pending | Must read the real export without writing to Helix. |
| `backup [dir]` | Dropped | Finished generations retain records, rules, and review decisions in Helix. Main's portable export has no rebuild equivalent. |
| `check` | Replaced by validation in `activate` | Activation verifies corpus identity, projection counts, supported claims, and source coverage before publication. |
| `dedupe` | Dropped | Corpus selection handles duplicate documents before building. Identity review in `map` handles entities. |
| `dedupe --yes` | Dropped | The rebuild does not delete documents in place. |
| `forget <file>` | Dropped | The rebuild requires the next active generation to retain existing document IDs. |
| `forget --yes` | Dropped | The removal command has no generation equivalent. |
| `prune` | Dropped | Mapping projects supported facts into a new generation. |
| `prune --yes` | Dropped | The rebuild does not prune the active graph in place. |
| `model` | Replaced by `ontology draft`, `ontology revise`, and `map` | These draft and compare rules and build supported mapped claims. |
| `model --yes` | Replaced by `activate` | Publication is separate from mapping. |
| `stats` | Kept | Prints counts and review results for the generation. |
| `profile` | Dropped | The rebuild does not construct an owner profile. |
| `taxonomy` | Replaced by `ontology` | Prints the rules for mapped types and relations. |
| `taxonomy --seed` | Replaced by `ontology draft` | Drafts rules from retained observations. |
| `taxonomy --yes` | Replaced by `map` and `activate` | Map writes a candidate. Activate makes it current. |
| `--help` | Kept | Effect CLI prints commands and flags. |
| `--version` | Kept | The rebuild reports version 3.0.0. |
