#!/usr/bin/env python3
"""Run the finite Everystate proofs and retain counterexamples for removed guards."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
REVISION = 'c1544c8dc536fb3460badab96db354f1165700ed'
if len(sys.argv) != 2:
    raise SystemExit('Usage: python3 scripts/prove.py /path/to/everystate-source')
source = Path(sys.argv[1]).resolve()
revision = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
if revision != REVISION:
    raise SystemExit(f'Proof engine source must be reviewed revision {REVISION}; found {revision}')
out = ROOT / '.audit' / 'proofs'
out.mkdir(parents=True, exist_ok=True)
binary = os.environ.get('EVERYSTATE_BINARY')
if not binary:
    target = out / 'engine'
    subprocess.run(['cargo', 'build', '--locked', '--release', '--bin', 'everystate', '--manifest-path', str(source / 'Cargo.toml'), '--target-dir', str(target)], check=True)
    binary = str(target / 'release' / 'everystate')
env = dict(os.environ)
env.pop('EVERYSTATE_DEBUG_ASSUMES', None)
checks = []
for spec in ['Publication', 'Generation', 'Revision', 'Mapping']:
    checks.extend([(spec, spec, 0, None), (spec, spec + 'Broken', 12, {
        'Publication': 'KeepsPublished', 'Generation': 'ActiveComplete',
        'Revision': 'StaleNeverCompared', 'Mapping': 'SupportedClaimsOnly',
    }[spec])])
    if spec != 'Revision':
        checks.append((spec, spec + 'Reachable', 12, 'NeverPublishes'))
checks.append(("Publication", "PublicationCoverageBroken", 12, "KeepsPublished"))
results = []
for spec, config, expected, invariant in checks:
    cfg = ROOT / 'proofs' / (config + '.cfg')
    for line in cfg.read_text().splitlines():
        if line.strip().startswith(('CONSTRAINT ', 'ACTION_CONSTRAINT ', 'VIEW ', 'SYMMETRY ')):
            raise SystemExit(f'{config}: state pruning is not allowed in these proofs')
    command = [binary, '-workers', '1', '-json', '-config', str(cfg), str(ROOT / 'proofs' / (spec + '.tla'))]
    run = subprocess.run(command, text=True, capture_output=True, env=env)
    (out / (config + '.json')).write_text(run.stdout)
    if run.stderr:
        (out / (config + '.stderr')).write_text(run.stderr)
    result = json.loads(run.stdout.splitlines()[-1])
    valid = run.returncode == expected
    if expected == 0:
        valid = valid and result.get('status') == 'ok' and result.get('distinct', 0) > 0 and result.get('queue') == 0
    else:
        valid = valid and result.get('status') == 'invariant' and result.get('name') == invariant and bool(result.get('trace'))
    results.append({'spec': spec, 'config': config, 'exit': run.returncode, 'accepted': valid,
                    'status': result.get('status'), 'invariant': result.get('name'),
                    'generated': result.get('generated'), 'distinct': result.get('distinct'), 'queue': result.get('queue')})
    print(config, 'PASS' if valid else 'FAILED', result.get('status'), result.get('name', ''), flush=True)
    if not valid:
        raise SystemExit(f'Unexpected proof result for {config}; inspect {out}')
paths = [*sorted((ROOT / 'proofs').glob('*.tla')), *sorted((ROOT / 'proofs').glob('*.cfg')),
         *(ROOT / 'src' / name for name in ['graph.ts', 'knowledge.ts', 'extraction.ts', 'cli.ts', 'helix.ts', 'ai.ts'])]
manifest = {'engineRevision': revision, 'engineBinarySha256': hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
            'head': subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip(),
            'files': {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
            'results': results,
            'limits': 'Finite safety models only. No semantic AI accuracy, arbitrary-input code correctness, storage durability, hash collision freedom or full-corpus rebuild is proved.'}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
