#!/usr/bin/env python3
"""Rebuild a legacy Helix v1 Chunk.embedding index from stored embeddings.

Drops and recreates chunks, including their HAS_CHUNK edges. Extra chunk
properties and other incident edges are not preserved. This uses the legacy
/v1/query protocol, not the current kb.ts /v2/query protocol.
"""
import argparse,json,urllib.request,time,sys,os
from concurrent.futures import ThreadPoolExecutor
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--test", action="store_true", help="Read the first document without writing to Helix.")
parser.parse_args()
H=os.environ.get("HELIX_REINDEX_URL", "http://localhost:6971/v1/query"); T=os.environ.get("KB_TENANT", "demo")
V=lambda v:{"Value":v}; Q=lambda n,s:{"Query":{"name":n,"condition":None,"steps":s}}
log_path = os.path.expanduser(os.environ.get("HELIX_REINDEX_LOG", "./data/reindex.log"))
os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
LOG=open(log_path,"a")
def log(m):
    line="[%s] %s"%(time.strftime("%H:%M:%S"),m); print(line,flush=True); LOG.write(line+"\n"); LOG.flush()
def send(rt,qs,rets,timeout=300):
    body=json.dumps({"parameters":{},"request_type":rt,"query":{"queries":qs,"returns":rets}}).encode()
    headers={"Content-Type":"application/json","x-helix-await-durable":"true"}
    if os.environ.get("HELIX_API_KEY"): headers["Authorization"]="Bearer "+os.environ["HELIX_API_KEY"]
    for a in range(8):
        try: return json.loads(urllib.request.urlopen(urllib.request.Request(H,data=body,headers=headers),timeout=timeout).read())
        except urllib.error.HTTPError as e:
            msg=e.read().decode()[:200]
            if e.code in (409,408) and a<7: time.sleep(0.3*(a+1)); continue
            raise RuntimeError("HTTP %d %s"%(e.code,msg))
def rows(d):
    return d.get("properties",[]) if isinstance(d,dict) else (d or [])
DOC=lambda did:[{"NWhere":{"Eq":["$label",{"String":"SourceDocument"}]}},{"Has":["documentId",{"String":did}]}]
CH=lambda did:[{"NWhere":{"Eq":["$label",{"String":"Chunk"}]}},{"Has":["documentId",{"String":did}]}]
docs=rows(send("read",[Q("d",[{"NWhere":{"Eq":["$label",{"String":"SourceDocument"}]}},{"Project":[{"source":"documentId","alias":"d"},{"source":"title","alias":"t"},{"source":"chunkCount","alias":"n"}]}])],["d"])["d"])
log("%d documents"%len(docs))
def fetch(doc):
    did=doc["d"]
    cs=rows(send("read",[Q("c",CH(did)+[{"Project":[{"source":"chunkId","alias":"cid"},{"source":"chunkIndex","alias":"i"},{"source":"content","alias":"c"},{"source":"embedding","alias":"e"}]}])],["c"])["c"])
    return did,doc.get("t",""),[c for c in cs if isinstance(c.get("e"),list) and len(c["e"])==1536]
t0=time.time(); data={}
if "--test" in sys.argv:
    did,t,cs=fetch(docs[0]); print(did,t[:40],len(cs),"chunks; first has",len(cs[0]["e"]) if cs else 0,"dims; keys",list(cs[0].keys()) if cs else None); sys.exit(0)
with ThreadPoolExecutor(6) as ex:
    for k,(did,t,cs) in enumerate(ex.map(fetch,docs)):
        data[did]=(t,cs)
        if k%200==0: log("read %d/%d docs (%d chunks) %.0fs"%(k,len(docs),sum(len(v[1]) for v in data.values()),time.time()-t0))
total=sum(len(v[1]) for v in data.values()); log("read done: %d chunks with embeddings in %.0fs"%(total,time.time()-t0))
SPEC={"NodeVector":{"label":"Chunk","property":"embedding","dimension":1536,"metric":"cosine","tenant_property":"tenant_id"}}
send("write",[Q("x",[{"DropIndex":{"spec":SPEC}}])],["x"]); log("vector index dropped")
send("write",[Q("i",[{"CreateIndex":{"spec":SPEC,"if_not_exists":True}}])],["i"]); log("vector index recreated (empty)")
done=0; failed=0
for k,(did,(title,cs)) in enumerate(data.items()):
    try:
        send("write",[Q("x",CH(did)+["Drop"])],["x"])
        for s in range(0,len(cs),24):
            qs=[];rets=[]
            for c in cs[s:s+24]:
                i=int(c["i"]); cn="c%d"%i
                qs.append(Q(cn,[{"AddN":{"label":"Chunk","properties":[["tenant_id",V({"String":T})],["chunkId",V({"String":c["cid"]})],["documentId",V({"String":did})],["title",V({"String":title})],["chunkIndex",V({"I64":i})],["content",V({"String":c["c"]})],["embedding",V({"F64Array":c["e"]})]]}}])); rets.append(cn)
                qs.append(Q("e%d"%i,DOC(did)+[{"AddE":{"label":"HAS_CHUNK","to":{"Var":cn},"properties":[["tenant_id",V({"String":T})]]}}])); rets.append("e%d"%i)
            send("write",qs,rets)
        done+=len(cs)
    except Exception as e:
        failed+=1; log("FAILED %s: %s"%(did,str(e)[:150]))
    if k%100==0: log("rewrote %d/%d docs, %d/%d chunks, %d failed, %.0fs"%(k,len(data),done,total,failed,time.time()-t0))
log("REINDEX DONE: %d chunks rewritten, %d docs failed, %.1f min"%(done,failed,(time.time()-t0)/60))
