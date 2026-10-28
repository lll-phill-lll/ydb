#!/usr/bin/env python3
"""Dependency adequacy of a built gn out dir: what every executable declares
(the closure of its gn deps) against what it needs (the objects reachable from
its own objects by strong-symbol references, like a static linker pulling
archive members).

    dep_audit.py --baseline <out> [--targets all tests] [--report <dir>] [-j 8]
                 [--roots all|tests|programs|<glob>] [--cut '<root glob>=<target glob>']
                 [--top 30] [--reuse]

First checks that the build dir is fit: a build of this source tree, nothing
building there, ninja -n --targets has no stale manifest and no C++ object to
compile again. The roots are the executables built and up to date there; the
others are left out (their closures may lack objects).

Read-only over the out dir: ninja -n, gn desc, obj/**/*.ninja, .ninja_log,
.ninja_deps, llvm-nm of the objects. Writes only into --report (default
<out>/dep_audit). Exit code: 0 if the build dir is fit, else 1.

Model
  w(T)    weight of a target: sum of its jobs' durations in .ninja_log, seconds
  D(R)    declared closure of a root executable R over gn deps (public included)
  N(R)    needed: objects reachable from R's objects and from always-linked
          objects (GLOBAL SRCS of ya.make, YQL UDF modules, *_resources.cpp) of
          D(R), over strong-symbol references (U/w/v -> T/D/B/R/C/G/S/i),
          providers restricted to D(R); a target is needed if one of its
          objects is; a target without objects (action, group, header-only) is
          needed if a needed target depends on it or uses headers of its dir;
          the deps of a needed action or tool executable are needed whole
          (to build, not to link)
  Adequacy(R) = w(N) / w(D); Ballast(R) = w(D) - w(N)
  head    a node v of the dominator tree of D(R) whose subtree is entirely
          unneeded while its immediate dominator's is not: dropping v drops
          the whole subtree; the only way in is v's in-edges ("entry edges")
  Carry   weight of a head's subtree
  Effort  of an edge P->Y: |U_sym| + |U_hdr| -- strong symbols of Y referenced
          by P's objects, headers of Y's dir in P's TUs (.ninja_deps)
  Score   Carry summed over roots / (1 + Effort): build seconds per edit
"""

import argparse
import array
import collections
import fnmatch
import json
import os
import pickle
import re
import shlex
import struct
import subprocess
import sys
import time
import types
from concurrent.futures import ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pch_update import running_ninja  # noqa: E402

_ROOT = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
_STATUS_RE = re.compile(r"^\[(\d+)/(\d+)\] (.*)$")
T0 = time.time()


def log(msg):
    sys.stderr.write("[%6.1fs] %s\n" % (time.time() - T0, msg))
    sys.stderr.flush()


# ---- config of the out dir ---------------------------------------------------

Cfg = types.SimpleNamespace


def read_cfg(out):
    cfg = Cfg()
    cfg.out = os.path.abspath(out)
    gn_cmd = None
    with open(os.path.join(cfg.out, "build.ninja")) as f:
        lines = f.read().splitlines()
    for i, line in enumerate(lines):
        if line.strip() == "rule gn":
            for l in lines[i + 1:i + 5]:
                l = l.strip()
                if l.startswith("command ="):
                    gn_cmd = shlex.split(l[len("command ="):])
            break
    if not gn_cmd:
        sys.exit("dep_audit: no 'rule gn' in build.ninja")
    cfg.gn = os.path.normpath(os.path.join(cfg.out, gn_cmd[0]))
    cfg.gn_flags = [a for a in gn_cmd[1:] if a.startswith("--script-executable")]
    cfg.root = cfg.out
    for a in gn_cmd[1:]:
        if a.startswith("--root="):
            cfg.root = os.path.normpath(os.path.join(cfg.out, a[len("--root="):]))
    args_gn = open(os.path.join(cfg.out, "args.gn")).read()

    def arg(name):
        m = re.search(r'^\s*%s\s*=\s*"?([^"\n]*)"?' % name, args_gn, re.M)
        return m.group(1).strip() if m else None

    cfg.pch_mode = arg("pch_mode") or "?"
    clang_bin = arg("clang_bin_path")
    cfg.nm = None
    for c in ([os.path.join(clang_bin, "llvm-nm")] if clang_bin else []) + ["llvm-nm"]:
        if os.path.isabs(c) and os.path.exists(c):
            cfg.nm = c
            break
    if cfg.nm is None:
        sys.exit("dep_audit: llvm-nm not found (clang_bin_path in args.gn)")
    cfg.cxxfilt = os.path.join(os.path.dirname(cfg.nm), "llvm-cxxfilt")
    return cfg


# ---- fitness of the build dir ----------------------------------------------------

def check(cfg, targets):
    """(errors, warnings, outputs ninja -n would build again)."""
    errors, warnings = [], []
    if os.path.realpath(cfg.root) != _ROOT:
        errors.append("a build of %s, not of %s" % (os.path.realpath(cfg.root), _ROOT))
    busy = running_ninja(os.path.realpath(cfg.out))
    if busy:
        errors.append("ninja is running there (pid %s)" % ", ".join(map(str, busy)))
    log("ninja -n %s ..." % " ".join(targets))
    env = dict(os.environ, NINJA_STATUS="[%f/%t] ")
    proc = subprocess.run(["ninja", "-C", cfg.out, "-n"] + targets, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, universal_newlines=True)
    if proc.returncode != 0:
        errors.append("ninja -n failed:\n%s" % proc.stdout[-4000:])
        return errors, warnings, set()
    descs = [m.group(3) for m in map(_STATUS_RE.match, proc.stdout.splitlines()) if m]
    if "Regenerating ninja files" in descs:
        errors.append("the ninja files are stale (gn files changed since the build)")
    again = set()
    kinds = collections.Counter()
    for d in descs:
        kind, _, rest = d.partition(" ")
        kinds[kind] += 1
        if rest:
            again.add(os.path.normpath(rest.split()[0]))
    cxx = [d for d in descs if d.startswith(("CXX ", "CC ", "ASM "))]
    if cxx:
        errors.append("%d C++ objects out of date (ninja -n would compile them again), e.g. %s"
                      % (len(cxx), cxx[0].split(" ", 1)[1]))
    other = sum(n for k, n in kinds.items() if k not in ("CXX", "CC", "ASM"))
    if other:
        warnings.append("ninja -n would also run %d other commands (%s)" % (
            other, ", ".join("%s %d" % kv for kv in sorted(kinds.items()) if kv[0] not in ("CXX", "CC", "ASM"))))
    return errors, warnings, again


# ---- graph: gn desc ------------------------------------------------------------

def load_desc(cfg, report, reuse):
    path = os.path.join(report, "desc.json")
    if not (reuse and os.path.exists(path)):
        log("gn desc //* ...")
        with open(path + ".tmp", "w") as f:
            subprocess.run([cfg.gn, "desc", "--root=" + cfg.root] + cfg.gn_flags +
                           [cfg.out, "//*", "--format=json"], stdout=f, check=True, cwd=cfg.root)
        os.replace(path + ".tmp", path)
    with open(path) as f:
        desc = json.load(f)
    log("gn desc: %d targets" % len(desc))
    return desc


def label_dir(label):
    return label[2:].split("(")[0].split(":")[0]


# ---- ninja files: target -> objects, outputs -----------------------------------

class Target:
    __slots__ = ("label", "idx", "type", "testonly", "deps", "rdeps", "dir", "objs",
                 "outputs", "weight", "host", "is_so", "exe")

    def __init__(self, label, idx):
        self.label, self.idx = label, idx
        self.type, self.testonly, self.deps, self.rdeps = "?", False, [], []
        self.dir = label_dir(label)
        self.objs, self.outputs, self.weight = [], [], 0.0   # outputs: first of each build line
        self.host = "(" in label
        self.is_so = False
        self.exe = None   # an executable's file, relative to the out dir


_BUILD_RE = re.compile(r"^build (.*?)(?<!\$): (\S+)(.*)$")


def ninja_label(out, f, tc_by_name):
    rel = os.path.relpath(f, out)
    parts = rel.split(os.sep)
    tc = None
    if parts[0] != "obj":
        tc, parts = parts[0], parts[1:]
    if not parts or parts[0] != "obj":
        return None
    d = "/".join(parts[1:-1])
    name = parts[-1][:-len(".ninja")]
    label = "//%s:%s" % (d, name)
    if tc:
        if tc not in tc_by_name:
            return None
        label += "(%s)" % tc_by_name[tc]
    return label


def load_ninja_files(cfg, targets):
    tc_by_name = {}
    for l in targets:
        if "(" in l:
            tcl = l.split("(", 1)[1].rstrip(")")
            tc_by_name[tcl.split(":")[-1]] = tcl
    files = []
    for top in ["obj"] + sorted(tc_by_name):
        base = os.path.join(cfg.out, top) if top == "obj" else os.path.join(cfg.out, top, "obj")
        for dp, _, fs in os.walk(base):
            files.extend(os.path.join(dp, x) for x in fs if x.endswith(".ninja"))
    nobj = 0
    for f in files:
        label = ninja_label(cfg.out, f, tc_by_name)
        t = targets.get(label)
        if t is None:
            continue
        with open(f) as fh:
            for line in fh:
                if not line.startswith("build "):
                    continue
                m = _BUILD_RE.match(line.rstrip("\n"))
                if not m:
                    continue
                outs = [o for o in m.group(1).replace("$ ", "\0").split() if o != "|"]
                outs = [os.path.normpath(o.replace("\0", " ")) for o in outs]
                if not outs:
                    continue
                t.outputs.append(outs[0])   # one job per build line (.o+.dwo, .so+.TOC)
                t.is_so = t.is_so or any(o.endswith(".so") for o in outs)
                rule = m.group(2)
                if rule in ("cxx", "cc", "asm", "objc", "objcxx") and outs and outs[0].endswith(".o"):
                    ins = m.group(3).split("|")[0].split()
                    src = ins[0] if ins else ""
                    t.objs.append((outs[0], src))
                    nobj += 1
    log("ninja files: %d, objects %d" % (len(files), nobj))


def load_ninja_log(cfg):
    dur = {}
    with open(os.path.join(cfg.out, ".ninja_log"), errors="replace") as f:
        for line in f:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) >= 4:
                try:
                    dur[os.path.normpath(p[3])] = (int(p[1]) - int(p[0])) / 1000.0
                except ValueError:
                    pass
    log(".ninja_log: %d outputs" % len(dur))
    return dur


# ---- symbols: llvm-nm ------------------------------------------------------------

HELPER_SUFFIXES = ("_enum_objs", "_link_slot_interfaces", "_link_slot_check", "_resources")
PCH_PREFIX = "//build/gn/pch/global:"
BUILD_TIME_TYPES = ("action", "action_foreach", "copy", "generated_file")
STRONG = set("TDBRCGSi")
REFS = set("Uwv")


def nm_batch(args):
    nm, out, paths = args
    r = subprocess.run([nm, "-A", "-P", "--no-sort", "-g"] + paths, cwd=out,
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    res = {p: ([], []) for p in paths}
    for line in r.stdout.decode("utf-8", "replace").splitlines():
        head, sep, rest = line.partition(": ")
        if not sep:
            continue
        e = res.get(head)
        if e is None:
            continue
        f = rest.split(" ")
        if len(f) < 2:
            continue
        ty = f[1]
        if ty in STRONG:
            e[0].append(f[0])
        elif ty in REFS:
            e[1].append(f[0])
    return res


class SymTab:
    def __init__(self):
        self.ids, self.names = {}, []

    def id(self, s):
        i = self.ids.get(s)
        if i is None:
            i = self.ids[s] = len(self.names)
            self.names.append(s)
        return i


def load_symbols(cfg, objs, skip, report, jobs, reuse):
    """objs: [path]; skip: indexes not to read (host toolchain). Returns (symtab, defs[oi], refs[oi]) as arrays of sym ids.
    Cache: nm_cache.pickle in report, keyed by (mtime_ns, size)."""
    cache_path = os.path.join(report, "nm_cache.pickle")
    st = SymTab()
    cache = {}
    if reuse and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            c = pickle.load(f)
        st.names = c["names"]
        st.ids = {s: i for i, s in enumerate(st.names)}
        cache = c["objs"]
    stats = {}
    todo = []
    for i, p in enumerate(objs):
        if i in skip:
            stats[p] = None
            continue
        try:
            s = os.stat(os.path.join(cfg.out, p))
            stats[p] = (s.st_mtime_ns, s.st_size)
        except OSError:
            stats[p] = None
            continue
        e = cache.get(p)
        if e is None or e[0] != stats[p]:
            todo.append(p)
    log("nm: %d objects, %d cached" % (len(objs), len(objs) - len(todo)))
    batches = [(cfg.nm, cfg.out, todo[i:i + 100]) for i in range(0, len(todo), 100)]
    done = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for res in ex.map(nm_batch, batches):
            for p, (d, r) in res.items():
                cache[p] = (stats[p], array.array("I", (st.id(s) for s in d)).tobytes(),
                            array.array("I", (st.id(s) for s in r)).tobytes())
            done += 1
            if done % 40 == 0:
                log("nm: %d / %d batches" % (done, len(batches)))
    with open(cache_path + ".tmp", "wb") as f:
        pickle.dump({"names": st.names, "objs": cache}, f, protocol=4)
    os.replace(cache_path + ".tmp", cache_path)
    defs, refs = [], []
    for p in objs:
        e = cache.get(p)
        if e is None or stats.get(p) is None:
            defs.append(array.array("I"))
            refs.append(array.array("I"))
            continue
        a, b = array.array("I"), array.array("I")
        a.frombytes(e[1])
        b.frombytes(e[2])
        defs.append(a)
        refs.append(b)
    log("nm: %d symbols" % len(st.names))
    return st, defs, refs


# ---- headers: .ninja_deps ----------------------------------------------------------

def read_ninja_deps(cfg, wanted):
    """{output path: array of input path ids} for outputs in `wanted` and for
    .pch/.gch outputs; plus [path] by id."""
    with open(os.path.join(cfg.out, ".ninja_deps"), "rb") as f:
        data = f.read()
    magic = b"# ninjadeps\n"
    if not data.startswith(magic):
        sys.exit("dep_audit: bad .ninja_deps")
    version = struct.unpack_from("<i", data, len(magic))[0]
    head = 12 if version >= 4 else 8
    pos = len(magic) + 4
    paths, recs = [], {}
    n = len(data)
    while pos + 4 <= n:
        size = struct.unpack_from("<I", data, pos)[0]
        pos += 4
        is_deps = size >> 31
        size &= 0x7FFFFFFF
        if pos + size > n:
            break
        if is_deps:
            out_id = struct.unpack_from("<i", data, pos)[0]
            recs[out_id] = (pos + head, (size - head) // 4)
        else:
            paths.append(data[pos:pos + size - 4].rstrip(b"\0").decode("utf-8", "replace"))
        pos += size
    out = {}
    for oid, (start, cnt) in recs.items():
        if oid >= len(paths):
            continue
        p = os.path.normpath(paths[oid])
        if p in wanted or p.endswith(".pch") or p.endswith(".gch"):
            a = array.array("i")
            a.frombytes(data[start:start + 4 * cnt])
            out[p] = a
    log(".ninja_deps v%d: %d paths, %d records, %d used" % (version, len(paths), len(recs), len(out)))
    return out, paths


class Modules:
    """Header path (as in the deps log) -> its module dir: the nearest dir
    with a BUILD.gn; gen/<x> counts as <x>; files outside the tree: None."""

    def __init__(self, cfg):
        self.cfg, self.dir_cache, self.path_cache = cfg, {}, {}
        self.root_abs = cfg.root.rstrip("/") + "/"

    def rel(self, p):
        if os.path.isabs(p):
            if not p.startswith(self.root_abs):
                return None
            return p[len(self.root_abs):]
        a = os.path.normpath(os.path.join(self.cfg.out, p))
        if a.startswith(self.root_abs) and not a.startswith(self.cfg.out + "/"):
            return a[len(self.root_abs):]
        q = os.path.normpath(p)
        for pre in ("gen/",):
            if q.startswith(pre):
                return q[len(pre):]
        return None

    def module_of_dir(self, d):
        r = self.dir_cache.get(d)
        if r is None:
            x = d
            r = ""
            while x:
                if os.path.exists(os.path.join(self.cfg.root, x, "BUILD.gn")):
                    r = x
                    break
                x = os.path.dirname(x)
            self.dir_cache[d] = r
        return r or None

    def of(self, pid, path):
        r = self.path_cache.get(pid, False)
        if r is False:
            rel = self.rel(path)
            r = self.module_of_dir(os.path.dirname(rel)) if rel else None
            self.path_cache[pid] = r
        return r


# ---- always-linked objects ----------------------------------------------------------

_MACRO_RE = re.compile(r"\b([A-Z_][A-Z0-9_]*)\s*\(([^()]*)\)", re.S)
UDF_MACROS = {"YQL_UDF", "YQL_UDF_YDB", "YQL_UDF_CONTRIB"}


def ya_make_globals(root, d):
    """(all objects global?, set of GLOBAL source paths relative to d) from
    <d>/ya.make, every IF branch taken (conservative)."""
    try:
        text = open(os.path.join(root, d, "ya.make"), errors="replace").read()
    except OSError:
        return False, set()
    text = re.sub(r"#[^\n]*", "", text)
    udf, glob_files = False, set()
    for m in _MACRO_RE.finditer(text):
        name, args = m.group(1), m.group(2).split()
        if name in UDF_MACROS:
            udf = True
        elif name == "GLOBAL_SRCS":
            glob_files.update(args)
        elif name == "SRCS":
            for i, a in enumerate(args[:-1]):
                if a == "GLOBAL":
                    glob_files.add(args[i + 1])
    return udf, {os.path.normpath(x) for x in glob_files}


# ---- per-root analysis (fork workers read these globals) ---------------------------------

G = {}


def analyze_root(ri):
    g = G
    T = g["targets"]
    deps, rdeps = g["deps"], g["rdeps"]
    # D(R) and a topological order (DFS postorder reversed)
    order, seen, stack = [], {ri}, [(ri, 0)]
    while stack:
        v, i = stack[-1]
        if i < len(deps[v]):
            stack[-1] = (v, i + 1)
            u = deps[v][i]
            if u not in seen:
                seen.add(u)
                stack.append((u, 0))
        else:
            stack.pop()
            order.append(v)
    order.reverse()
    D = seen
    # needed objects
    obj_t, obj_refs, troots = g["obj_target"], g["obj_refs"], g["target_root_objs"]
    tobjs = g["target_objs"]
    start = list(tobjs[ri])
    for v in D:
        start.extend(troots[v])
    nobj = set(start)
    stack = list(nobj)
    while stack:
        o = stack.pop()
        for p in obj_refs[o]:
            if p not in nobj and obj_t[p] in D:
                nobj.add(p)
                stack.append(p)
    needed = {obj_t[o] for o in nobj}
    needed.add(ri)
    used_dirs = set()
    hdr = g["hdr_dirs"]
    for v in needed:
        used_dirs.update(hdr[v])
    build_time = set()   # deps of needed actions and tools: needed whole, to build, not to link
    for v in order:      # parents before children
        if v in build_time:
            needed.add(v)
        elif not tobjs[v] and (T[v].dir in used_dirs or any(u in needed for u in rdeps[v] if u in D)):
            needed.add(v)
        if v in needed and (T[v].type in BUILD_TIME_TYPES or (T[v].type == "executable" and v != ri)):
            build_time.update(deps[v])
    w = g["weight"]
    wD = sum(w[v] for v in D)
    wN = sum(w[v] for v in needed)
    wNo = sum(g["obj_weight"][o] for o in nobj) + sum(g["link_weight"][v] for v in needed)
    # dominator tree over the DAG D(R)
    idom, depth = {ri: ri}, {ri: 0}
    for v in order[1:]:
        cur = None
        for u in rdeps[v]:
            if u not in idom:
                continue
            if cur is None:
                cur = u
                continue
            a, b = cur, u
            while a != b:
                while depth[a] > depth[b]:
                    a = idom[a]
                while depth[b] > depth[a]:
                    b = idom[b]
                if a != b:
                    a, b = idom[a], idom[b]
            cur = a
        idom[v] = cur
        depth[v] = depth[cur] + 1
    sub_w = {v: w[v] for v in D}
    sub_need = {v: (v in needed) for v in D}
    children = collections.defaultdict(list)
    for v in reversed(order[1:]):
        p = idom[v]
        children[p].append(v)
        sub_w[p] += sub_w[v]
        sub_need[p] = sub_need[p] or sub_need[v]
    heads = []
    for v in order[1:]:
        if not sub_need[v] and sub_need[idom[v]]:
            heads.append(v)
    heads.sort(key=lambda v: -sub_w[v])
    out_heads = []
    for v in heads:
        if sub_w[v] < g["min_carry"]:
            break
        sub, st = [], [v]
        while st:
            x = st.pop()
            sub.append(x)
            st.extend(children[x])
        subset = set(sub)
        entries = []
        for p in rdeps[v]:
            if p in D:
                us = sum(len(g["usage_sym"][p].get(y, ())) for y in subset)
                dirs = {T[y].dir for y in subset}
                uh = sum(len(g["hdr_sets"][p].get(dd, ())) for dd in dirs if dd != T[p].dir)
                entries.append((p, us, uh))
        out_heads.append((v, sub_w[v], len(sub), entries))
    link_D = sum(1 for v in D if g["is_so"][v])
    link_N = sum(1 for v in needed if g["is_so"][v])
    return {
        "root": ri, "nD": len(D), "nN": len(needed), "wD": wD, "wN": wN, "wNobj": wNo,
        "soD": link_D, "soN": link_N, "heads": out_heads,
        "nobj": array.array("I", sorted(nobj)).tobytes(),
    }


# ---- min cut ----------------------------------------------------------------------

def min_cut(ri, sinks):
    """Edmonds-Karp on D(R): capacity of P->Y = 1 + Effort; sinks merged into
    one node. Returns cut edges [(P, Y)]."""
    g = G
    deps = g["deps"]
    D, st = {ri}, [ri]
    while st:
        v = st.pop()
        for u in deps[v]:
            if u not in D:
                D.add(u)
                st.append(u)
    SINK = -1
    cap = collections.defaultdict(int)
    adj = collections.defaultdict(set)
    T = g["targets"]
    for p in D:
        if p in sinks:
            continue
        for y in deps[p]:
            c = 1 + len(g["usage_sym"][p].get(y, ())) + (
                len(g["hdr_sets"][p].get(T[y].dir, ())) if T[y].dir != T[p].dir else 0)
            q = SINK if y in sinks else y
            cap[(p, q)] += c
            adj[p].add(q)
            adj[q].add(p)
    if not any(s in D for s in sinks):
        return None, 0
    flow = collections.defaultdict(int)
    total = 0
    while True:
        prev, qd = {ri: None}, collections.deque([ri])
        while qd and SINK not in prev:
            x = qd.popleft()
            for y in adj[x]:
                if y not in prev and cap[(x, y)] - flow[(x, y)] > 0:
                    prev[y] = x
                    qd.append(y)
        if SINK not in prev:
            break
        path, y = [], SINK
        while prev[y] is not None:
            path.append((prev[y], y))
            y = prev[y]
        f = min(cap[e] - flow[e] for e in path)
        for a, b in path:
            flow[(a, b)] += f
            flow[(b, a)] -= f
        total += f
    reach = set(prev)
    cut = []
    for p in reach:
        for y in deps[p]:
            q = SINK if y in sinks else y
            if q not in reach and cap[(p, q)] > 0:
                cut.append((p, y))
    return cut, total


# ---- reporting helpers --------------------------------------------------------------

def demangle(cfg, names):
    if not names:
        return {}
    try:
        r = subprocess.run([cfg.cxxfilt], input="\n".join(names), stdout=subprocess.PIPE,
                           universal_newlines=True)
        outl = r.stdout.splitlines()
        if len(outl) == len(names):
            return dict(zip(names, outl))
    except OSError:
        pass
    return {n: n for n in names}


def fmt_s(x):
    if x >= 3600:
        return "%.1fh" % (x / 3600)
    if x >= 60:
        return "%.0fm" % (x / 60)
    return "%.0fs" % x


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", required=True, help="built gn out dir")
    ap.add_argument("--targets", nargs="+", default=["all", "tests"], help="what was built there")
    ap.add_argument("--report", help="output dir (default <out>/dep_audit)")
    ap.add_argument("-j", "--jobs", type=int, default=8)
    ap.add_argument("--roots", default="all",
                    help="all | tests | programs | glob of executable labels (default all)")
    ap.add_argument("--cut", action="append", default=[],
                    help="'<root glob>=<target glob>': cheapest edges to cut so the root "
                         "stops reaching the targets; repeatable")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--min-carry", type=float, default=1.0, help="seconds; smaller heads are not listed")
    ap.add_argument("--reuse", action="store_true", help="reuse desc.json and nm cache in --report")
    args = ap.parse_args()

    if not os.path.exists(os.path.join(args.baseline, "build.ninja")):
        sys.exit("dep_audit: %s is not a gn build dir (no build.ninja)" % args.baseline)
    cfg = read_cfg(args.baseline)
    log("out=%s root=%s pch_mode=%s nm=%s" % (cfg.out, cfg.root, cfg.pch_mode, cfg.nm))
    errors, warnings, again = check(cfg, args.targets)
    for w in warnings:
        print("warning: " + w)
    if errors:
        for e in errors:
            print("error: " + e)
        print("FAILED")
        sys.exit(1)
    report = os.path.abspath(args.report or os.path.join(cfg.out, "dep_audit"))
    os.makedirs(report, exist_ok=True)

    desc = load_desc(cfg, report, args.reuse)
    labels = sorted(desc)
    targets = {}
    for i, l in enumerate(labels):
        t = Target(l, i)
        d = desc[l]
        t.type, t.testonly = d.get("type", "?"), bool(d.get("testonly"))
        if t.type == "executable" and d.get("outputs"):
            o = d["outputs"][0]   # source-absolute "//..." in the tree, system-absolute "/..." outside
            t.exe = os.path.relpath(os.path.join(cfg.root, o[2:]) if o.startswith("//") else o, cfg.out)
        targets[l] = t
    T = [targets[l] for l in labels]
    load_ninja_files(cfg, targets)
    dur = load_ninja_log(cfg)

    # helper targets of templates join their owner; PCH targets leave the graph
    owner = list(range(len(T)))
    n_helpers = 0
    for t in T:
        d, _, rest = t.label.partition(":")
        name, tc = (rest.split("(", 1) + [""])[:2]
        for suf in HELPER_SUFFIXES:
            if name.endswith(suf):
                o = targets.get("%s:%s%s" % (d, name[:-len(suf)], "(" + tc if tc else ""))
                if o is not None:
                    owner[t.idx] = o.idx
                    n_helpers += 1
                break
    pch_objs = set()
    for t in T:
        if t.label.startswith(PCH_PREFIX):
            pch_objs.update(o for o, _ in t.objs)
            t.objs = []
    for l in labels:
        t = targets[l]
        o = T[owner[t.idx]]
        o.deps.extend(targets[x].idx for x in desc[l].get("deps", [])
                      if x in targets and not x.startswith(PCH_PREFIX))
        if o is not t:
            o.objs.extend(t.objs)
            o.outputs.extend(t.outputs)
            o.is_so = o.is_so or t.is_so
            t.objs, t.outputs, t.type = [], [], "helper"
    del desc
    for t in T:
        t.deps = sorted({owner[u] for u in t.deps} - {t.idx}) if t.type != "helper" else []
        for u in t.deps:
            T[u].rdeps.append(t.idx)
    log("helper targets joined to their owners: %d; PCH targets dropped from the graph" % n_helpers)

    # objects, weights
    obj_paths, obj_target, obj_src = [], [], []
    target_objs = [[] for _ in T]
    for t in T:
        for o, src in t.objs:
            target_objs[t.idx].append(len(obj_paths))
            obj_paths.append(o)
            obj_target.append(t.idx)
            obj_src.append(src)
    obj_weight = [dur.get(o, 0.0) for o in obj_paths]
    objset = set(obj_paths)
    weight, link_weight, is_so = [], [], []
    for t in T:
        w = sum(dur.get(o, 0.0) for o in t.outputs)
        weight.append(w)
        link_weight.append(w - sum(obj_weight[o] for o in target_objs[t.idx]))
        is_so.append(t.is_so)
    cxx = [obj_weight[i] for i in range(len(obj_paths)) if obj_paths[i] in dur]
    fast_share = (sum(1 for x in cxx if x < 0.2) / len(cxx)) if cxx else 0.0
    log("weights: total %s, objects with a log entry %d of %d, under 0.2 s: %.0f%%" % (
        fmt_s(sum(weight)), len(cxx), len(obj_paths), 100 * fast_share))

    # symbols
    st, defs, refs = load_symbols(cfg, obj_paths, {i for i, t in enumerate(obj_target) if T[t].host},
                                  report, args.jobs, args.reuse)
    prov = {}
    multi = {}
    for oi, a in enumerate(defs):
        if T[obj_target[oi]].host:
            continue
        for s in a:
            p = prov.get(s)
            if p is None:
                prov[s] = oi
            else:
                multi.setdefault(s, [p]).append(oi)
    obj_refs = []
    usage_sym = [dict() for _ in T]  # A -> {B: set(sym)}
    obj_sym_targets = []
    nref = nres = 0
    for oi, a in enumerate(refs):
        ta = obj_target[oi]
        ps, tg = set(), set()
        if not T[ta].host:
            ua = usage_sym[ta]
            for s in a:
                nref += 1
                p = prov.get(s)
                if p is None:
                    continue
                nres += 1
                cand = multi.get(s, (p,))
                for q in cand:
                    if q == oi:
                        continue
                    ps.add(q)
                    tb = obj_target[q]
                    if tb != ta:
                        tg.add(tb)
                        ua.setdefault(tb, set()).add(s)
        obj_refs.append(array.array("I", sorted(ps)))
        obj_sym_targets.append(tg)
    del defs, refs
    log("symbols: %d strong defs, %d defined in several objects; refs %d, resolved %d (%.0f%%)" % (
        len(prov), len(multi), nref, nres, 100.0 * nres / max(1, nref)))

    # headers
    mods = Modules(cfg)
    deps_recs, dpaths = read_ninja_deps(cfg, objset | pch_objs)
    pch = []
    for p, ids in deps_recs.items():
        if p in pch_objs or p.endswith(".pch") or p.endswith(".gch"):
            s = set(ids)
            prim = {i for i in s if "build/gn/pch/" in dpaths[i]}
            pch.append((prim, s))
    hdr_sets = [dict() for _ in T]  # A -> {dir: set(path id)}
    hdr_dirs = [set() for _ in T]
    obj_hdr_dirs = []
    pch_cut = 0
    n_with_deps = 0
    for oi, o in enumerate(obj_paths):
        ids = deps_recs.get(o)
        ta = obj_target[oi]
        dirs = set()
        if ids is not None and not T[ta].host:
            n_with_deps += 1
            s = set(ids)
            for prim, ps in pch:
                if prim & s:
                    s -= ps
                    pch_cut += 1
            hs = hdr_sets[ta]
            own = T[ta].dir
            for pid in s:
                path = dpaths[pid]
                m = mods.of(pid, path)
                if m is None:
                    continue
                dirs.add(m)
                if m != own:
                    hs.setdefault(m, set()).add(pid)
        hdr_dirs[ta] |= dirs
        obj_hdr_dirs.append(dirs)
    del deps_recs
    log("headers: %d of %d objects with deps; PCH subtracted in %d" % (n_with_deps, len(obj_paths), pch_cut))
    hdr_lower_bound = pch_cut > 0

    # always-linked objects
    target_root_objs = [[] for _ in T]
    n_glob = 0
    ya_cache = {}
    for t in T:
        if t.host or not target_objs[t.idx]:
            continue
        if t.dir not in ya_cache:
            ya_cache[t.dir] = ya_make_globals(cfg.root, t.dir)
        udf, gfiles = ya_cache[t.dir]
        for oi in target_objs[t.idx]:
            src = obj_src[oi]
            rel = mods.rel(src) or src
            local = os.path.relpath(rel, t.dir) if rel.startswith(t.dir + "/") else rel
            if udf or local in gfiles or os.path.basename(local) in gfiles or rel.endswith("_resources.cpp"):
                target_root_objs[t.idx].append(oi)
                n_glob += 1
    log("always-linked objects: %d" % n_glob)

    # roots: the executables built and up to date
    all_exes = [t for t in T if t.type == "executable" and not t.host]
    exes = [t for t in all_exes if t.exe and os.path.exists(os.path.join(cfg.out, t.exe))
            and os.path.normpath(t.exe) not in again]
    if len(exes) < len(all_exes):
        left = sorted(t.label for t in all_exes if t not in exes)
        warnings.append("%d executables not built or out of date, left out of the roots, e.g. %s"
                        % (len(left), ", ".join(left[:5])))
    missing_objs = [o for o, ti in zip(obj_paths, obj_target)
                    if not T[ti].host and not os.path.exists(os.path.join(cfg.out, o))]
    if missing_objs:
        warnings.append("%d objects not built, e.g. %s" % (len(missing_objs), missing_objs[0]))
    if args.roots == "tests":
        roots = [t for t in exes if t.testonly]
    elif args.roots == "programs":
        roots = [t for t in exes if not t.testonly]
    elif args.roots == "all":
        roots = exes
    else:
        roots = [t for t in exes if fnmatch.fnmatch(t.label, args.roots)]
    log("roots: %d (%d tests, %d programs)" % (
        len(roots), sum(1 for t in roots if t.testonly), sum(1 for t in roots if not t.testonly)))

    G.update(targets=T, deps=[t.deps for t in T], rdeps=[t.rdeps for t in T],
             obj_target=obj_target, obj_refs=obj_refs, target_objs=target_objs,
             target_root_objs=target_root_objs, hdr_dirs=hdr_dirs, hdr_sets=hdr_sets,
             usage_sym=usage_sym, weight=weight, obj_weight=obj_weight, link_weight=link_weight,
             is_so=is_so, min_carry=args.min_carry)

    results = []
    with ProcessPoolExecutor(max_workers=args.jobs) as ex:
        for k, r in enumerate(ex.map(analyze_root, [t.idx for t in roots], chunksize=4)):
            results.append(r)
            if (k + 1) % 100 == 0:
                log("roots: %d / %d" % (k + 1, len(roots)))
    log("roots analyzed")

    # ---- aggregate
    declared_any = set()
    obj_needed_any = set()
    head_agg = {}   # v -> [roots, sum carry, max carry, nodes, {P: (us, uh)}]
    edge_carry = collections.defaultdict(lambda: [0, 0.0])  # (P, v) -> [roots, sum carry]
    for r in results:
        a = array.array("I")
        a.frombytes(r["nobj"])
        obj_needed_any.update(a)
        for v, carry, n, entries in r["heads"]:
            h = head_agg.setdefault(v, [0, 0.0, 0.0, 0, {}])
            h[0] += 1
            h[1] += carry
            h[2] = max(h[2], carry)
            h[3] = max(h[3], n)
            for p, us, uh in entries:
                h[4][p] = (us, uh)
                ec = edge_carry[(p, v)]
                ec[0] += 1
                ec[1] += carry
    # union closures for the "all roots" numbers
    for t in roots:
        st_ = [t.idx]
        seen = {t.idx}
        while st_:
            x = st_.pop()
            for u in T[x].deps:
                if u not in seen:
                    seen.add(u)
                    st_.append(u)
        declared_any |= seen
    needed_targets_any = {obj_target[o] for o in obj_needed_any} | {t.idx for t in roots}

    # sample symbols for printing
    want = set()
    for t in T:
        for b, ss in usage_sym[t.idx].items():
            if len(ss) <= 3:
                want.update(ss)
    name_of = st.names
    dm = demangle(cfg, sorted({name_of[s] for s in want}))

    def sample(p, y, k=2):
        ss = sorted(usage_sym[p].get(y, ()))[:k]
        return "; ".join(dm.get(name_of[s], name_of[s])[:120] for s in ss)

    dir_targets = collections.defaultdict(list)
    for t in T:
        if not t.host:
            dir_targets[t.dir].append(t.idx)

    # ---- write tables
    def w_tsv(name, header, rows):
        with open(os.path.join(report, name), "w") as f:
            f.write("\t".join(header) + "\n")
            for row in rows:
                f.write("\t".join(str(x) for x in row) + "\n")

    res_by_root = sorted(results, key=lambda r: -(r["wD"] - r["wN"]))
    w_tsv("roots.tsv", ["root", "kind", "targets_declared", "targets_needed", "w_declared_s",
                        "w_needed_s", "w_needed_objects_s", "adequacy", "ballast_s", "so_declared", "so_needed"],
          [(T[r["root"]].label, "test" if T[r["root"]].testonly else "program", r["nD"], r["nN"],
            "%.1f" % r["wD"], "%.1f" % r["wN"], "%.1f" % r["wNobj"],
            "%.3f" % (r["wN"] / r["wD"] if r["wD"] else 1.0), "%.1f" % (r["wD"] - r["wN"]),
            r["soD"], r["soN"]) for r in res_by_root])

    with open(os.path.join(report, "ballast_by_root.tsv"), "w") as f:
        f.write("root\thead\tcarry_s\tsubtree_targets\tentry_edges(from:sym/hdr)\n")
        for r in res_by_root:
            for v, carry, n, entries in r["heads"][:args.top]:
                f.write("%s\t%s\t%.1f\t%d\t%s\n" % (T[r["root"]].label, T[v].label, carry, n,
                        " ".join("%s:%d/%d" % (T[p].label, us, uh) for p, us, uh in entries)))

    heads_sorted = sorted(head_agg.items(), key=lambda kv: -kv[1][1])
    w_tsv("heads.tsv", ["head", "roots", "sum_carry_s", "max_carry_s", "max_subtree_targets", "entry_edges(from:sym/hdr)"],
          [(T[v].label, h[0], "%.1f" % h[1], "%.1f" % h[2], h[3],
            " ".join("%s:%d/%d" % (T[p].label, us, uh) for p, (us, uh) in sorted(h[4].items(), key=lambda x: x[1])))
           for v, h in heads_sorted])

    edge_rows = []
    cls_count = collections.Counter()
    for t in T:
        if t.host or not target_objs[t.idx]:
            continue
        nobj = len(target_objs[t.idx])
        tus = collections.Counter()
        for oi in target_objs[t.idx]:
            used = set(obj_sym_targets[oi])
            for d in obj_hdr_dirs[oi]:
                used.update(dir_targets.get(d, ()))
            for b in used:
                tus[b] += 1
        for b in t.deps:
            B = T[b]
            us = len(usage_sym[t.idx].get(b, ()))
            uh = len(hdr_sets[t.idx].get(B.dir, ())) if B.dir != t.dir else -1
            if B.host or B.type in ("action", "action_foreach", "copy", "generated_file"):
                cls = "codegen/tool"
            elif us == 0 and uh <= 0:
                cls = "dead?" if hdr_lower_bound or uh < 0 else "dead"
            elif us == 0:
                cls = "header-only"
            elif us <= 3:
                cls = "thin"
            else:
                cls = "used"
            cls_count[cls] += 1
            ec = edge_carry.get((t.idx, b), [0, 0.0])
            score = ec[1] / (1 + us + max(uh, 0))
            edge_rows.append((score, t.label, B.label, cls, us, uh if uh >= 0 else "same-dir",
                              "%d/%d" % (tus.get(b, 0), nobj), ec[0], "%.1f" % ec[1], "%.2f" % score,
                              sample(t.idx, b) if 0 < us <= 3 else ""))
    edge_rows.sort(key=lambda r: -r[0])
    w_tsv("edges.tsv", ["from", "to", "class", "sym", "hdr", "tus_using", "ballast_roots", "ballast_sum_s",
                        "score", "symbols(sample)"], [r[1:] for r in edge_rows])

    # used without a direct edge (symbols)
    ind = []
    for t in T:
        dset = set(t.deps)
        for b, ss in usage_sym[t.idx].items():
            if b not in dset:
                ind.append((t.label, T[b].label, len(ss)))
    ind.sort(key=lambda r: -r[2])
    w_tsv("indirect_symbol_use.tsv", ["from", "to(not a direct dep)", "symbols"], ind)

    # fill: share of a target's objects needed by any root
    fill = []
    for t in T:
        ob = target_objs[t.idx]
        if not ob or t.host:
            continue
        wn = sum(obj_weight[o] for o in ob if o in obj_needed_any)
        wa = sum(obj_weight[o] for o in ob)
        nn = sum(1 for o in ob if o in obj_needed_any)
        if t.idx in declared_any:
            fill.append((wa - wn, t.label, nn, len(ob), "%.1f" % wn, "%.1f" % wa))
    fill.sort(key=lambda r: -r[0])
    w_tsv("fill.tsv", ["target", "objs_needed", "objs", "w_needed_s", "w_s"], [r[1:] for r in fill])

    # cuts
    cut_lines = []
    for spec in args.cut:
        rg, _, tg = spec.partition("=")
        sinks = {t.idx for t in T if fnmatch.fnmatch(t.label, tg)}
        for t in [x for x in exes if fnmatch.fnmatch(x.label, rg)]:
            cut, total = min_cut(t.idx, sinks)
            if cut is None:
                cut_lines.append("cut %s -> %s: not reachable" % (t.label, tg))
                continue
            cut_lines.append("cut %s -> %s: %d edges, effort %d" % (t.label, tg, len(cut), total - len(cut)))
            for p, y in sorted(cut, key=lambda e: T[e[0]].label):
                us = len(usage_sym[p].get(y, ()))
                uh = len(hdr_sets[p].get(T[y].dir, ())) if T[y].dir != T[p].dir else 0
                cut_lines.append("    %s -> %s  sym %d hdr %d  %s" % (T[p].label, T[y].label, us, uh,
                                                                     sample(p, y, 3) if us <= 3 else ""))
    with open(os.path.join(report, "cuts.txt"), "w") as f:
        f.write("\n".join(cut_lines) + "\n")

    # ---- summary
    wsum = lambda s: sum(weight[v] for v in s)
    adeq = sorted(r["wN"] / r["wD"] for r in results if r["wD"] > 0)
    S = []
    S.append("dep_audit  out=%s  pch_mode=%s  %s" % (cfg.out, cfg.pch_mode, time.strftime("%Y-%m-%d %H:%M")))
    S.append("")
    S.append("Inputs")
    S.extend("  warning: " + w for w in warnings)
    S.append("  targets %d, objects %d, executables %d (roots analyzed %d)" % (
        len(T), len(obj_paths), len(exes), len(roots)))
    S.append("  symbols resolved %.0f%% of refs; defined in several objects: %d" % (
        100.0 * nres / max(1, nref), len(multi)))
    S.append("  headers: %s" % ("PCH subtracted -> hdr counts are a lower bound, 'dead?' edges may use headers"
                                if hdr_lower_bound else "clean (no PCH in depfiles)"))
    S.append("  weights: total %s; %.0f%% of compiles under 0.2 s%s" % (
        fmt_s(sum(weight)), 100 * fast_share,
        " -> looks cached, weights unreliable" if fast_share > 0.3 else ""))
    S.append("")
    S.append("All roots together")
    S.append("  declared %d targets, %s; needed %d targets, %s; ballast %s (%.0f%%)" % (
        len(declared_any), fmt_s(wsum(declared_any)), len(needed_targets_any & declared_any),
        fmt_s(wsum(needed_targets_any & declared_any)),
        fmt_s(wsum(declared_any - needed_targets_any)),
        100.0 * wsum(declared_any - needed_targets_any) / max(1e-9, wsum(declared_any))))
    if adeq:
        S.append("  adequacy per root: median %.2f, p10 %.2f, p90 %.2f" % (
            adeq[len(adeq) // 2], adeq[len(adeq) // 10], adeq[len(adeq) * 9 // 10]))
    S.append("")
    S.append("Top roots by ballast (declared -> needed, targets / .so)")
    for r in res_by_root[:args.top]:
        S.append("  %-70s %6s -> %6s  adequacy %.2f  targets %d -> %d  .so %d -> %d" % (
            T[r["root"]].label, fmt_s(r["wD"]), fmt_s(r["wN"]), r["wN"] / r["wD"] if r["wD"] else 1,
            r["nD"], r["nN"], r["soD"], r["soN"]))
    S.append("")
    S.append("Top ballast heads (summed over roots): head  roots  sum  max  <- entry edges from:sym/hdr")
    for v, h in heads_sorted[:args.top]:
        ent = sorted(h[4].items(), key=lambda x: x[1])
        S.append("  %-60s %4d %7s %6s  <- %s" % (
            T[v].label, h[0], fmt_s(h[1]), fmt_s(h[2]),
            ", ".join("%s:%d/%d" % (T[p].label, us, uh) for p, (us, uh) in ent[:4]) +
            (" +%d" % (len(ent) - 4) if len(ent) > 4 else "")))
    S.append("")
    S.append("Top edges by score (ballast seconds per edit)")
    for r in edge_rows[:args.top]:
        if r[0] <= 0:
            break
        S.append("  %s -> %s  [%s] sym %s hdr %s  roots %s  ballast %s  score %s  %s" % (
            r[1], r[2], r[3], r[4], r[5], r[7], fmt_s(float(r[8])), r[9], r[10]))
    S.append("")
    S.append("Edge classes: " + ", ".join("%s %d" % kv for kv in sorted(cls_count.items())))
    S.append("Targets mostly unneeded by any root: see fill.tsv (top: %s)" % (
        ", ".join("%s %s/%s" % (r[1], r[2], r[3]) for r in fill[:5])))
    if cut_lines:
        S.append("")
        S.extend(cut_lines)
    S.append("")
    S.append("Files: roots.tsv heads.tsv ballast_by_root.tsv edges.tsv fill.tsv indirect_symbol_use.tsv cuts.txt")
    with open(os.path.join(report, "summary.txt"), "w") as f:
        f.write("\n".join(S) + "\n")
    print("\n".join(S))
    log("done, report in %s" % report)
    print("OK")


if __name__ == "__main__":
    main()
