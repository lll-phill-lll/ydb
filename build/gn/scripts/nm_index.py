"""Symbols of the objects of a gn out dir by llvm-nm, cached by (mtime, size) of
each object: shared by dep_audit.py and autofix.py."""

import array
import hashlib
import os
import pickle
import subprocess
from concurrent.futures import ProcessPoolExecutor

STRONG = set("TDBRCGSi")
WEAK = set("WVu")   # explicit template instantiations, inline functions, vtables of templates
REFS = set("U")
WEAK_REFS = set("wv")   # a link does not fail on these
NM_CACHE_VERSION = 3


def find_nm(out):
    """llvm-nm next to the clang of the out dir (clang_bin_path in args.gn), or None."""
    try:
        with open(os.path.join(out, "args.gn")) as f:
            for line in f:
                k, sep, v = line.partition("=")
                if sep and k.strip() == "clang_bin_path":
                    nm = os.path.join(v.strip().strip('"'), "llvm-nm")
                    return nm if os.path.isabs(nm) and os.path.exists(nm) else None
    except OSError:
        pass
    return None


def sym_hash(s):
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "little")


def nm_batch(args):
    """Per object: strong defs, refs and weak refs as names, weak defs as sym_hash
    (there are many of them, only those that resolve a reference are kept later)."""
    nm, out, paths = args
    r = subprocess.run([nm, "-A", "-P", "--no-sort", "-g"] + paths, cwd=out,
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    res = {p: ([], [], [], []) for p in paths}
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
        elif ty in WEAK_REFS:
            e[2].append(f[0])
        elif ty in WEAK:
            e[3].append(sym_hash(f[0]))
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


def _quiet(msg):
    pass


def load_symbols(nm, out, objs, skip, cache_path, jobs, reuse=True, weak_refs=True, log=_quiet):
    """objs: [path relative to out]; skip: indexes not to read (host toolchain). Returns
    (symtab, defs[oi], refs[oi], weak[oi]): strong defs and refs as arrays of sym ids
    (refs with the weak ones if weak_refs), weak defs as arrays of sym_hash. A missing
    object gets empty arrays. Cache: cache_path, read if reuse, written always."""
    st = SymTab()
    cache = {}
    if reuse and os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                c = pickle.load(f)
        except (OSError, EOFError, pickle.UnpicklingError):
            c = {}
        if c.get("version") == NM_CACHE_VERSION:
            st.names = c["names"]
            st.ids = {s: i for i, s in enumerate(st.names)}
            cache = c["objs"]
        else:
            log("nm: no cache of this format, reading all objects again")
    stats = {}
    todo = []
    for i, p in enumerate(objs):
        if i in skip:
            stats[p] = None
            continue
        try:
            s = os.stat(os.path.join(out, p))
            stats[p] = (s.st_mtime_ns, s.st_size)
        except OSError:
            stats[p] = None
            continue
        e = cache.get(p)
        if e is None or e[0] != stats[p]:
            todo.append(p)
    log("nm: %d objects, %d cached" % (len(objs), len(objs) - len(todo)))
    batches = [(nm, out, todo[i:i + 100]) for i in range(0, len(todo), 100)]
    done = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for res in ex.map(nm_batch, batches):
            for p, (d, r, wr, w) in res.items():
                cache[p] = (stats[p], array.array("I", (st.id(s) for s in d)).tobytes(),
                            array.array("I", (st.id(s) for s in r)).tobytes(),
                            array.array("I", (st.id(s) for s in wr)).tobytes(),
                            array.array("Q", w).tobytes())
            done += 1
            if done % 40 == 0:
                log("nm: %d / %d batches" % (done, len(batches)))
    if todo or not reuse:
        os.makedirs(os.path.dirname(os.path.abspath(cache_path)), exist_ok=True)
        with open(cache_path + ".tmp", "wb") as f:
            pickle.dump({"version": NM_CACHE_VERSION, "names": st.names, "objs": cache}, f, protocol=4)
        os.replace(cache_path + ".tmp", cache_path)
    defs, refs, weak = [], [], []
    for p in objs:
        e = cache.get(p)
        a, b, w = array.array("I"), array.array("I"), array.array("Q")
        if e is not None and stats.get(p) is not None:
            a.frombytes(e[1])
            b.frombytes(e[2])
            if weak_refs:
                b.frombytes(e[3])
            w.frombytes(e[4])
        defs.append(a)
        refs.append(b)
        weak.append(w)
    log("nm: %d symbols" % len(st.names))
    return st, defs, refs, weak


def dynamic_defs(nm, paths):
    """Names the shared libraries export (no symbol versions)."""
    if not paths:
        return set()
    r = subprocess.run([nm, "-D", "-P", "--defined-only"] + paths,
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
    return {line.split(" ", 1)[0].split("@", 1)[0] for line in r.stdout.splitlines() if " " in line}


def demangle(cxxfilt, names):
    """{mangled: demangled} by llvm-cxxfilt; names as they are if it fails."""
    if not names:
        return {}
    try:
        r = subprocess.run([cxxfilt], input="\n".join(names), stdout=subprocess.PIPE,
                           universal_newlines=True)
        outl = r.stdout.splitlines()
        if len(outl) == len(names):
            return dict(zip(names, outl))
    except OSError:
        pass
    return {n: n for n in names}
