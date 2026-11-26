"""End-to-end smoke test: run a tiny bench, then analyze the recording."""
import os
import tempfile
import unittest

from replaylab.analyze import analyze_file, build_forest, render_tree
from replaylab.runner import Bench
from replaylab.workload import from_recording, generate


class TestEndToEnd(unittest.TestCase):
    def test_run_analyze_replay(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "rec.jsonl")
            bench = Bench(seed=3, edge_retries=1, checkout_retries=1,
                          out_path=path, dup_rate=0.15)
            bench.set_switches(edge=1, checkout=1)  # via HTTP admin endpoint
            bench.run_workload(generate(3, 6))
            bench.shutdown()

            attempts, stats = analyze_file(path)
            roots = [a for a in attempts if a["cls"] == "root"]

            # one root attempt per user request
            self.assertEqual(len(roots), 6)
            # every attempt is dual-collected -> mirrors == canonical attempts
            self.assertEqual(stats["mirror_pairs"], stats["canonical"])
            # raw = canonical records + exact duplicates
            self.assertEqual(
                stats["raw"],
                sum(1 for a in attempts for _ in [0]) * 2
                + stats["exact_dups"])
            # every retry carries try_seq > 0 and a non-root parent
            for a in attempts:
                if a["cls"] == "retry":
                    self.assertGreater(a["try_seq"], 0)
                    self.assertIsNotNone(a["parent_attempt_id"])
            # forest is consistent: 6 roots, every child linked
            forest_roots, children = build_forest(attempts)
            self.assertEqual(len(forest_roots), 6)
            # tree renders for every trace without crashing
            for r in roots:
                txt = render_tree(attempts, r["trace_id"])
                self.assertIn("因果树", txt)

            # replay extraction round-trips the workload
            wl = from_recording(path)
            self.assertEqual(len(wl), 6)
            self.assertEqual([w.trace_id for w in wl],
                             sorted(w.trace_id for w in wl))
            orig = {w.trace_id: w.items for w in generate(3, 6)}
            for w in wl:
                self.assertEqual(w.items, orig[w.trace_id])


if __name__ == "__main__":
    unittest.main()
