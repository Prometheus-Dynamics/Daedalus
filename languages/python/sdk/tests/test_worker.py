"""The worker loop: declarations, faults, and that node output never reaches stdout."""

import io
import os
import subprocess
import sys
import unittest

from daedalus import NodeError, T, node, wire
from daedalus.worker import Node, NODES

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(HERE, "..", "..", "fixtures", "worker.py")


class DeclarationTests(unittest.TestCase):
    def test_an_unsupported_annotation_fails_at_decoration(self):
        with self.assertRaises(wire.WireError):
            node("demo.bad", inputs=[("x", ("scalar", "complex"))], outputs=[])(lambda x: x)

    def test_a_declared_node_keeps_its_function(self):
        @node("demo.twice", inputs=[("x", T.i64)], outputs=[("out", T.i64)])
        def twice(x):
            return 2 * x

        self.assertEqual(twice(4), 8)
        self.assertIn("demo.twice", [declared.id for declared in NODES])
        NODES[:] = [declared for declared in NODES if declared.id != "demo.twice"]


class StdoutTests(unittest.TestCase):
    def test_a_print_in_a_body_does_not_reach_stdout(self):
        script = (
            "import sys; sys.path.insert(0, %r); sys.path.insert(0, %r)\n"
            "from daedalus import node, serve, T\n"
            "@node('demo.chatty', inputs=[('x', T.i64)], outputs=[('out', T.i64)])\n"
            "def chatty(x):\n"
            "    print('hello from a body')\n"
            "    return x\n"
            "serve()\n"
        ) % (os.path.join(HERE, ".."), os.path.join(HERE, "..", ".."))
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        hello = wire.read_packet(process.stdout)
        self.assertEqual(hello[0], wire.KIND_HELLO)
        schema = wire.Schema()
        schema.port("demo.chatty", "input", "x", T.i64)
        schema.port("demo.chatty", "output", "out", T.i64)
        out = wire.Writer()
        out.u8(wire.KIND_RUN)
        out.u64(1)
        out.u32(0)
        out.u32(1)
        out.str("demo.chatty")
        out.u32(1)
        out.str("demo.chatty")
        out.str("x")
        out.i64(5)
        wire.write_packet(process.stdin, bytes(out.buf))
        reply = wire.read_packet(process.stdout)
        self.assertEqual(reply[0], wire.KIND_OUTPUTS, "the reply is a packet, not the print")
        wire.write_packet(process.stdin, bytes([wire.KIND_SHUTDOWN]))
        process.stdin.close()
        self.assertEqual(process.wait(timeout=10), 0)
        self.assertIn(b"hello from a body", process.stderr.read())
        process.stdout.close()


if __name__ == "__main__":
    unittest.main()
