"""The in-process host: argument checks and error mapping, and a live run when the test runner gives
the host library, the fixture plugin and the pass graph in the environment (`tests/units.rs` does).
"""

import os
import unittest
from unittest import mock

from daedalus import host as dh
from daedalus.host import Graph, Host, LibraryError, UnsupportedPort
from daedalus._ffi import DaedalusError

LIVE = all(os.environ.get(name) for name in ("DAEDALUS_HOST_LIB", "DAEDALUS_TEST_PLUGIN", "DAEDALUS_TEST_GRAPH"))
DAEDALUS_GRAPH_INVALID = 12
DAEDALUS_TYPE_MISMATCH = 9


class _NoCall:
    def __call__(self, *args, **kwargs):
        raise AssertionError("the library was called, but the argument should have been refused")


class _FakeApi:
    def __getattr__(self, name):
        return _NoCall()


class NoLibraryTests(unittest.TestCase):
    def test_no_host_library_names_the_variable_and_how_to_build_it(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(dh.ENV_VAR, None)
            with self.assertRaises(LibraryError) as raised:
                dh.library_for(None)
        self.assertIn("DAEDALUS_HOST_LIB", str(raised.exception))
        self.assertIn("cargo build -p daedalus-capi", str(raised.exception))

    def test_a_host_library_that_is_not_there_is_a_library_error(self):
        with self.assertRaises(LibraryError):
            dh.library_for("/no/such/dir/libdaedalus_capi.so")


class ArgumentTests(unittest.TestCase):
    """Refused before the library is called: the fake fails the test if it is reached."""

    def _graph(self):
        return Graph(_FakeApi(), 3, host=mock.Mock())

    def test_a_value_the_host_does_not_take_is_an_unsupported_port(self):
        graph = self._graph()
        for value in ("text", [1, 2], {"a": 1}, None):
            with self.assertRaises(UnsupportedPort):
                graph.push("in", value)

    def test_a_port_is_named_by_text_or_by_a_non_negative_index(self):
        graph = self._graph()
        with self.assertRaises(TypeError):
            graph.push(1.5, 3)
        with self.assertRaises(ValueError):
            graph.push(-1, 3)

    def test_a_closed_graph_refuses_a_call(self):
        graph = self._graph()
        graph._closed = True
        with self.assertRaises(DaedalusError) as raised:
            graph.tick()
        self.assertEqual(raised.exception.status, 3)

    def test_closing_a_graph_tells_its_host_once(self):
        host = mock.Mock()
        api = mock.Mock()
        api.graph_release.return_value = 0
        graph = Graph(api, 3, host=host)
        graph.close()
        graph.close()
        host._forget.assert_called_once_with(graph)
        api.graph_release.assert_called_once_with(3)


@unittest.skipUnless(LIVE, "the host library, the fixture plugin and the graph are not in the environment")
class LiveTests(unittest.TestCase):
    def setUp(self):
        self.host = Host()
        self.host.install(os.environ["DAEDALUS_TEST_PLUGIN"])
        with open(os.environ["DAEDALUS_TEST_GRAPH"], encoding="utf-8") as handle:
            self.text = handle.read()

    def tearDown(self):
        self.host.close()

    def test_the_library_reports_its_version(self):
        major, minor, layout = Host.version(os.environ["DAEDALUS_HOST_LIB"])
        self.assertIsInstance(major, int)
        self.assertIsInstance(minor, int)
        self.assertIsInstance(layout, int)

    def test_a_graph_runs_a_value_through_in_to_out(self):
        graph = self.host.compile(self.text)
        self.assertEqual([(port.name, port.type_key, port.is_input) for port in graph.ports],
                         [("in", "u32", True), ("out", "u32", False)])
        self.assertEqual(graph.port_index("out"), 1)
        graph.push("in", 41)
        self.assertIsNone(graph.take("out"), "nothing has run yet")
        tick = graph.tick()
        self.assertGreaterEqual(tick.step_calls, 1)
        self.assertEqual(graph.take("out"), 41)
        self.assertGreaterEqual(graph.telemetry()["ticks"], 1)
        graph.close()

    def test_a_port_can_be_named_by_index(self):
        graph = self.host.compile(self.text)
        graph.push(0, 7)
        graph.tick()
        self.assertEqual(graph.take(1), 7)
        graph.close()

    def test_a_value_of_the_wrong_kind_is_the_library_refusal(self):
        graph = self.host.compile(self.text)
        with self.assertRaises(DaedalusError) as raised:
            graph.push("in", True)
        self.assertEqual(raised.exception.status, DAEDALUS_TYPE_MISMATCH)
        graph.close()

    def test_a_graph_that_does_not_compile_is_refused_with_its_status(self):
        with self.assertRaises(DaedalusError) as raised:
            self.host.compile("{}")
        self.assertEqual(raised.exception.status, DAEDALUS_GRAPH_INVALID)
        self.assertTrue(str(raised.exception))

    def test_closing_the_host_closes_its_graphs(self):
        graph = self.host.compile(self.text)
        self.host.close()
        self.assertTrue(graph.closed)
        self.assertTrue(self.host.closed)

    def test_the_host_lists_its_nodes_as_json(self):
        schemas = self.host.schemas()
        self.assertIsNotNone(schemas)


if __name__ == "__main__":
    unittest.main()
