"""The client without a daemon: argument checks, error mapping, closing, and the watch iterator.

The calls that need a daemon are in the drive suite (`tests/drive.rs`). Here every call that would
reach the library is replaced by a fake, so the tests show what is checked before a call and how a
status becomes an exception.
"""

import itertools
import os
import socket
import unittest
from unittest import mock

from daedalus import client as dc
from daedalus import wire
from daedalus.client import (
    SERVICE_CODES,
    Client,
    ClosedError,
    DaedalusError,
    LibraryError,
    ServiceError,
    StartConfig,
    Watch,
    WatchTimeout,
)
from daedalus._ffi import STATUS_OK


class _NoCall:
    """A stand-in for a library function that must not be reached."""

    def __call__(self, *args, **kwargs):
        raise AssertionError("the library was called, but the argument should have been refused")


class _FakeApi:
    """The parts of the library a client touches before a call: its connection's close."""

    def __init__(self):
        self.closed = []

    def __getattr__(self, name):
        # Every other function of the library: reaching one is a failure of the test.
        return _NoCall()

    def close(self, handle):
        self.closed.append(handle)
        return STATUS_OK


def _client(handles=(7,)):
    api = _FakeApi()
    connection = dc._Connection(api)
    connection.handles.extend(handles)
    return Client(api, connection, handles[0]), api


class UnixPlatformTests(unittest.TestCase):
    @unittest.skipIf(hasattr(socket, "AF_UNIX"), "this platform has unix sockets")
    @unittest.skipUnless(os.environ.get(dc.ENV_VAR), "the client library is not in the environment")
    def test_a_unix_endpoint_is_unsupported_where_there_are_no_unix_sockets(self):
        with self.assertRaises(DaedalusError) as raised:
            Client.connect("unix:/tmp/daedalus.sock")
        self.assertEqual(raised.exception.status, dc.STATUS_UNSUPPORTED)
        self.assertIsNone(raised.exception.code)
        self.assertIn("unix sockets are not available", str(raised.exception))


class EndpointTests(unittest.TestCase):
    def test_an_endpoint_is_unix_or_tcp(self):
        with self.assertRaises(ValueError):
            Client.connect("http://example")
        with self.assertRaises(ValueError):
            Client.connect("")

    def test_an_endpoint_is_text(self):
        with self.assertRaises(TypeError):
            Client.connect(b"unix:/tmp/x")


class LibraryTests(unittest.TestCase):
    def test_no_library_names_the_variable_and_how_to_build_it(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(dc.ENV_VAR, None)
            with self.assertRaises(LibraryError) as raised:
                dc.library_for(None)
        self.assertIn("DAEDALUS_CLIENT_LIB", str(raised.exception))
        self.assertIn("cargo build -p daedalus-client-capi", str(raised.exception))

    def test_a_library_that_is_not_there_is_a_library_error(self):
        with self.assertRaises(LibraryError) as raised:
            dc.library_for("/no/such/dir/libdaedalus_client_capi.so")
        self.assertIn("/no/such/dir", str(raised.exception))


class ConfigTests(unittest.TestCase):
    def test_a_start_config_has_the_protocol_defaults(self):
        config = StartConfig()
        self.assertEqual((config.fifo, config.faults, config.telemetry, config.unmerged), (0, 0, "basic", False))
        self.assertEqual(tuple(config.priority), ())

    def test_telemetry_is_one_of_four_words(self):
        with self.assertRaises(ValueError):
            StartConfig(telemetry="loud")

    def test_fifo_and_faults_fit_a_u32(self):
        with self.assertRaises(ValueError):
            StartConfig(fifo=-1)
        with self.assertRaises(ValueError):
            StartConfig(faults=2**32)
        with self.assertRaises(TypeError):
            StartConfig(fifo=True)


class ArgumentTests(unittest.TestCase):
    """Each of these is refused before the library is called (the fakes would fail the test)."""

    def test_a_run_is_a_u64_and_run_numbers_start_at_one(self):
        client, _ = _client()
        with self.assertRaises(ValueError):
            client.step(-1)
        with self.assertRaises(TypeError):
            client.step("1")
        with self.assertRaises(ValueError):
            client.watch(run=0)

    def test_a_step_count_is_a_u32(self):
        client, _ = _client()
        with self.assertRaises(ValueError):
            client.step(1, 2**32)

    def test_a_stop_mode_is_drain_or_immediate(self):
        client, _ = _client()
        with self.assertRaises(ValueError):
            client.stop(1, "sideways")

    def test_a_plan_form_is_steps_blob_or_source(self):
        client, _ = _client()
        with self.assertRaises(ValueError):
            client.plan("g", "flat")

    def test_a_watch_kind_is_named_or_a_number(self):
        client, _ = _client()
        with self.assertRaises(ValueError):
            client.watch(kinds="sometimes")

    def test_a_value_that_does_not_fit_its_type_is_refused_before_the_call(self):
        client, _ = _client()
        with self.assertRaises(wire.WireError):
            client.feed(1, "in", 300, ty=wire.T.u8)

    def test_a_port_name_is_text(self):
        client, _ = _client()
        with self.assertRaises(TypeError):
            client.feed(1, 5, 1, ty=wire.T.u32)


class ErrorTests(unittest.TestCase):
    def test_each_protocol_code_has_its_name(self):
        self.assertEqual(SERVICE_CODES[1], "invalid_request")
        self.assertEqual(SERVICE_CODES[3], "unknown_run")
        self.assertEqual(SERVICE_CODES[7], "planning_failed")
        self.assertEqual(SERVICE_CODES[11], "frame_too_large")
        self.assertEqual(SERVICE_CODES[12], "unsupported")
        self.assertEqual(SERVICE_CODES[65535], "other")
        self.assertNotIn(0, SERVICE_CODES, "0 is no refusal by the daemon")

    def test_a_service_error_carries_the_status_the_code_and_the_message(self):
        error = ServiceError(21, "daedalus_client_service_error", "no run 9", "unknown_run")
        self.assertIsInstance(error, DaedalusError)
        self.assertEqual((error.status, error.code, error.message), (21, "unknown_run", "no run 9"))
        self.assertIn("unknown_run", str(error))
        self.assertIn("no run 9", str(error))

    def test_a_failure_with_no_code_is_not_a_service_error(self):
        native = dc.Native.__new__(dc.Native)
        native.status_name = lambda status: b"daedalus_client_disconnected"
        native.last_message = lambda: "the connection is gone"
        error = native.failure(19)
        self.assertNotIsInstance(error, ServiceError)
        self.assertIsNone(error.code)
        self.assertEqual(error.status_name, "daedalus_client_disconnected")

    def test_a_closed_client_refuses_a_call_with_stale_handle(self):
        client, api = _client()
        client.close()
        self.assertTrue(client.closed)
        with self.assertRaises(ClosedError) as raised:
            client.ping()
        self.assertEqual(raised.exception.status, 3)


class CloseTests(unittest.TestCase):
    def test_closing_releases_every_handle_once_and_the_last_made_first(self):
        client, api = _client(handles=(7, 8, 9))
        client.close()
        client.close()
        self.assertEqual(api.closed, [9, 8, 7])

    def test_a_session_shares_the_connection_and_closing_it_closes_both(self):
        api = _FakeApi()
        connection = dc._Connection(api)
        connection.handles.append(7)
        root = Client(api, connection, 7)
        # A session is an ordinary handle of the connection: closing any of them closes it all.
        connection.handles.append(8)
        session = Client(api, connection, 8)
        session.close()
        self.assertTrue(root.closed)
        self.assertEqual(api.closed, [8, 7])

    def test_the_context_manager_closes(self):
        client, api = _client(handles=(4,))
        with client:
            pass
        self.assertEqual(api.closed, [4])


class WatchTests(unittest.TestCase):
    def _watch(self, timeout=None):
        api = mock.Mock()
        api.watch_close.return_value = STATUS_OK
        return Watch(api, 5, timeout), api

    def test_an_iterator_stops_and_closes_when_no_event_comes_in_time(self):
        watch, api = self._watch(timeout=0.01)
        with mock.patch.object(Watch, "_poll", return_value=(0, None)):
            events = list(watch)
        self.assertEqual(events, [])
        self.assertTrue(watch.closed)
        api.watch_close.assert_called_once_with(5)

    def test_next_event_raises_a_timeout_while_the_watch_stays_open(self):
        watch, api = self._watch()
        with mock.patch.object(Watch, "_poll", return_value=(0, None)):
            with self.assertRaises(WatchTimeout):
                watch.next_event(timeout=0.01)
        self.assertFalse(watch.closed)
        api.watch_close.assert_not_called()

    def test_the_stream_end_gives_none_and_the_iterator_closes(self):
        watch, api = self._watch()
        with mock.patch.object(Watch, "_poll", return_value=(2, None)):
            self.assertIsNone(watch.next_event())
        watch, api = self._watch()
        with mock.patch.object(Watch, "_poll", return_value=(2, None)):
            self.assertEqual(list(watch), [])
        self.assertTrue(watch.closed)

    def test_events_come_in_the_order_they_were_sent(self):
        watch, _ = self._watch(timeout=0.01)
        script = [(1, {"event": "lagged", "dropped": 1}), (1, {"event": "ended", "run": 1})]
        polls = itertools.chain(script, itertools.repeat((0, None)))
        with mock.patch.object(Watch, "_poll", side_effect=lambda wait: next(polls)):
            events = list(watch)
        self.assertEqual([event["event"] for event in events], ["lagged", "ended"])

    def test_drain_gives_what_is_queued_and_stops_at_idle(self):
        watch, _ = self._watch()
        script = [(1, {"event": "telemetry", "run": 1}), (1, {"event": "ended", "run": 1}), (0, None)]
        with mock.patch.object(Watch, "_poll", side_effect=script):
            self.assertEqual([e["event"] for e in watch.drain()], ["telemetry", "ended"])

    def test_a_closed_watch_refuses_a_poll(self):
        watch, _ = self._watch()
        watch.close()
        with self.assertRaises(ClosedError):
            watch._poll(0)


if __name__ == "__main__":
    unittest.main()
