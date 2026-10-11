import base64
import importlib.util
import json
from pathlib import Path
import sys
import unittest
import zlib

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("history_wire_audit", SCRIPTS / "audit_baostock_history.py")
audit_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_module)


class HistoryWireAuditTests(unittest.TestCase):
    def capture(self):
        params = {"code": "sz.000001", "start": "2020-01-02", "end": "2020-01-02"}
        row = ["2020-01-02", "sz.000001", "10", "11", "9", "10", "10", "123", "1230", "3", "1", "1", "0", "0"]
        body = "0\1success\1query_history_k_data_plus\1anonymous\11\12000\1" + json.dumps({"record": [row]})
        body += "\1" + "\1".join([params["code"], audit_module.FIELDS, params["start"], params["end"], "d", "3"])
        compressed = zlib.compress(body.encode())
        header = f"00.9.10\x0196\x01{len(compressed):010d}"
        wire = header.encode() + compressed + b"\x011234\n<![CDATA[]]>\n"
        return {"params": params, "wire_request_base64": base64.b64encode(b"history-query\n").decode(),
                "wire_response_base64": base64.b64encode(wire).decode(), "decoded_protocol_response": header + body}, row

    def test_saved_wire_independently_recovers_source_row(self):
        capture, row = self.capture()
        self.assertEqual(audit_module.protocol_rows(capture), [row])

    def test_truncated_wire_is_rejected(self):
        capture, _ = self.capture()
        wire = base64.b64decode(capture["wire_response_base64"])
        capture["wire_response_base64"] = base64.b64encode(wire[:-8]).decode()
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            audit_module.protocol_rows(capture)

    def test_changed_decoded_copy_does_not_pass_wire_check(self):
        capture, _ = self.capture()
        capture["decoded_protocol_response"] = capture["decoded_protocol_response"].replace('"1230"', '"9999"')
        with self.assertRaisesRegex(ValueError, "differs"):
            audit_module.protocol_rows(capture)

    def test_wrong_security_in_capture_parameters_is_rejected(self):
        capture, _ = self.capture()
        capture["params"]["code"] = "sz.000002"
        with self.assertRaisesRegex(ValueError, "parameters"):
            audit_module.protocol_rows(capture)
