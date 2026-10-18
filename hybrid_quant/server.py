"""Loopback-only dashboard routes with no broker or remote execution endpoints."""
from __future__ import annotations

from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import threading
import time
from urllib.parse import parse_qs, urlsplit


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes
    content_type: str = "application/json; charset=utf-8"


def json_response(status: int, value) -> Response:
    return Response(status, json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"))


class DashboardApplication:
    def __init__(self, project_root: Path, port: int = 8765, *, store=None, market=None, demo_runner=None,
                 overview_ttl: float = 15.0):
        self.project_root = project_root.resolve()
        self.web_root = Path(__file__).resolve().parent / "web"
        self.allowed_hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}
        self.allowed_origins = {f"http://{host}" for host in self.allowed_hosts}
        if store is None:
            from .dashboard_store import DashboardStore
            store = DashboardStore(self.project_root)
        if demo_runner is None:
            from .cli import create_demo_report
            demo_runner = lambda: create_demo_report(self.project_root / "config/demo.json", self.project_root / "reports/demo")
        self.store, self.market, self.demo_runner = store, market, demo_runner
        self._demo_lock = threading.Lock()
        if isinstance(overview_ttl, bool) or not isinstance(overview_ttl, (int, float)) or overview_ttl < 0:
            raise ValueError("overview_ttl must be a nonnegative number of seconds")
        self._overview_ttl = float(overview_ttl)
        self._overview_lock = threading.Lock()
        self._overview_response: Response | None = None
        self._overview_generated = 0.0

    def _market_data(self):
        if self.market is not None:
            return self.market
        from .data_registry import DatasetRegistry
        from .market_data import StagedMarketData
        return StagedMarketData(DatasetRegistry(self.project_root / "config/datasets.json", self.project_root))

    @staticmethod
    def _error(status: int, message: str) -> Response:
        return json_response(status, {"message": message})

    def _overview(self) -> Response:
        """Serve the overview from a short-lived cache.

        Building an overview re-validates every registered manifest and every
        demo run, which is far too expensive to repeat on each page load.
        Callers wait for a single build instead of stacking identical work, and
        a newly created demo run invalidates the cached answer immediately.
        """
        with self._overview_lock:
            if (self._overview_response is not None
                    and time.monotonic() - self._overview_generated <= self._overview_ttl):
                return self._overview_response
            self._overview_response = json_response(200, self.store.overview())
            self._overview_generated = time.monotonic()
            return self._overview_response

    def dispatch(self, method: str, target: str, headers: dict[str, str], body: bytes = b"") -> Response:
        headers = {key.lower(): value for key, value in headers.items()}
        if headers.get("host") not in self.allowed_hosts:
            return self._error(403, "仅允许从本机工作台地址访问")
        if len(target) > 2048 or len(body) > 1024 or headers.get("transfer-encoding"):
            return self._error(400, "请求超出允许范围")
        try:
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc:
                return self._error(400, "请求地址必须是本机相对路径")
            query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=8)
            if any(len(values) != 1 for values in query.values()):
                return self._error(400, "查询参数不能重复")
            params = {key: values[0] for key, values in query.items()}
            path = parsed.path
            if method == "POST":
                if path != "/api/demo" or params:
                    return self._error(404, "没有该操作")
                if headers.get("origin") not in self.allowed_origins:
                    return self._error(403, "请从本机工作台运行工程演示")
                if body and json.loads(body.decode("utf-8")) != {}:
                    return self._error(400, "工程演示使用项目中保存的配置")
                if not self._demo_lock.acquire(blocking=False):
                    return self._error(409, "工程演示正在运行，请等待完成")
                try:
                    response = json_response(201, self.demo_runner())
                finally:
                    self._demo_lock.release()
                # A new archived run changes the overview; drop the cached copy.
                with self._overview_lock:
                    self._overview_response = None
                return response
            if method != "GET":
                return self._error(405, "不支持该请求方法")
            if path == "/api/overview":
                return self._overview()
            if path == "/api/config":
                config = json.loads((self.project_root / "config/demo.json").read_text(encoding="utf-8"))
                return json_response(200, config)
            if path == "/api/runs":
                return json_response(200, self.store.runs())
            match = re.fullmatch(r"/api/runs/([0-9]{8}T[0-9]{6}Z_[0-9a-f]{8})(?:/([a-z_]+))?", path)
            if match:
                run_id, kind = match.groups()
                if kind is None:
                    return json_response(200, self.store.run_detail(run_id))
                return json_response(200, self.store.run_records(run_id, kind, offset=int(params.get("offset", "0")), limit=int(params.get("limit", "50"))))
            if path == "/api/data/catalog":
                return json_response(200, self._market_data().catalog(params.get("dataset", "stock_daily"), q=params.get("q", ""), limit=int(params.get("limit", "50"))))
            if path == "/api/data/bars":
                return json_response(200, self._market_data().preview_bars(params.get("dataset", "stock_daily"), params.get("symbol", ""), limit=int(params.get("limit", "200"))))
            assets = {"/": ("index.html", "text/html; charset=utf-8"),
                      "/app.css": ("app.css", "text/css; charset=utf-8"),
                      "/app.js": ("app.js", "text/javascript; charset=utf-8"),
                      "/assets/app.css": ("app.css", "text/css; charset=utf-8"),
                      "/assets/app.js": ("app.js", "text/javascript; charset=utf-8")}
            if path in assets and not params:
                filename, content_type = assets[path]
                return Response(200, (self.web_root / filename).read_bytes(), content_type)
            return self._error(404, "未找到页面或数据")
        except FileNotFoundError:
            return self._error(404, "所需文件或记录尚未生成")
        except (ValueError, KeyError, TypeError) as exc:
            return self._error(400, str(exc))
        except OSError as exc:
            return self._error(503, f"本地文件暂时不可读取：{exc}")
        except Exception:
            return self._error(500, "工作台处理失败，请查看本地服务日志")


class DashboardServer(ThreadingHTTPServer):
    """Loopback server with a bounded accept backlog and daemon workers."""

    daemon_threads = True
    request_queue_size = 32


def handler_type(application: DashboardApplication):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        # A slow or idle client must not pin a worker forever: reads time out
        # and the base handler closes the connection.
        timeout = 30
        # Do not advertise the interpreter or library version to clients.
        server_version = "HybridQuantDashboard"
        sys_version = ""

        def _dispatch(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 <= length <= 1024:
                    response = application._error(400, "请求体超出允许范围")
                    self.close_connection = True
                else:
                    response = application.dispatch(self.command, self.path, dict(self.headers), self.rfile.read(length) if length else b"")
            except ValueError:
                response = application._error(400, "无效的请求长度")
                self.close_connection = True
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'")
            self.end_headers()
            self.wfile.write(response.body)

        do_GET = _dispatch
        do_POST = _dispatch
        do_PUT = _dispatch
        do_DELETE = _dispatch
        do_OPTIONS = _dispatch

    return Handler


def serve(project_root: Path, port: int = 8765) -> None:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("端口必须在1至65535之间")
    application = DashboardApplication(project_root, port)
    with DashboardServer(("127.0.0.1", port), handler_type(application)) as server:
        print(f"独立混合量化研究工作台：http://127.0.0.1:{port}", flush=True)
        print("按 Ctrl+C 停止本地服务。真实数据准入仍按证据检查。", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
