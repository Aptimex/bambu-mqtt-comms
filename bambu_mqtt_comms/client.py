"""
MQTT client for communicating with Bambu Lab printers.

Provides raw connection management and request/response handling.
Command formatting and response parsing are left to the caller (e.g., bambu-mqtt-generator).
"""

import copy
import json
import random
import ssl
import time
import threading
from typing import Any, Dict, Optional, Union

import paho.mqtt.client as mqtt

from .config import PrinterConfig


def _deep_merge(target: Dict, updates: Dict) -> None:
    """
    Recursively merge updates into target, in place.

    Nested dictionaries are merged key by key so an incremental report only
    changes what it mentions. Lists are replaced: the printer sends them whole
    (a partial ams report still carries every tray), and a partial list could
    not be reconciled by position anyway.
    """
    for key, value in updates.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            _deep_merge(existing, value)
        else:
            target[key] = value


class BambuMQTTError(Exception):
    """Base exception for Bambu MQTT client errors."""
    pass


class ConnectionError(BambuMQTTError):
    """Connection-related errors."""
    pass


class TimeoutError(BambuMQTTError):
    """Timeout waiting for response."""
    pass


class BambuMQTTClient:
    """
    MQTT client for Bambu Lab printer communication.
    
    Handles TLS connection, request/response correlation via sequence_id,
    and provides raw send/receive methods. Does not perform any message
    signing, bootstrap, or command formatting - that is left to the caller.
    
    Usage:
        config = PrinterConfig(ip="192.168.1.100", serial="01S00A123456", access_code="abcdef12")
        
        with BambuMQTTClient(config) as client:
            # Send arbitrary JSON payload, wait for matching response
            response = client.send_and_wait({
                "print": {"sequence_id": "123", "command": "get_version"}
            })
            
            # Or send pre-built payload string
            response = client.send_and_wait('{"print": {"sequence_id": "123", "command": "get_version"}}')
            
            # Fire-and-forget (no response wait)
            client.publish({"print": {"sequence_id": "456", "command": "pause"}})
    """
    
    def __init__(self, config: PrinterConfig):
        """
        Initialize the MQTT client.
        
        Args:
            config: PrinterConfig with connection details.
        """
        self.config = config
        
        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._connect_event = threading.Event()
        self._connect_error: Optional[Exception] = None
        
        # Pending requests: sequence_id -> {"event": Event, "response": dict}
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._pending_lock = threading.Lock()
        
        # Push status waiting. The printer emits both full reports and
        # incremental diffs on the same topic, so they are tracked separately.
        self._status_data: Optional[Dict] = None
        self._full_status_data: Optional[Dict] = None
        # Last full report with every later incremental report merged onto it,
        # so the current state can be read without asking for a new one.
        self._merged_status: Optional[Dict] = None
        self._merged_status_at: float = 0.0
        self._status_lock = threading.Lock()
        self._status_event = threading.Event()
        self._full_status_event = threading.Event()
    
    def _make_tls_context(self) -> ssl.SSLContext:
        """Create TLS context for MQTT connection (server verification disabled)."""
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        """MQTT on_connect callback."""
        if reason_code.is_failure:
            self._connect_error = ConnectionError(f"MQTT connection failed: {reason_code}")
            self._connect_event.set()
            return
        
        self._connected = True
        client.subscribe(self.config.report_topic)
        self._connect_event.set()
    
    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        """MQTT on_disconnect callback."""
        self._connected = False
        if reason_code.is_failure:
            print(f"[bambu-mqtt-comms] Unexpected disconnect: {reason_code}")
    
    def _on_message(self, client, userdata, msg):
        """MQTT on_message callback."""
        try:
            data = json.loads(msg.payload.decode())
        except json.JSONDecodeError:
            return
        
        # Check for push_status (async status updates)
        if isinstance(data.get("print"), dict) and data["print"].get("command") == "push_status":
            status = data["print"]
            # "msg" distinguishes a full report (0) from an incremental diff
            # (1). Firmware that omits it only sends full reports.
            is_full = status.get("msg", 0) == 0
            with self._status_lock:
                self._status_data = status
                if is_full:
                    self._full_status_data = status
                    self._merged_status = copy.deepcopy(status)
                elif self._merged_status is not None:
                    _deep_merge(self._merged_status, status)
                if self._merged_status is not None:
                    self._merged_status_at = time.monotonic()
            self._status_event.set()
            if is_full:
                self._full_status_event.set()
            return
        
        # Extract sequence_id from response
        seq_id = None
        for scope in ("print", "security", "pushing", "info", "system"):
            if scope in data and isinstance(data[scope], dict):
                seq_id = data[scope].get("sequence_id")
                if seq_id:
                    break
        
        if seq_id:
            with self._pending_lock:
                if seq_id in self._pending:
                    self._pending[seq_id]["response"] = data
                    self._pending[seq_id]["event"].set()
    
    def connect(self, timeout: float = 15.0) -> None:
        """
        Connect to the printer's MQTT broker.
        
        Args:
            timeout: Connection timeout in seconds.
            
        Raises:
            ConnectionError: If connection fails or times out.
        """
        if self._connected:
            return
        
        self._connect_event.clear()
        self._connect_error = None
        
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.username_pw_set(self.config.username, self.config.access_code)
        self._client.tls_set_context(self._make_tls_context())
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        
        try:
            self._client.connect(self.config.ip, self.config.port, keepalive=self.config.keepalive)
            self._client.loop_start()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self.config.ip}:{self.config.port}: {e}")
        
        if not self._connect_event.wait(timeout):
            self._client.loop_stop()
            self._client = None
            raise ConnectionError(f"Connection timeout after {timeout}s")
        
        if self._connect_error:
            self._client.loop_stop()
            self._client = None
            raise self._connect_error
    
    def disconnect(self) -> None:
        """Disconnect from the printer."""
        if self._client:
            self._client.disconnect()
            self._client.loop_stop()
            self._client = None
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        """Check if connected to printer."""
        return self._connected and self._client is not None
    
    def publish(self, payload: Union[Dict, str], qos: int = 1) -> None:
        """
        Publish a message to the printer (fire-and-forget).
        
        Args:
            payload: Dict or JSON string to send.
            qos: MQTT QoS level (default: 1).
        """
        if not self.is_connected:
            raise ConnectionError("Not connected. Call connect() first.")
        
        if isinstance(payload, dict):
            payload = json.dumps(payload, separators=(",", ":"))
        
        self._client.publish(self.config.request_topic, payload, qos=qos)
    
    def wait_for_status(self, timeout: float = 10.0) -> Optional[Dict]:
        """
        Wait for a push_status update from the printer.
        
        Args:
            timeout: Maximum time to wait.
            
        Returns:
            Status dict or None if timeout.
        """
        self._status_event.clear()
        if self._status_event.wait(timeout):
            with self._status_lock:
                return self._status_data
        return None
    
    def get_status(self, max_age: Optional[float] = None) -> Optional[Dict]:
        """
        Return the current status without asking the printer for a new one.

        The printer pushes status continuously - some firmware sends full
        reports, some sends incremental ones - and those are accumulated into a
        single picture as they arrive. Reading that costs nothing, where
        request_status() publishes a pushall and waits for a full reply;
        repeating the latter often enough makes a printer stop answering.

        Args:
            max_age: Reject the snapshot if it has not been updated within this
                many seconds. None accepts it at any age.

        Returns:
            A copy of the current status, or None if nothing has been received
            yet or the snapshot is older than max_age.
        """
        with self._status_lock:
            if self._merged_status is None:
                return None
            if max_age is not None and (time.monotonic() - self._merged_status_at) > max_age:
                return None
            return copy.deepcopy(self._merged_status)

    def request_status(
        self, sequence_id: Optional[str] = None, timeout: float = 15.0
    ) -> Dict:
        """
        Request a full status push (pushall) and wait for the push_status reply.

        The reply is asynchronous and does not carry the request's sequence_id,
        so it is matched by type rather than id. The printer also emits
        incremental push_status diffs on the same topic, which contain only the
        fields that changed; those are skipped here, because a caller asking
        for status wants the complete picture (a diff arriving first would
        otherwise look like a printer with no AMS).

        Args:
            sequence_id: Optional custom sequence_id for the pushall request.
            timeout: How long to wait for the full report.

        Returns:
            The full push_status response data.

        Raises:
            TimeoutError: If no full push_status arrives within the timeout.
        """
        seq = sequence_id or self.random_sequence_id()
        payload = {"pushing": {"sequence_id": seq, "command": "pushall", "version": 1, "push_target": 1}}

        # Clear previous status
        self._status_event.clear()
        self._full_status_event.clear()
        with self._status_lock:
            self._status_data = None
            self._full_status_data = None

        # Send pushall as fire-and-forget (no response expected with same seq_id)
        self.publish(payload)

        if not self._full_status_event.wait(timeout=timeout):
            raise TimeoutError(
                f"Timeout waiting for a full push_status response after {timeout}s"
            )

        with self._status_lock:
            return self._full_status_data or {}
    
    def get_trusted_certs(self, timeout: float = 10.0) -> Dict:
        """
        Query the printer for its list of trusted application certificates.
        
        Sends an app_cert_list request and returns the list of trusted cert IDs.
        
        Args:
            timeout: Response timeout in seconds.
            
        Returns:
            Dict with 'result' (str or None), 'cert_ids' (list of str), and 'cert_count' (int).
            
        Raises:
            TimeoutError: If no response received within timeout.
            ConnectionError: If not connected.
        """
        seq = self.random_sequence_id()
        payload = {
            "security": {
                "command": "app_cert_list",
                "sequence_id": seq,
                "timestamp": int(time.time() * 1000),
                "type": "app"
            }
        }

        response = self.send_and_wait(payload, timeout=timeout)

        # Extract result from security scope
        security = response.get("security", {})
        result = security.get("result")
        cert_ids = security.get("cert_ids", [])

        return {
            "result": result,
            "cert_ids": cert_ids,
            "cert_count": len(cert_ids),
        }

    def install_app_cert(
        self,
        bootstrap_message: Union[Dict, str],
        cert_id: Optional[str] = None,
        attempts: int = 6,
        interval: float = 5.0,
        query_timeout: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Publish an app_cert_install bootstrap and verify the printer accepted it.

        The printer never acknowledges app_cert_install directly, so the message
        is published fire-and-forget and the registration is confirmed by polling
        app_cert_list until `cert_id` shows up among the trusted certs. Signed
        commands sent before that lands are rejected (err_code 84033545), so
        callers should treat a False `trusted` as "do not send signed commands".

        Registration is not instantaneous and is lost on printer power-cycle, so
        this must be re-run per session.

        Args:
            bootstrap_message: The app_cert_install payload, e.g. from
                MQTTSigner.build_app_cert_install(). Strings are published as-is.
            cert_id: The cert_id expected to become trusted (e.g.
                MQTTSigner.get_cert_id()). If omitted, no verification is done
                and the bootstrap is simply published.
            attempts: Total app_cert_list polls, including the immediate first one.
            interval: Seconds to wait between polls.
            query_timeout: Per-poll response timeout in seconds.

        Returns:
            Dict with 'trusted' (bool), 'cert_ids' (list of str), and
            'attempts_used' (int).

        Raises:
            ConnectionError: If not connected.
        """
        if not self.is_connected:
            raise ConnectionError("Not connected. Call connect() first.")

        self.publish(bootstrap_message)

        if cert_id is None:
            return {"trusted": False, "cert_ids": [], "attempts_used": 0}

        cert_ids: list = []
        for attempt in range(1, max(1, attempts) + 1):
            # Poll immediately on the first pass: the cert may already be
            # trusted from an earlier bootstrap this power cycle.
            if attempt > 1:
                time.sleep(interval)
            try:
                certs = self.get_trusted_certs(timeout=query_timeout)
            except TimeoutError:
                continue
            cert_ids = certs.get("cert_ids", [])
            if cert_id in cert_ids:
                return {
                    "trusted": True,
                    "cert_ids": cert_ids,
                    "attempts_used": attempt,
                }

        return {
            "trusted": False,
            "cert_ids": cert_ids,
            "attempts_used": max(1, attempts),
        }

    def send_and_wait(
        self,
        payload: Union[Dict, str],
        timeout: Optional[float] = None,
        qos: int = 1,
    ) -> Dict:
        """
        Send a payload and wait for the matching response.

        Matches response by sequence_id in the payload.

        A payload passed as a string is published byte-for-byte as given. This
        matters for signed messages: their signature covers the exact bytes of
        the command object, so re-serializing them would invalidate it.

        Args:
            payload: Dict or JSON string containing a sequence_id.
            timeout: Response timeout in seconds (default: config.response_timeout).
            qos: MQTT QoS level (default: 1).

        Returns:
            Response dict from printer.

        Raises:
            TimeoutError: If no response received within timeout.
            ConnectionError: If not connected.
            ValueError: If payload lacks sequence_id.
        """
        if not self.is_connected:
            raise ConnectionError("Not connected. Call connect() first.")

        timeout = timeout or self.config.response_timeout

        # Parse a string payload only to read its sequence_id — the original
        # bytes are what actually go on the wire.
        if isinstance(payload, str):
            wire_msg = payload
            payload_dict = json.loads(payload)
        else:
            payload_dict = payload
            wire_msg = json.dumps(payload_dict, separators=(",", ":"))

        # Extract sequence_id
        seq_id = self._extract_sequence_id(payload_dict)
        if not seq_id:
            raise ValueError("Payload must contain a sequence_id")

        # Register pending request
        event = threading.Event()
        with self._pending_lock:
            self._pending[seq_id] = {"event": event, "response": None, "timestamp": time.time()}

        # Send
        self._client.publish(self.config.request_topic, wire_msg, qos=qos)
        
        # Wait for response
        if not event.wait(timeout):
            with self._pending_lock:
                self._pending.pop(seq_id, None)
            raise TimeoutError(f"Timeout waiting for response to sequence_id={seq_id} after {timeout}s")
        
        with self._pending_lock:
            result = self._pending.pop(seq_id, {}).get("response")
        
        if result is None:
            raise TimeoutError(f"No response data for sequence_id={seq_id}")
        
        return result
    
    @staticmethod
    def random_sequence_id() -> str:
        """Generate next sequence ID as a random 5-digit number string starting with '2'."""
        return str(random.randint(20_000, 29_999))
    
    def _extract_sequence_id(self, payload: Dict) -> Optional[str]:
        """Extract sequence_id from payload dict.
        
        Prefers 'print' key over 'header' when both are present.
        Skips 'header' key which is metadata only.
        """
        # Prefer 'print' key (common in Bambu responses)
        if "print" in payload and isinstance(payload["print"], dict):
            return payload["print"].get("sequence_id")
        # Fallback to known command sections, skipping 'header'
        for key in ("pushing", "security", "info", "system"):
            if key in payload and isinstance(payload[key], dict):
                return payload[key].get("sequence_id")
        # Fallback to any key except 'header'
        for key, value in payload.items():
            if key == "header":
                continue
            if isinstance(value, dict):
                return value.get("sequence_id")
        return None
    
    def __enter__(self) -> "BambuMQTTClient":
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()