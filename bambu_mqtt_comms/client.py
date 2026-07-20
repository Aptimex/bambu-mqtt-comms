"""
MQTT client for communicating with Bambu Lab printers.

Provides raw connection management and request/response handling.
Command formatting and response parsing are left to the caller (e.g., bambu-mqtt-generator).
"""

import json
import random
import ssl
import time
import threading
from typing import Any, Dict, Optional, Union

import paho.mqtt.client as mqtt

from .config import PrinterConfig


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
        
        # Push status waiting
        self._status_data: Optional[Dict] = None
        self._status_lock = threading.Lock()
        self._status_event = threading.Event()
    
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
        if "print" in data and data["print"].get("command") == "push_status":
            with self._status_lock:
                self._status_data = data["print"]
            self._status_event.set()
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
    
    def request_status(self, sequence_id: Optional[str] = None) -> Dict:
        """
        Request a full status push (pushall) and wait for push_status response.
        
        This uses a special mechanism: sends pushall as fire-and-forget, then waits for push_status
        which is delivered asynchronously and doesn't share the request's sequence_id.
        
        Args:
            sequence_id: Optional custom sequence_id for the pushall request.
            
        Returns:
            The push_status response data.
        """
        seq = sequence_id or self.random_sequence_id()
        payload = {"pushing": {"sequence_id": seq, "command": "pushall", "version": 1, "push_target": 1}}
        
        # Clear previous status
        self._status_event.clear()
        with self._status_lock:
            self._status_data = None
        
        # Send pushall as fire-and-forget (no response expected with same seq_id)
        self.publish(payload)
        
        # Wait for push_status (async, different sequence_id)
        if not self._status_event.wait(timeout=15.0):
            raise TimeoutError("Timeout waiting for push_status response")
        
        with self._status_lock:
            return self._status_data or {}
    
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
                "timestamp": int(time.time()),
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
    
    def send_and_wait(
        self,
        payload: Union[Dict, str],
        timeout: Optional[float] = None,
        qos: int = 1,
    ) -> Dict:
        """
        Send a payload and wait for the matching response.
        
        Matches response by sequence_id in the payload.
        
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
        
        # Parse payload if string
        if isinstance(payload, str):
            payload_dict = json.loads(payload)
        else:
            payload_dict = payload
        
        # Extract sequence_id
        seq_id = self._extract_sequence_id(payload_dict)
        if not seq_id:
            raise ValueError("Payload must contain a sequence_id")
        
        # Register pending request
        event = threading.Event()
        with self._pending_lock:
            self._pending[seq_id] = {"event": event, "response": None, "timestamp": time.time()}
        
        # Send
        wire_msg = json.dumps(payload_dict, separators=(",", ":"))
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