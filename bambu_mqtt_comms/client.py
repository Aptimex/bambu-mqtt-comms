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
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

try:                                    # Protocol landed in typing in 3.8
    from typing import Protocol
except ImportError:                     # pragma: no cover
    Protocol = object                   # type: ignore[assignment,misc]

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


# Shortest gap allowed between two pushalls to the same printer, matching
# Bambu Studio's REQUEST_PUSH_MIN_TIME (DeviceManager.hpp) and the override in
# MachineObject::command_request_push_all, which drops a too-soon request
# unless the caller passes request_now.
#
# Although Bambu Studio uses this value, some older printers actually require 
# much longer delays in practice (~30s) or risk becoming unresponsive/unstable. 
REQUEST_PUSH_MIN_INTERVAL = 3.0


class BambuMQTTError(Exception):
    """Base exception for Bambu MQTT client errors."""
    pass


class ConnectionError(BambuMQTTError):
    """Connection-related errors."""
    pass


class TimeoutError(BambuMQTTError):
    """Timeout waiting for response."""
    pass


# The printer's "you did not sign a command that needs signing" code. This is
# deliberately duplicated from bambu-mqtt-generator's SIGNATURE_REQUIRED_ERR
# rather than imported: this library knows how to talk to a printer and nothing
# about payload construction, and taking a dependency on the generator just to
# share one integer would couple the two for no benefit.
SIGNATURE_REQUIRED_ERR = 84033543

# Prefix for the throwaway command name used by probe_signing_required(). The
# random suffix keeps it from ever colliding with a real command, including one
# Bambu might add in future firmware.
_SIGNING_PROBE_PREFIX = "bmqtt_sign_probe_"

# How long to wait for a certificate bootstrap started at connect time before
# giving up on it and sending unsigned. install_app_cert() polls for up to
# roughly 30s, which is what slower printers actually take.
BOOTSTRAP_WAIT_TIMEOUT = 45.0


class SignerProtocol(Protocol):
    """What this library needs from a signer, structurally.

    bambu-mqtt-generator's MQTTSigner satisfies this as-is. Typing it as a
    protocol rather than importing that class keeps the two libraries
    independent: the caller owns the credentials and the payload format, and
    this library only ever asks for bytes to put on the wire.
    """

    def sign(self, payload: Dict) -> str: ...

    def get_cert_id(self) -> str: ...

    def build_app_cert_install(self, sequence_id: Optional[str] = None) -> str: ...


@dataclass
class SigningState:
    """What we have worked out about a printer's signing policy.

    Lives and dies with one connection. The printer forgets registered
    certificates when it power-cycles, and a user can toggle Developer mode
    between sessions, so none of this is worth carrying across connections.
    """

    # Does this printer reject unsigned commands? None until determined.
    required: Optional[bool] = None
    # True when the probe could not get an answer and `required` is a guess.
    probe_inconclusive: bool = False
    # How we found out: "probe" at connect time, "rejection" from a real command.
    detected_by: str = ""
    # Whether a signer was supplied at all.
    credentials_available: bool = False
    # Printer answered app_cert_list (None = never asked or no answer).
    supported: Optional[bool] = None
    # Printer currently trusts our certificate, so signed commands will verify.
    cert_trusted: bool = False
    # A registration is in flight on a background thread.
    bootstrap_pending: bool = False
    # Why registration failed, if it did.
    bootstrap_error: Optional[str] = None

    def describe(self) -> Optional[str]:
        """A one-line explanation of anything the user needs to act on.

        Returns None when there is nothing to say, which is the usual case:
        a printer that does not require signing needs no credentials and
        produces no message, whether or not any were supplied.
        """
        if not self.required:
            return None

        if not self.credentials_available:
            return (
                "This printer requires signed commands. Either enable LAN Mode "
                "and Developer mode on the printer, or supply signing "
                "credentials (key_pem_file, cert_chain_pem_file, crl_pem_file)."
            )

        if self.cert_trusted:
            return None

        if self.bootstrap_pending:
            return (
                "This printer requires signed commands; registering the signing "
                "certificate with it. This can take up to 30 seconds."
            )

        if self.bootstrap_error:
            return (
                "This printer requires signed commands, but registering the "
                f"signing certificate failed: {self.bootstrap_error}."
            )

        return (
            "This printer requires signed commands, but its signing certificate "
            "is not registered, so commands will be rejected."
        )


def find_err_code(response: Dict[str, Any]) -> Optional[int]:
    """Pull an err_code out of a printer response, whichever scope carries it."""
    if not isinstance(response, dict):
        return None
    for section in response.values():
        if isinstance(section, dict) and "err_code" in section:
            try:
                return int(section["err_code"])
            except (TypeError, ValueError):
                continue
    return None


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
    
    def __init__(
        self,
        config: PrinterConfig,
        signer: Optional["SignerProtocol"] = None,
    ):
        """
        Initialize the MQTT client.
        
        Args:
            config: PrinterConfig with connection details.
            signer: Optional signer (e.g. bambu-mqtt-generator's MQTTSigner).
                Supplying one lets send_command() sign automatically on
                printers that need it. Omitting it is entirely normal: most
                printers are in LAN + Developer mode and accept unsigned
                commands, and this client never needs credentials for them.
        """
        self.config = config
        self._signer = signer

        # Signing policy for the current connection; see _detect_signing().
        self.signing = SigningState(credentials_available=signer is not None)
        self._bootstrap_done = threading.Event()
        self._bootstrap_done.set()
        self._bootstrap_lock = threading.Lock()
        
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
        # When this printer was last sent a pushall. Kept on the client rather
        # than reset per connection, so reconnecting cannot be used to ask
        # again immediately. See REQUEST_PUSH_MIN_INTERVAL.
        self._pushall_at: float = 0.0
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
    
    def connect(
        self,
        timeout: float = 15.0,
        prime_status: bool = True,
        detect_signing: bool = True,
    ) -> None:
        """
        Connect to the printer's MQTT broker.

        Unless prime_status is False, one pushall is sent once connected so
        get_status() has something to report. This is not just an optimization:
        the accumulated status is only ever seeded by a full report, and some
        printers send none unprompted, so without it there would be nothing
        for those reports to accumulate onto and get_status() would keep
        returning None for the life of the connection.

        A printer that does not answer the pushall is not treated as a failed
        connection: the request is throttled like any other, so the next caller
        to want status will ask again.

        Signing policy is worked out here too, unless detect_signing is False.
        It costs one round trip and no credentials; see _detect_signing().

        Args:
            timeout: Connection timeout in seconds.
            prime_status: Request one full report once connected.
            detect_signing: Work out whether this printer needs signed
                commands, and start registering our certificate if it does
                and we have one.

        Raises:
            ConnectionError: If connection fails or times out.
        """
        if self._connected:
            return
        
        self._reset_signing_state()
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

        # Before prime_status, so that a certificate registration started here
        # runs while the pushall round trip is in flight rather than after it.
        if detect_signing:
            try:
                self._detect_signing()
            except Exception:
                # Detection is an optimization. A printer we could not probe is
                # handled by send_command()'s reaction to a rejection instead,
                # so nothing here is worth failing a working connection over.
                pass

        if prime_status:
            try:
                self.request_status(timeout=timeout)
            except (TimeoutError, ConnectionError):
                pass  # not fatal; the next status read will ask again
    
    def disconnect(self) -> None:
        """Disconnect from the printer."""
        if self._client:
            self._client.disconnect()
            self._client.loop_stop()
            self._client = None
        self._connected = False
        self._reset_signing_state()
    
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
        self,
        sequence_id: Optional[str] = None,
        timeout: float = 15.0,
        force: bool = False,
    ) -> Dict:
        """
        Request a full status push (pushall) and wait for the push_status reply.

        The reply is asynchronous and does not carry the request's sequence_id,
        so it is matched by type rather than id. The printer also emits
        incremental push_status diffs on the same topic, which contain only the
        fields that changed; those are skipped here, because a caller asking
        for status wants the complete picture (a diff arriving first would
        otherwise look like a printer with no AMS).

        Asking too often is what to avoid: a pushall makes the printer build
        and send a full report, and enough of them in a row makes some printers
        stop answering altogether. So a request made within
        REQUEST_PUSH_MIN_INTERVAL of the last one returns the accumulated
        status instead of going to the printer, the way Bambu Studio's
        command_request_push_all drops one and returns -1. Callers who MUST get
        a genuinely fresh report pass force=True, its version of request_now.

        Args:
            sequence_id: Optional custom sequence_id for the pushall request.
            timeout: How long to wait for the full report.
            force: Send even if the last pushall was too recent.

        Returns:
            The full push_status response data. When the request is throttled
            this is the accumulated status, which is not a full report but is
            the most current picture available; it is empty only if nothing has
            been received at all.

        Raises:
            TimeoutError: If no full push_status arrives within the timeout.
        """
        with self._status_lock:
            since = time.monotonic() - self._pushall_at
        if not force and since < REQUEST_PUSH_MIN_INTERVAL:
            current = self.get_status()
            if current is not None:
                return current

        seq = sequence_id or self.random_sequence_id()
        payload = {"pushing": {"sequence_id": seq, "command": "pushall", "version": 1, "push_target": 1}}

        # Clear previous status
        self._status_event.clear()
        self._full_status_event.clear()
        with self._status_lock:
            self._status_data = None
            self._full_status_data = None

        # Recorded before the wait, so a printer that never answers still
        # counts as having been asked and cannot be asked again immediately.
        with self._status_lock:
            self._pushall_at = time.monotonic()

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

    # ── Signing policy ────────────────────────────────────────────────────────

    @property
    def can_sign(self) -> bool:
        """Whether a signed command would actually verify right now."""
        return self._signer is not None and self.signing.cert_trusted

    def _reset_signing_state(self) -> None:
        """Forget everything learned about signing. Called per connection."""
        self.signing = SigningState(
            credentials_available=self._signer is not None
        )
        self._bootstrap_done.set()

    def probe_signing_required(self, timeout: Optional[float] = None) -> Optional[bool]:
        """
        Ask the printer whether it requires signed commands.

        Publishes an unsigned message whose command name is randomly generated
        and therefore matches nothing in the firmware. The printer checks the
        signature before it looks up the command name, so:

          * a printer that requires signing answers SIGNATURE_REQUIRED_ERR
          * a printer that does not answers success (having found nothing to do)

        Because no such command exists there is no handler to reach, so this is
        safe to send in any printer state, including mid-print. It needs no
        certificate and no bootstrap, which is the whole point: it tells us
        whether paying for a registration is necessary before we pay for one.

        Args:
            timeout: Response timeout (default: config.response_timeout).

        Returns:
            True if signing is required, False if not, None if the printer did
            not answer. Callers should treat None as "not required" and rely on
            send_command() reacting to a rejection instead.
        """
        payload = {
            "print": {
                "command": f"{_SIGNING_PROBE_PREFIX}{random.randrange(16 ** 8):08x}",
                "sequence_id": self.random_sequence_id(),
            }
        }
        try:
            response = self.send_and_wait(payload, timeout=timeout)
        except (TimeoutError, ConnectionError):
            return None
        return find_err_code(response) == SIGNATURE_REQUIRED_ERR

    def _detect_signing(self) -> None:
        """Work out this printer's signing policy, cheaply, at connect time.

        The ladder is ordered so that the expensive step is only ever reached
        by a printer that actually needs it:

          1. probe (one round trip, no credentials) - most printers stop here
          2. app_cert_list, only if signing is required: our certificate may
             already be registered from earlier this power cycle, in which case
             there is nothing to do
          3. app_cert_install on a background thread, only if it is not
        """
        required = self.probe_signing_required()
        self.signing.probe_inconclusive = required is None
        self.signing.required = bool(required)
        self.signing.detected_by = "probe" if required is not None else ""

        # Nothing further to do for a printer that accepts unsigned commands —
        # which is the common case, and costs exactly the one probe above.
        if not self.signing.required:
            return

        # Signing is required but we have no credentials. Not an error here:
        # the caller may not care, and send_command() will explain itself if a
        # command is actually rejected.
        if self._signer is None:
            return

        try:
            certs = self.get_trusted_certs()
            self.signing.supported = True
            if self._signer.get_cert_id() in certs.get("cert_ids", []):
                self.signing.cert_trusted = True
                return
        except (TimeoutError, ConnectionError, BambuMQTTError):
            # Couldn't read the list; fall through and try registering anyway.
            pass

        self._start_bootstrap()

    def _start_bootstrap(self) -> None:
        """Register our certificate on a background thread.

        Backgrounded because this is the one slow step in the whole process:
        the printer acknowledges nothing, so install_app_cert() polls, and some
        printers take close to 30 seconds to report the certificate as
        trusted. Callers that are about to send a signed command wait via
        await_signing_ready(); everything else proceeds immediately.
        """
        with self._bootstrap_lock:
            if self.signing.bootstrap_pending:
                return
            self.signing.bootstrap_pending = True
            self.signing.bootstrap_error = None
            self._bootstrap_done.clear()

        threading.Thread(
            target=self._run_bootstrap,
            name=f"bambu-cert-bootstrap-{self.config.serial}",
            daemon=True,
        ).start()

    def _run_bootstrap(self) -> None:
        cert_id = None
        try:
            cert_id = self._signer.get_cert_id()
            result = self.install_app_cert(
                self._signer.build_app_cert_install(), cert_id=cert_id
            )
            self.signing.supported = True
            self.signing.cert_trusted = bool(result.get("trusted"))
            if not self.signing.cert_trusted:
                self.signing.bootstrap_error = (
                    f"printer did not report certificate {cert_id} as trusted "
                    f"after {result.get('attempts_used')} checks"
                )
        except Exception as e:
            self.signing.cert_trusted = False
            self.signing.bootstrap_error = f"{type(e).__name__}: {e}"
        finally:
            self.signing.bootstrap_pending = False
            self._bootstrap_done.set()

    def await_signing_ready(self, timeout: float = BOOTSTRAP_WAIT_TIMEOUT) -> bool:
        """Wait for any in-flight certificate registration to settle.

        Args:
            timeout: Seconds to wait.

        Returns:
            True if a signed command would now verify.
        """
        self._bootstrap_done.wait(timeout)
        return self.can_sign

    def signing_status_message(self) -> Optional[str]:
        """A user-facing explanation of the signing situation, or None.

        None means there is nothing the user needs to know or do, which is the
        normal outcome for a printer in LAN + Developer mode with no
        credentials configured.
        """
        return self.signing.describe()

    def send_command(
        self,
        payload: Union[Dict, str],
        timeout: Optional[float] = None,
        retry_signed: bool = True,
    ) -> Dict:
        """
        Send a command, signing it if this printer needs that, and wait.

        This is the method to use for anything the printer might refuse over
        signing. It signs when signing is both required and possible, and if a
        command still comes back rejected for want of a signature it records
        that, registers the certificate, and retries once. That last part is
        the backstop for a printer whose policy changed since we connected, or
        one the connect-time probe could not reach.

        A printer that does not require signing is sent the payload untouched,
        so a caller with no credentials is fully supported and pays nothing.

        Args:
            payload: The command, as a dict or a pre-serialized JSON string.
                Signing needs a dict; a string is parsed first, and the signer
                re-serializes it canonically.
            timeout: Response timeout (default: config.response_timeout).
            retry_signed: Register and retry once on a signature rejection.

        Returns:
            The printer's response dict. Signing problems are reported through
            the response's err_code and signing_status_message(), not raised,
            so callers handle them alongside every other command failure.

        Raises:
            TimeoutError: If no response is received.
            ConnectionError: If not connected.
        """
        if not self.is_connected:
            raise ConnectionError("Not connected. Call connect() first.")

        # Don't send unsigned while a registration we know is needed is still
        # in flight — waiting is strictly better than a guaranteed rejection.
        if self.signing.required and self._signer is not None:
            self.await_signing_ready()

        response = self.send_and_wait(self._wire(payload), timeout=timeout)

        if find_err_code(response) != SIGNATURE_REQUIRED_ERR:
            return response

        # The printer just told us, authoritatively, that it needs signatures.
        # Only claim credit for finding that out here if the probe had not
        # already established it, so detected_by stays diagnostic.
        if not self.signing.required:
            self.signing.detected_by = "rejection"
        self.signing.required = True
        self.signing.probe_inconclusive = False

        if not retry_signed or self._signer is None:
            return response

        if not self.can_sign:
            self._start_bootstrap()
            self.await_signing_ready()
        if not self.can_sign:
            return response

        return self.send_and_wait(self._wire(payload), timeout=timeout)

    def _wire(self, payload: Union[Dict, str]) -> Union[Dict, str]:
        """Sign the payload if we can, otherwise hand it back unchanged."""
        if not self.can_sign:
            return payload
        as_dict = json.loads(payload) if isinstance(payload, str) else payload
        return self._signer.sign(as_dict)

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