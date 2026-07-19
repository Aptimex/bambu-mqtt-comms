"""
Configuration classes for Bambu MQTT client.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PrinterConfig:
    """Configuration for connecting to a Bambu printer via MQTT."""
    
    ip: str
    """Printer IP address or hostname."""
    
    serial: str
    """Printer serial number."""
    
    access_code: str
    """Printer access code (from printer display)."""
    
    port: int = 8883
    """MQTT port (default: 8883 for TLS)."""
    
    username: str = "bblp"
    """MQTT username (default: 'bblp')."""
    
    keepalive: int = 60
    """MQTT keepalive interval in seconds."""
    
    response_timeout: float = 10.0
    """Default timeout for waiting for command responses."""
    
    @property
    def request_topic(self) -> str:
        """MQTT topic for sending commands to printer."""
        return f"device/{self.serial}/request"
    
    @property
    def report_topic(self) -> str:
        """MQTT topic for receiving responses from printer."""
        return f"device/{self.serial}/report"


@dataclass
class SigningConfig:
    """Configuration for MQTT message signing."""
    
    cert_pem: Optional[str] = None
    """Leaf certificate PEM string (required for signing)."""
    
    key_pem: Optional[str] = None
    """Private key PEM string (required for signing)."""
    
    cert_pem_path: Optional[str] = None
    """Path to leaf certificate file (alternative to cert_pem)."""
    
    key_pem_path: Optional[str] = None
    """Path to private key file (alternative to key_pem)."""
    
    cert_chain_pem: Optional[str] = None
    """Full certificate chain PEM (leaf + intermediate + root). Required for app_cert_install bootstrap."""
    
    crl_pem: Optional[str] = None
    """Certificate Revocation List PEM. Required for app_cert_install bootstrap."""
    
    cert_chain_pem_path: Optional[str] = None
    """Path to certificate chain file."""
    
    crl_pem_path: Optional[str] = None
    """Path to CRL file."""
    
    def __post_init__(self):
        # Load from file paths if provided
        if self.cert_pem_path and not self.cert_pem:
            with open(self.cert_pem_path, "r") as f:
                self.cert_pem = f.read()
        if self.key_pem_path and not self.key_pem:
            with open(self.key_pem_path, "r") as f:
                self.key_pem = f.read()
        if self.cert_chain_pem_path and not self.cert_chain_pem:
            with open(self.cert_chain_pem_path, "r") as f:
                self.cert_chain_pem = f.read()
        if self.crl_pem_path and not self.crl_pem:
            with open(self.crl_pem_path, "r") as f:
                self.crl_pem = f.read()
    
    @property
    def can_sign(self) -> bool:
        """Whether this config has the minimum required credentials for signing."""
        return bool(self.cert_pem and self.key_pem)
    
    @property
    def can_bootstrap(self) -> bool:
        """Whether this config has credentials for app_cert_install bootstrap."""
        return self.can_sign and bool(self.cert_chain_pem and self.crl_pem)