#!/usr/bin/env python3
import argparse
import base64
import json
import logging
import os
import queue
import signal
import socket
import sys
import threading
import time
import uuid
from typing import Dict, Optional, Tuple

import paho.mqtt.client as mqtt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class Encryptor:
    """Handles AES-GCM encryption/decryption with key derivation"""

    def __init__(self, password: Optional[str] = None, salt: bytes = b"", iterations: int = 100000):
        self.key = None
        if password:
            self.derive_key(password.encode(), salt, iterations)

    def derive_key(self, password: bytes, salt: bytes, iterations: int):
        """Derive a key from password using PBKDF2"""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=iterations,
            backend=default_backend(),
        )
        self.key = kdf.derive(password)

    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt data with AES-GCM"""
        if not self.key:
            return plaintext

        nonce = os.urandom(12)
        aesgcm = AESGCM(self.key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return nonce + ciphertext

    def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt data with AES-GCM"""
        if not self.key or len(ciphertext) < 12:
            return ciphertext

        nonce = ciphertext[:12]
        ciphertext = ciphertext[12:]
        aesgcm = AESGCM(self.key)
        try:
            return aesgcm.decrypt(nonce, ciphertext, None)
        except Exception as e:
            logging.error(f"Decryption failed: {str(e)}")
            return b""


class MQTTTunnel:
    def __init__(
        self, profiles_file: str, profile_name: str, topic_prefix: str, debug: bool = False
    ):
        self.debug = debug
        self._setup_logging()

        self.profiles = self.load_profiles(profiles_file)
        self.profile = self.profiles.get(profile_name)
        if not self.profile:
            raise ValueError(f"Profile '{profile_name}' not found")

        self.topic_prefix = topic_prefix
        self.keepalive = self.profile.get("keepalive", 60)
        self.connections: Dict[str, Tuple[socket.socket, mqtt.Client]] = {}
        self.control_queue = queue.Queue()
        self.shutdown_event = threading.Event()
        self.active_connections = threading.BoundedSemaphore(100)
        self.connection_timeout = 5

        # Initialize encryption
        self.encryptor = Encryptor(
            password=self.profile.get("encryption_key"),
            salt=base64.b64decode(self.profile.get("encryption_salt", "")),
            iterations=self.profile.get("encryption_iterations", 100000),
        )

    def _setup_logging(self):
        level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=level
        )
        self.log = logging.getLogger("MQTTTunnel")

        # MQTT client logging
        mqtt_logger = logging.getLogger("MQTT")
        if self.debug:
            mqtt_logger.setLevel(logging.DEBUG)
        else:
            mqtt_logger.setLevel(logging.WARNING)

    def load_profiles(self, filename: str) -> Dict:
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except Exception as e:
            self.log.error(f"Error loading profiles: {str(e)}")
            raise

    def create_mqtt_client(self, client_id: Optional[str] = None) -> mqtt.Client:
        client_id = client_id or f"mqtttunnel_{uuid.uuid4().hex[:8]}"
        client = mqtt.Client(client_id=client_id, clean_session=True)

        # Set credentials if available
        username = self.profile.get("username", "").strip() or None
        password = self.profile.get("password", "").strip() or None
        if username and password:
            client.username_pw_set(username, password)

        # Enable TLS if configured
        if self.profile.get("tls", False):
            import ssl

            tls_version_map = {
                "tlsv1": ssl.PROTOCOL_TLSv1,
                "tlsv1.1": ssl.PROTOCOL_TLSv1_1,
                "tlsv1.2": ssl.PROTOCOL_TLSv1_2,
                "tls": ssl.PROTOCOL_TLS,
            }
            tls_version = tls_version_map.get(
                self.profile.get("tls_version", "tlsv1.2"), ssl.PROTOCOL_TLS
            )

            client.tls_set(
                ca_certs=self.profile.get("ca_cert"),
                certfile=self.profile.get("client_cert"),
                keyfile=self.profile.get("client_key"),
                tls_version=tls_version,
            )
            client.tls_insecure_set(self.profile.get("tls_insecure", False))

        # Debug logging
        if self.debug:
            client.enable_logger(logging.getLogger("MQTT"))

        return client

    def setup_control_channel(self):
        self.control_client = self.create_mqtt_client()
        control_topic = f"{self.topic_prefix}/control"

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(control_topic, qos=1)
                self.log.info(
                    f"Control channel connected to {self.profile['host']}:{self.profile['port']}"
                )
            else:
                self.log.error(f"Control connection failed: {mqtt.connack_string(rc)}")

        def on_message(client, userdata, msg):
            try:
                # Decrypt payload
                payload = self.encryptor.decrypt(msg.payload)
                payload = json.loads(payload)
                self.log.debug(f"Control message: {payload}")
                self.control_queue.put(payload)
            except Exception as e:
                self.log.error(f"Invalid control message: {str(e)}")

        def on_disconnect(client, userdata, rc):
            if rc != 0 and not self.shutdown_event.is_set():
                self.log.error(
                    f"Control channel disconnected unexpectedly: {mqtt.error_string(rc)}"
                )
                self.shutdown_event.set()

        self.control_client.on_connect = on_connect
        self.control_client.on_message = on_message
        self.control_client.on_disconnect = on_disconnect

        try:
            self.control_client.connect(
                self.profile["host"], int(self.profile["port"]), self.keepalive
            )
            self.control_client.loop_start()
        except Exception as e:
            self.log.error(f"Control connection error: {str(e)}")
            raise

    def start_server(self, service_host: str, service_port: int):
        """Run in server mode: forward MQTT connections to local service"""
        self.setup_control_channel()
        self.log.info(f"Server mode: Forwarding to {service_host}:{service_port}")

        # Thread to handle control messages
        def control_handler():
            while not self.shutdown_event.is_set():
                try:
                    msg = self.control_queue.get(timeout=1)
                    if msg.get("action") == "connect":
                        conn_id = msg["conn_id"]
                        self.log.info(f"New connection request: {conn_id}")
                        if self.active_connections.acquire(blocking=False):
                            threading.Thread(
                                target=self.handle_server_connection,
                                args=(conn_id, service_host, service_port),
                                daemon=True,
                                name=f"svr-conn-{conn_id[:8]}",
                            ).start()
                        else:
                            self.log.error(f"Max connections reached, rejecting {conn_id}")
                            self.send_control_message("reject", conn_id)
                    elif msg.get("action") == "disconnect":
                        conn_id = msg["conn_id"]
                        if conn_id in self.connections:
                            self.log.info(f"Remote requested disconnect: {conn_id}")
                            self.close_connection(conn_id, notify=False)
                except queue.Empty:
                    continue

        threading.Thread(target=control_handler, daemon=True, name="ctrl-handler").start()

        # Wait for shutdown signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.log.info("Server ready. Press Ctrl+C to exit.")
        self.shutdown_event.wait()
        self.cleanup()

    def start_client(self, local_host: str, local_port: int):
        """Run in client mode: expose local port via MQTT tunnel"""
        self.setup_control_channel()
        self.log.info(f"Client mode: Listening on {local_host}:{local_port}")

        # Start TCP server
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((local_host, local_port))
        server_sock.listen(5)
        server_sock.settimeout(1)  # Make socket non-blocking with timeout

        def accept_connections():
            while not self.shutdown_event.is_set():
                try:
                    client_sock, addr = server_sock.accept()
                    conn_id = str(uuid.uuid4())
                    self.log.info(f"New client connection: {addr} ({conn_id})")
                    threading.Thread(
                        target=self.handle_client_connection,
                        args=(conn_id, client_sock),
                        daemon=True,
                        name=f"cli-conn-{conn_id[:8]}",
                    ).start()
                except socket.timeout:
                    continue
                except OSError as e:
                    if not self.shutdown_event.is_set():
                        self.log.error(f"Accept error: {str(e)}")
            server_sock.close()

        threading.Thread(target=accept_connections, daemon=True, name="accept-thread").start()

        # Wait for shutdown signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.log.info("Client ready. Press Ctrl+C to exit.")
        self.shutdown_event.wait()
        self.cleanup()

    def handle_server_connection(self, conn_id: str, service_host: str, service_port: int):
        """Server-side connection handler"""
        service_sock = None
        mqtt_client = None
        try:
            # Connect to local service with timeout
            service_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            service_sock.settimeout(self.connection_timeout)

            try:
                service_sock.connect((service_host, service_port))
            except (ConnectionRefusedError, TimeoutError) as e:
                self.log.error(f"Service connection failed: {str(e)}")
                self.send_control_message("service_unavailable", conn_id)
                return
            except Exception as e:
                self.log.error(f"Service connection error: {str(e)}")
                self.send_control_message("service_error", conn_id)
                return

            service_sock.settimeout(5)  # Reset timeout for health checks

            # Create MQTT client for this connection
            mqtt_client = self.create_mqtt_client()

            # Add events to track connection and subscription status
            connected_event = threading.Event()
            subscribed_event = threading.Event()

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    connected_event.set()
                else:
                    self.log.error(f"Connection failed: {mqtt.connack_string(rc)}")

            def on_subscribe(client, userdata, mid, granted_qos):
                subscribed_event.set()

            mqtt_client.on_connect = on_connect
            mqtt_client.on_subscribe = on_subscribe

            mqtt_client.connect(self.profile["host"], int(self.profile["port"]), self.keepalive)
            mqtt_client.loop_start()

            # Wait for MQTT connection
            if not connected_event.wait(timeout=10):
                self.log.error("MQTT connection timed out")
                raise TimeoutError("MQTT connection timeout")

            # Set up data channels
            outbound_topic = f"{self.topic_prefix}/{conn_id}/outbound"
            inbound_topic = f"{self.topic_prefix}/{conn_id}/inbound"

            # Subscribe to outbound data
            mqtt_client.subscribe(outbound_topic, qos=1)

            # Wait for subscription confirmation
            if not subscribed_event.wait(timeout=10):
                self.log.error("MQTT subscription timed out")
                raise TimeoutError("MQTT subscription timeout")

            self.log.debug(f"Server subscribed to: {outbound_topic}")
            self.log.debug(f"Server publishing to: {inbound_topic}")

            # Now we're ready to notify client
            self.send_control_message("service_ready", conn_id)

            # Store connection
            self.connections[conn_id] = (service_sock, mqtt_client)

            def mqtt_to_service():
                def on_message(client, userdata, msg):
                    if msg.topic == outbound_topic and not self.shutdown_event.is_set():
                        try:
                            # Decrypt payload before sending to service
                            payload = self.encryptor.decrypt(msg.payload)
                            self.log.debug(f"MQTT->SERVICE: {len(payload)} bytes")
                            service_sock.sendall(payload)
                        except (BrokenPipeError, ConnectionResetError) as e:
                            self.log.warning(f"Service write error: {str(e)}")
                            self.close_connection(conn_id)
                        except Exception as e:
                            self.log.error(f"Unexpected error: {str(e)}")
                            self.close_connection(conn_id)

                mqtt_client.on_message = on_message

                while not self.shutdown_event.is_set() and conn_id in self.connections:
                    time.sleep(0.1)

            def service_to_mqtt():
                while not self.shutdown_event.is_set() and conn_id in self.connections:
                    try:
                        data = service_sock.recv(4096)
                        if not data:
                            self.log.info("Service closed connection")
                            self.close_connection(conn_id)
                            break
                        # Encrypt data before sending over MQTT
                        encrypted_data = self.encryptor.encrypt(data)
                        self.log.debug(f"SERVICE->MQTT: {len(data)} bytes")
                        mqtt_client.publish(inbound_topic, encrypted_data, qos=1)
                    except socket.timeout:
                        continue  # Timeout is normal for non-blocking
                    except (ConnectionResetError, BrokenPipeError) as e:
                        self.log.warning(f"Service read error: {str(e)}")
                        self.close_connection(conn_id)
                        break
                    except BlockingIOError:
                        time.sleep(0.01)
                    except Exception as e:
                        if not self.shutdown_event.is_set():
                            self.log.error(f"Unexpected error: {str(e)}")
                        self.close_connection(conn_id)
                        break

            # Start data handlers
            threading.Thread(
                target=mqtt_to_service, daemon=True, name=f"mqtt2svc-{conn_id[:8]}"
            ).start()
            threading.Thread(
                target=service_to_mqtt, daemon=True, name=f"svc2mqtt-{conn_id[:8]}"
            ).start()

            # Monitor connection health
            while conn_id in self.connections and not self.shutdown_event.is_set():
                # Check if service socket is still connected
                try:
                    # Test if socket is still alive
                    service_sock.send(b"")
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self.log.info("Service connection lost")
                    self.close_connection(conn_id)
                    break
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        self.log.error(f"Connection health check failed: {str(e)}")
                    self.close_connection(conn_id)
                    break

                time.sleep(5)

        except Exception as e:
            self.log.error(f"Server connection setup failed: {str(e)}")
            if service_sock:
                service_sock.close()
            if mqtt_client:
                try:
                    mqtt_client.loop_stop()
                    mqtt_client.disconnect()
                except:
                    pass
            self.send_control_message("disconnect", conn_id)
        finally:
            self.active_connections.release()

    def handle_client_connection(self, conn_id: str, client_sock: socket.socket):
        """Client-side connection handler"""
        mqtt_client = None
        try:
            # Set socket timeout for disconnect detection
            client_sock.settimeout(5)

            # Store client socket temporarily
            self.connections[conn_id] = (client_sock, None)

            # Notify server about new connection
            self.send_control_message("connect", conn_id)

            # Create an event to track service availability
            service_ready = threading.Event()
            service_failed = threading.Event()

            def control_handler():
                while not (
                    service_ready.is_set()
                    or service_failed.is_set()
                    or self.shutdown_event.is_set()
                ):
                    try:
                        msg = self.control_queue.get(timeout=0.5)
                        if msg.get("action") == "service_ready" and msg["conn_id"] == conn_id:
                            self.log.info("Service connection established on server side")
                            service_ready.set()
                        elif (
                            msg.get("action") == "service_unavailable" and msg["conn_id"] == conn_id
                        ):
                            self.log.error("Service unavailable on server side")
                            service_failed.set()
                        elif msg.get("action") == "service_error" and msg["conn_id"] == conn_id:
                            self.log.error("Service connection error on server side")
                            service_failed.set()
                        elif msg.get("action") == "reject" and msg["conn_id"] == conn_id:
                            self.log.error("Connection rejected by server")
                            service_failed.set()
                    except queue.Empty:
                        continue

            # Start temporary control handler
            ctrl_thread = threading.Thread(
                target=control_handler, daemon=True, name=f"ctrl-{conn_id[:8]}"
            )
            ctrl_thread.start()

            # Wait for service to be ready or failed
            start_time = time.time()
            while not (service_ready.is_set() or service_failed.is_set()):
                # Check if we've exceeded timeout
                if time.time() - start_time > self.connection_timeout:
                    self.log.error("Service connection timed out")
                    service_failed.set()
                    break
                time.sleep(0.1)

            if service_failed.is_set():
                self.log.error("Cannot establish connection to service")
                self.close_connection(conn_id)
                return

            # Create MQTT client now that service is ready
            mqtt_client = self.create_mqtt_client()
            connected_event = threading.Event()
            subscribed_event = threading.Event()

            def on_connect(client, userdata, flags, rc):
                if rc == 0:
                    connected_event.set()
                else:
                    self.log.error(f"Connection failed: {mqtt.connack_string(rc)}")

            def on_subscribe(client, userdata, mid, granted_qos):
                subscribed_event.set()

            mqtt_client.on_connect = on_connect
            mqtt_client.on_subscribe = on_subscribe

            mqtt_client.connect(self.profile["host"], int(self.profile["port"]), self.keepalive)
            mqtt_client.loop_start()

            # Wait for MQTT connection
            if not connected_event.wait(timeout=10):
                self.log.error("MQTT connection timed out")
                raise TimeoutError("MQTT connection timeout")

            # Set up data channels
            outbound_topic = f"{self.topic_prefix}/{conn_id}/outbound"
            inbound_topic = f"{self.topic_prefix}/{conn_id}/inbound"

            # Subscribe to inbound data
            mqtt_client.subscribe(inbound_topic, qos=1)

            # Wait for subscription confirmation
            if not subscribed_event.wait(timeout=10):
                self.log.error("MQTT subscription timed out")
                raise TimeoutError("MQTT subscription timeout")

            self.log.debug(f"Client subscribed to: {inbound_topic}")
            self.log.debug(f"Client publishing to: {outbound_topic}")

            # Update connection with MQTT client
            self.connections[conn_id] = (client_sock, mqtt_client)

            def mqtt_to_client():
                def on_message(client, userdata, msg):
                    if msg.topic == inbound_topic and not self.shutdown_event.is_set():
                        try:
                            # Decrypt payload before sending to client
                            payload = self.encryptor.decrypt(msg.payload)
                            self.log.debug(f"MQTT->CLIENT: {len(payload)} bytes")
                            client_sock.sendall(payload)
                        except (BrokenPipeError, ConnectionResetError) as e:
                            self.log.warning(f"Client write error: {str(e)}")
                            self.close_connection(conn_id)
                        except Exception as e:
                            self.log.error(f"Unexpected error: {str(e)}")
                            self.close_connection(conn_id)

                mqtt_client.on_message = on_message

                while not self.shutdown_event.is_set() and conn_id in self.connections:
                    time.sleep(0.1)

            def client_to_mqtt():
                while not self.shutdown_event.is_set() and conn_id in self.connections:
                    try:
                        data = client_sock.recv(4096)
                        if not data:
                            self.log.info("Client closed connection")
                            self.close_connection(conn_id)
                            break
                        # Encrypt data before sending over MQTT
                        encrypted_data = self.encryptor.encrypt(data)
                        self.log.debug(f"CLIENT->MQTT: {len(data)} bytes")
                        mqtt_client.publish(outbound_topic, encrypted_data, qos=1)
                    except socket.timeout:
                        continue  # Timeout is normal for non-blocking
                    except (ConnectionResetError, BrokenPipeError) as e:
                        self.log.warning(f"Client read error: {str(e)}")
                        self.close_connection(conn_id)
                        break
                    except BlockingIOError:
                        time.sleep(0.01)
                    except Exception as e:
                        if not self.shutdown_event.is_set():
                            self.log.error(f"Unexpected error: {str(e)}")
                        self.close_connection(conn_id)
                        break

            # Start data handlers
            threading.Thread(
                target=mqtt_to_client, daemon=True, name=f"mqtt2cli-{conn_id[:8]}"
            ).start()
            threading.Thread(
                target=client_to_mqtt, daemon=True, name=f"cli2mqtt-{conn_id[:8]}"
            ).start()

            # Monitor connection health
            while conn_id in self.connections and not self.shutdown_event.is_set():
                # Check if client socket is still connected
                try:
                    # Test if socket is still alive
                    client_sock.send(b"")
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self.log.info("Client connection lost")
                    self.close_connection(conn_id)
                    break
                except Exception as e:
                    if not self.shutdown_event.is_set():
                        self.log.error(f"Connection health check failed: {str(e)}")
                    self.close_connection(conn_id)
                    break

                time.sleep(5)

        except Exception as e:
            self.log.error(f"Client connection setup failed: {str(e)}")
            if conn_id in self.connections:
                client_sock = self.connections[conn_id][0]
                if client_sock:
                    client_sock.close()
                del self.connections[conn_id]
            self.send_control_message("disconnect", conn_id)

    def send_control_message(self, action: str, conn_id: str):
        payload = json.dumps(
            {"action": action, "conn_id": conn_id, "timestamp": time.time()}
        ).encode()

        # Encrypt control messages
        encrypted_payload = self.encryptor.encrypt(payload)

        topic = f"{self.topic_prefix}/control"
        result = self.control_client.publish(topic, encrypted_payload, qos=1)
        if self.debug:
            try:
                result.wait_for_publish(timeout=1)
            except:
                pass
            self.log.debug(f"Sent control: {action} for {conn_id}")

    def close_connection(self, conn_id: str, notify: bool = True):
        if conn_id in self.connections:
            self.log.info(f"Closing connection: {conn_id}")
            sock, mqtt_client = self.connections[conn_id]
            try:
                # Stop MQTT client properly
                if mqtt_client:
                    mqtt_client.loop_stop()
                    mqtt_client.disconnect()
            except Exception as e:
                self.log.warning(f"MQTT cleanup error: {str(e)}")
            try:
                sock.close()
            except Exception as e:
                self.log.warning(f"Socket close error: {str(e)}")
            del self.connections[conn_id]
            if notify:
                self.send_control_message("disconnect", conn_id)

    def signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        self.log.info(f"Received {signame}, shutting down...")
        self.shutdown_event.set()

    def cleanup(self):
        self.log.info("Cleaning up resources...")
        # Close all active connections
        for conn_id in list(self.connections.keys()):
            self.close_connection(conn_id, notify=False)

        # Clean up control client
        if hasattr(self, "control_client"):
            try:
                self.control_client.loop_stop()
                self.control_client.disconnect()
            except:
                pass
        self.log.info("Cleanup complete")


def main():
    parser = argparse.ArgumentParser(
        description="MQTT Tunnel - Secure network tunneling over MQTT",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("mode", choices=["server", "client"], help="Operation mode")
    parser.add_argument("profiles_file", help="Profiles JSON file")
    parser.add_argument("profile_name", help="Profile to use")
    parser.add_argument("topic_prefix", help="Base topic for tunnel")

    # Debug argument
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Server mode arguments
    server_group = parser.add_argument_group("Server mode arguments")
    server_group.add_argument("--service-host", default="localhost", help="Service host")
    server_group.add_argument("--service-port", type=int, help="Service port")

    # Client mode arguments
    client_group = parser.add_argument_group("Client mode arguments")
    client_group.add_argument("--local-host", default="localhost", help="Local bind host")
    client_group.add_argument("--local-port", type=int, help="Local bind port")

    args = parser.parse_args()

    # Validate mode-specific arguments
    if args.mode == "server" and not args.service_port:
        parser.error("--service-port is required for server mode")
    if args.mode == "client" and not args.local_port:
        parser.error("--local-port is required for client mode")

    try:
        tunnel = MQTTTunnel(
            args.profiles_file, args.profile_name, args.topic_prefix, debug=args.debug
        )

        if args.mode == "server":
            tunnel.start_server(args.service_host, args.service_port)
        else:
            tunnel.start_client(args.local_host, args.local_port)

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
