#!/usr/bin/env python3
import argparse
import json
import logging
import queue
import signal
import socket
import sys
import threading
import time
import uuid
from typing import Dict, Optional, Tuple

import paho.mqtt.client as mqtt


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
        self.active_connections = threading.BoundedSemaphore(100)  # Limit concurrent connections
        self.connection_timeout = 5  # Seconds to wait for service connection

    def _setup_logging(self):
        level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=level)
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
        client = mqtt.Client(client_id=client_id, clean_session=True)
        if "username" in self.profile and "password" in self.profile:
            username = self.profile["username"].strip() or None
            password = self.profile["password"].strip() or None
            client.username_pw_set(username, password)

        # Enable TLS if configured
        if self.profile.get("tls", False):
            import ssl

            tls_version = {
                "tlsv1": ssl.PROTOCOL_TLSv1,
                "tlsv1.1": ssl.PROTOCOL_TLSv1_1,
                "tlsv1.2": ssl.PROTOCOL_TLSv1_2,
                "tls": ssl.PROTOCOL_TLS,
            }.get(self.profile.get("tls_version", "tls"), ssl.PROTOCOL_TLS)

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
        self.control_client = self.create_mqtt_client(client_id=f"control_{uuid.uuid4().hex[:8]}")
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
                payload = json.loads(msg.payload)
                self.log.debug(f"Control message: {payload}")
                self.control_queue.put(payload)
            except Exception as e:
                self.log.error(f"Invalid control message: {str(e)}")

        self.control_client.on_connect = on_connect
        self.control_client.on_message = on_message
        self.control_client.on_disconnect = lambda c, u, rc: self.log.warning(
            f"Control channel disconnected: {mqtt.error_string(rc)}"
        )

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

        threading.Thread(target=control_handler, daemon=True).start()

        # Wait for shutdown signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
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

        # Make socket non-blocking
        server_sock.settimeout(1)

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
                    ).start()
                except socket.timeout:
                    continue
                except OSError as e:
                    if not self.shutdown_event.is_set():
                        self.log.error(f"Accept error: {str(e)}")

        threading.Thread(target=accept_connections, daemon=True).start()

        # Wait for shutdown signal
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.shutdown_event.wait()
        server_sock.close()
        self.cleanup()

    def handle_server_connection(self, conn_id: str, service_host: str, service_port: int):
        """Server-side connection handler"""
        service_sock = None
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
            self.send_control_message("service_ready", conn_id)

            # Create MQTT client for this connection
            mqtt_client = self.create_mqtt_client(client_id=f"srv_{conn_id[:8]}")
            mqtt_client.connect(self.profile["host"], int(self.profile["port"]), self.keepalive)
            mqtt_client.loop_start()

            # Store connection
            self.connections[conn_id] = (service_sock, mqtt_client)

            # Set up data channels
            outbound_topic = f"{self.topic_prefix}/{conn_id}/outbound"  # Client -> Server
            inbound_topic = f"{self.topic_prefix}/{conn_id}/inbound"  # Server -> Client

            # Subscribe to outbound data (client->server)
            mqtt_client.subscribe(outbound_topic, qos=1)
            self.log.debug(f"Server subscribed to: {outbound_topic}")
            self.log.debug(f"Server publishing to: {inbound_topic}")

            def mqtt_to_service():
                def on_message(client, userdata, msg):
                    if msg.topic == outbound_topic:
                        try:
                            self.log.debug(f"MQTT->SERVICE: {len(msg.payload)} bytes")
                            service_sock.sendall(msg.payload)
                        except (BrokenPipeError, ConnectionResetError) as e:
                            self.log.warning(f"Service write error: {str(e)}")
                            self.close_connection(conn_id)
                        except Exception as e:
                            self.log.error(f"Unexpected error: {str(e)}")
                            self.close_connection(conn_id)

                mqtt_client.on_message = on_message

                while not self.shutdown_event.is_set():
                    if conn_id not in self.connections:
                        break
                    time.sleep(0.1)

            def service_to_mqtt():
                while not self.shutdown_event.is_set():
                    if conn_id not in self.connections:
                        break
                    try:
                        data = service_sock.recv(4096)
                        if not data:
                            self.log.info("Service closed connection")
                            self.close_connection(conn_id)
                            break
                        self.log.debug(f"SERVICE->MQTT: {len(data)} bytes")
                        mqtt_client.publish(inbound_topic, data, qos=1)
                    except socket.timeout:
                        continue  # Timeout is normal for non-blocking
                    except (ConnectionResetError, BrokenPipeError) as e:
                        self.log.warning(f"Service read error: {str(e)}")
                        self.close_connection(conn_id)
                        break
                    except BlockingIOError:
                        time.sleep(0.01)
                    except Exception as e:
                        self.log.error(f"Unexpected error: {str(e)}")
                        self.close_connection(conn_id)
                        break

            # Start data handlers
            threading.Thread(target=mqtt_to_service, daemon=True).start()
            threading.Thread(target=service_to_mqtt, daemon=True).start()

            # Monitor connection health
            while conn_id in self.connections:
                # Check if service socket is still connected
                try:
                    # Test if socket is still alive
                    service_sock.send(b"")
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self.log.info("Service connection lost")
                    self.close_connection(conn_id)
                    break
                except Exception as e:
                    self.log.error(f"Connection health check failed: {str(e)}")
                    self.close_connection(conn_id)
                    break

                time.sleep(5)

        except Exception as e:
            self.log.error(f"Server connection setup failed: {str(e)}")
            if service_sock:
                service_sock.close()
            self.send_control_message("disconnect", conn_id)
        finally:
            self.active_connections.release()

    def handle_client_connection(self, conn_id: str, client_sock: socket.socket):
        """Client-side connection handler"""
        try:
            # Set socket timeout for disconnect detection
            client_sock.settimeout(5)

            # Create MQTT client for this connection
            mqtt_client = self.create_mqtt_client(client_id=f"cli_{conn_id[:8]}")
            mqtt_client.connect(self.profile["host"], int(self.profile["port"]), self.keepalive)
            mqtt_client.loop_start()

            # Store connection
            self.connections[conn_id] = (client_sock, mqtt_client)

            # Set up data channels
            outbound_topic = f"{self.topic_prefix}/{conn_id}/outbound"  # Client -> Server
            inbound_topic = f"{self.topic_prefix}/{conn_id}/inbound"  # Server -> Client

            # Subscribe to inbound data (server->client)
            mqtt_client.subscribe(inbound_topic, qos=1)
            self.log.debug(f"Client subscribed to: {inbound_topic}")
            self.log.debug(f"Client publishing to: {outbound_topic}")

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
                    except queue.Empty:
                        continue

            # Start temporary control handler
            threading.Thread(target=control_handler, daemon=True).start()

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

            def mqtt_to_client():
                def on_message(client, userdata, msg):
                    if msg.topic == inbound_topic:
                        try:
                            self.log.debug(f"MQTT->CLIENT: {len(msg.payload)} bytes")
                            client_sock.sendall(msg.payload)
                        except (BrokenPipeError, ConnectionResetError) as e:
                            self.log.warning(f"Client write error: {str(e)}")
                            self.close_connection(conn_id)
                        except Exception as e:
                            self.log.error(f"Unexpected error: {str(e)}")
                            self.close_connection(conn_id)

                mqtt_client.on_message = on_message

                while not self.shutdown_event.is_set():
                    if conn_id not in self.connections:
                        break
                    time.sleep(0.1)

            def client_to_mqtt():
                while not self.shutdown_event.is_set():
                    if conn_id not in self.connections:
                        break
                    try:
                        data = client_sock.recv(4096)
                        if not data:
                            self.log.info("Client closed connection")
                            self.close_connection(conn_id)
                            break
                        self.log.debug(f"CLIENT->MQTT: {len(data)} bytes")
                        mqtt_client.publish(outbound_topic, data, qos=1)
                    except socket.timeout:
                        continue  # Timeout is normal for non-blocking
                    except (ConnectionResetError, BrokenPipeError) as e:
                        self.log.warning(f"Client read error: {str(e)}")
                        self.close_connection(conn_id)
                        break
                    except BlockingIOError:
                        time.sleep(0.01)
                    except Exception as e:
                        self.log.error(f"Unexpected error: {str(e)}")
                        self.close_connection(conn_id)
                        break

            # Start data handlers
            threading.Thread(target=mqtt_to_client, daemon=True).start()
            threading.Thread(target=client_to_mqtt, daemon=True).start()

            # Monitor connection health
            while conn_id in self.connections:
                # Check if client socket is still connected
                try:
                    # Test if socket is still alive
                    client_sock.send(b"")
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self.log.info("Client connection lost")
                    self.close_connection(conn_id)
                    break
                except Exception as e:
                    self.log.error(f"Connection health check failed: {str(e)}")
                    self.close_connection(conn_id)
                    break

                time.sleep(5)

        except Exception as e:
            self.log.error(f"Client connection setup failed: {str(e)}")
            client_sock.close()
            self.send_control_message("disconnect", conn_id)

    def send_control_message(self, action: str, conn_id: str):
        payload = json.dumps({"action": action, "conn_id": conn_id, "timestamp": time.time()})
        result = self.control_client.publish(f"{self.topic_prefix}/control", payload, qos=1)
        if self.debug:
            result.wait_for_publish()
            self.log.debug(f"Sent control message: {action} for {conn_id}")

    def close_connection(self, conn_id: str, notify: bool = True):
        if conn_id in self.connections:
            self.log.info(f"Closing connection: {conn_id}")
            sock, mqtt_client = self.connections[conn_id]
            try:
                sock.close()
            except Exception as e:
                self.log.warning(f"Socket close error: {str(e)}")
            try:
                mqtt_client.disconnect()
            except Exception as e:
                self.log.warning(f"MQTT disconnect error: {str(e)}")
            del self.connections[conn_id]
            if notify:
                self.send_control_message("disconnect", conn_id)

    def signal_handler(self, signum, frame):
        self.log.info(f"Received signal {signum}, shutting down...")
        self.shutdown_event.set()

    def cleanup(self):
        self.log.info("Cleaning up resources...")
        # Close all active connections
        for conn_id in list(self.connections.keys()):
            self.close_connection(conn_id, notify=False)

        # Clean up control client
        try:
            self.control_client.disconnect()
        except:
            pass
        self.log.info("Cleanup complete")


def main():
    parser = argparse.ArgumentParser(description="MQTT Tunnel")
    parser.add_argument("mode", choices=["server", "client"], help="Operation mode")
    parser.add_argument("profiles_file", help="Profiles JSON file")
    parser.add_argument("profile_name", help="Profile to use")
    parser.add_argument("topic_prefix", help="Base topic for tunnel")

    # Debug argument
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Server mode arguments
    parser.add_argument("--service-host", default="localhost", help="Service host (server mode)")
    parser.add_argument(
        "--service-port",
        type=int,
        required=("server" in sys.argv),
        help="Service port (server mode)",
    )

    # Client mode arguments
    parser.add_argument("--local-host", default="localhost", help="Local bind host (client mode)")
    parser.add_argument(
        "--local-port",
        type=int,
        required=("client" in sys.argv),
        help="Local bind port (client mode)",
    )

    args = parser.parse_args()

    try:
        tunnel = MQTTTunnel(
            args.profiles_file, args.profile_name, args.topic_prefix, debug=args.debug
        )

        if args.mode == "server":
            if not args.service_port:
                parser.error("--service-port is required for server mode")
            tunnel.start_server(args.service_host, args.service_port)
        else:
            if not args.local_port:
                parser.error("--local-port is required for client mode")
            tunnel.start_client(args.local_host, args.local_port)

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
