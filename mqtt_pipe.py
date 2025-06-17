#!/usr/bin/env python3
import argparse
import json
import select
import signal
import ssl
import sys
import time

import paho.mqtt.client as mqtt

# Default values
DEFAULT_CHUNK_SIZE = 1024 * 64
DEFAULT_QOS = 0
DEFAULT_KEEPALIVE = 60
DEFAULT_MAX_PENDING = 10  # Maximum in-flight messages for throttling
DEFAULT_QOS0_DELAY_MS = 1  # 1ms default delay for QoS 0


def load_profiles(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(userdata["subscribe_topic"], qos=userdata["qos"])
        sys.stderr.write(
            f"Connected to broker, subscribed to {userdata['subscribe_topic']} (QoS: {userdata['qos']})\n"
        )
    else:
        sys.stderr.write(f"Connection failed with code {rc}\n")


def on_message(client, userdata, msg):
    sys.stdout.buffer.write(msg.payload)
    sys.stdout.buffer.flush()


def on_disconnect(client, userdata, rc):
    userdata["disconnected"] = rc
    if rc != 0:
        # Map common disconnect reasons to human-readable messages
        disconnect_messages = {
            mqtt.MQTT_ERR_CONN_LOST: "Broker connection lost",
            mqtt.MQTT_ERR_CONN_REFUSED: "Connection refused",
            mqtt.MQTT_ERR_NO_CONN: "No connection available",
        }
        message = disconnect_messages.get(rc, f"Unexpected disconnect (rc: {rc})")
        sys.stderr.write(f"{message}\n")


# New callback for message acknowledgments
def on_publish(client, userdata, mid):
    """Called when a message is acknowledged by the broker"""
    userdata["pending_count"] -= 1


def main():
    parser = argparse.ArgumentParser(description="MQTT Netcat-like Tool")
    parser.add_argument(
        "mode", choices=["listen", "connect"], help="Operation mode: listen or connect"
    )
    parser.add_argument("prefix", help="Topic prefix for communication")
    parser.add_argument("profiles_file", help="JSON file containing MQTT profiles")
    parser.add_argument("profile_name", help="Profile name to use from profiles file")

    # Performance parameters
    parser.add_argument(
        "--qos",
        type=int,
        choices=[0, 1, 2],
        default=DEFAULT_QOS,
        help=f"Quality of Service level (default: {DEFAULT_QOS})",
    )
    parser.add_argument(
        "--keepalive",
        type=int,
        default=DEFAULT_KEEPALIVE,
        help=f"Keepalive interval in seconds (default: {DEFAULT_KEEPALIVE})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Chunk size for reading stdin (bytes, default: {DEFAULT_CHUNK_SIZE})",
    )
    parser.add_argument(
        "--max-pending",
        type=int,
        default=DEFAULT_MAX_PENDING,
        help=f"Max pending acknowledgments before throttling (default: {DEFAULT_MAX_PENDING})",
    )
    parser.add_argument(
        "--qos0-delay",
        type=float,
        default=DEFAULT_QOS0_DELAY_MS,
        help=f"Delay between QoS 0 sends in milliseconds (default: {DEFAULT_QOS0_DELAY_MS} ms)",
    )

    args = parser.parse_args()

    # Validate and warn about chunk size
    if args.chunk_size < 256:
        sys.stderr.write("Warning: Small chunk sizes (<256B) may reduce performance\n")
    elif args.chunk_size > 1024 * 1024:
        sys.stderr.write("Warning: Large chunk sizes (>1MB) may cause buffer issues\n")

    # Validate max pending
    if args.max_pending < 1:
        sys.stderr.write("Max pending must be at least 1\n")
        sys.exit(1)

    # Convert milliseconds to seconds for internal use
    qos0_delay_seconds = args.qos0_delay / 1000.0

    # Validate QoS 0 delay
    if args.qos0_delay < 0:
        sys.stderr.write("QoS 0 delay must be non-negative\n")
        sys.exit(1)
    elif args.qos0_delay == 0:
        sys.stderr.write("Warning: Zero QoS 0 delay may overwhelm broker/receiver\n")
    elif args.qos0_delay > 100:  # 100ms
        sys.stderr.write("Warning: Large QoS 0 delay (>100ms) may reduce throughput\n")

    # Set topics based on mode
    if args.mode == "listen":
        subscribe_topic = f"{args.prefix}/connect"
        publish_topic = f"{args.prefix}/listen"
    else:  # connect mode
        subscribe_topic = f"{args.prefix}/listen"
        publish_topic = f"{args.prefix}/connect"

    # Load MQTT profile
    profiles = load_profiles(args.profiles_file)
    profile = profiles.get(args.profile_name)
    if not profile:
        sys.stderr.write(f"Profile '{args.profile_name}' not found\n")
        sys.exit(1)

    # Configure MQTT client with clean session
    userdata = {
        "subscribe_topic": subscribe_topic,
        "disconnected": None,
        "qos": args.qos,
        "pending_count": 0,  # Track unacknowledged messages
        "max_pending": args.max_pending,  # Max allowed in-flight messages
        "current_chunk": None,  # For storing unsent data during throttling
    }
    client = mqtt.Client(clean_session=True, userdata=userdata)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish  # For tracking message acknowledgments

    # Set credentials if available
    if "username" in profile and "password" in profile:
        client.username_pw_set(profile["username"].strip(), profile["password"].strip())

    # TLS Configuration
    tls_enabled = profile.get("tls", False)
    if tls_enabled:
        tls_args = {
            "ca_certs": profile.get("ca_certs"),
            "certfile": profile.get("certfile"),
            "keyfile": profile.get("keyfile"),
            "cert_reqs": ssl.CERT_REQUIRED,
        }

        # Allow self-signed certificates if requested
        if profile.get("insecure", False):
            tls_args["cert_reqs"] = ssl.CERT_NONE

        # Remove None values from tls_args
        tls_args = {k: v for k, v in tls_args.items() if v is not None}

        try:
            client.tls_set(**tls_args)
            client.tls_insecure_set(profile.get("insecure", False))
        except Exception as e:
            sys.stderr.write(f"TLS setup error: {str(e)}\n")
            sys.exit(1)

    # Connect to broker with improved error handling
    try:
        client.connect(profile["host"], int(profile["port"]), args.keepalive)
    except ConnectionRefusedError:
        sys.stderr.write("Connection refused. Check broker availability and port.\n")
        sys.exit(1)
    except ssl.SSLError as e:
        sys.stderr.write(f"TLS handshake failed: {str(e)}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Connection error: {str(e)}\n")
        sys.exit(1)

    # Start MQTT loop in background thread
    client.loop_start()

    # Signal handling
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        sys.stderr.write("\nDisconnecting...\n")
        running = False
        client.disconnect()
        client.loop_stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Throttling variables
    qos0_delay = qos0_delay_seconds  # Use user-specified delay in seconds
    last_send_time = 0

    # Read from stdin using non-blocking I/O with throttling
    try:
        while running:
            # Check if we got an unexpected disconnect
            if userdata["disconnected"] is not None:
                if userdata["disconnected"] != 0:
                    sys.stderr.write("Disconnected from broker, exiting.\n")
                    break
                else:  # Normal disconnect
                    break

            # Throttling logic
            credit_available = True
            if userdata["qos"] > 0:
                # For QoS 1/2: Check pending acknowledgments
                if userdata["pending_count"] >= userdata["max_pending"]:
                    credit_available = False
            else:
                # For QoS 0: Rate limit sends using specified delay
                current_time = time.monotonic()
                if current_time - last_send_time < qos0_delay:
                    credit_available = False
                else:
                    last_send_time = current_time

            # Check if there's data available to read
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)

            if rlist and credit_available:
                data = sys.stdin.buffer.read1(args.chunk_size)
                if not data:  # EOF
                    break
                result = client.publish(publish_topic, data, qos=args.qos)

                # Track pending messages for QoS 1/2
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    if args.qos > 0:
                        userdata["pending_count"] += 1
                else:
                    sys.stderr.write(f"Publish failed (rc: {result.rc}), retrying...\n")
                    # Store unsent data for retry
                    userdata["current_chunk"] = data

            # Retry sending any failed chunks when credit available
            elif userdata["current_chunk"] and credit_available:
                data = userdata["current_chunk"]
                result = client.publish(publish_topic, data, qos=args.qos)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    if args.qos > 0:
                        userdata["pending_count"] += 1
                    userdata["current_chunk"] = None  # Clear stored chunk
                else:
                    sys.stderr.write(f"Retry failed (rc: {result.rc}), will retry...\n")

            # Brief pause to prevent busy-waiting
            elif not credit_available:
                time.sleep(min(0.01, qos0_delay))  # Sleep proportional to delay setting

    except BrokenPipeError:
        pass
    except KeyboardInterrupt:
        pass
    except Exception as e:
        sys.stderr.write(f"Error: {str(e)}\n")
    finally:
        running = False
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    main()
