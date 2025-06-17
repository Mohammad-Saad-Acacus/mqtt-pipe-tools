#!/usr/bin/env python3
import argparse
import json
import os
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
DEFAULT_HANDSHAKE_TIMEOUT = 5
HELLO_PREFIX = b"HELLO:"
HELLO_ACK_PREFIX = b"HELLO_ACK:"


def load_profiles(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)


def generate_short_id():
    """Generate a short 4-byte (8-character) hex ID"""
    return os.urandom(4).hex()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(userdata["subscribe_topic"], qos=userdata["qos"])
        sys.stderr.write(
            f"Connected to broker, subscribed to {userdata['subscribe_topic']} (QoS: {userdata['qos']})\n"
        )
    else:
        sys.stderr.write(f"Connection failed with code {rc}\n")


def on_message(client, userdata, msg):
    # If we're waiting for a handshake acknowledgment
    if userdata.get("waiting_for_ack") and msg.payload.startswith(HELLO_ACK_PREFIX):
        ack_id = msg.payload[len(HELLO_ACK_PREFIX) :]
        if ack_id == userdata["session_id"]:
            userdata["handshake_complete"] = True
            userdata["waiting_for_ack"] = False
            sys.stderr.write("Handshake acknowledged. Starting data transfer.\n")
        return

    # If we're in listen mode and receive a hello message
    if userdata.get("expect_hello") and msg.payload.startswith(HELLO_PREFIX):
        session_id = msg.payload[len(HELLO_PREFIX) :]
        if userdata["handshake"]:
            # Respond with ACK containing the same session ID
            ack_msg = HELLO_ACK_PREFIX + session_id
            client.publish(userdata["publish_topic"], ack_msg, qos=userdata["qos"])
            userdata["expect_hello"] = False
            sys.stderr.write(
                f"Handshake received for session {session_id.decode()}. Acknowledging.\n"
            )
        return

    # Normal data message
    sys.stdout.buffer.write(msg.payload)
    sys.stdout.buffer.flush()


def on_disconnect(client, userdata, rc):
    userdata["disconnected"] = rc
    if rc != 0:
        sys.stderr.write(f"Unexpected disconnect (rc: {rc})\n")


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

    # Handshake parameters
    parser.add_argument(
        "--handshake",
        action="store_true",
        help="Enable handshake mechanism to verify listener presence",
    )
    parser.add_argument(
        "--hello-timeout",
        type=int,
        default=DEFAULT_HANDSHAKE_TIMEOUT,
        help=f"Timeout for handshake acknowledgment in seconds (default: {DEFAULT_HANDSHAKE_TIMEOUT})",
    )
    parser.add_argument(
        "--hello-interval",
        type=int,
        default=1,
        help="Interval between hello retries in seconds (default: 1)",
    )
    parser.add_argument(
        "--session-id",
        default="",
        help="Custom session ID for connection (default: random 8-char hex)",
    )

    args = parser.parse_args()

    # Validate chunk size
    if args.chunk_size <= 0:
        sys.stderr.write("Chunk size must be a positive integer\n")
        sys.exit(1)

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

    # Generate session ID
    if args.session_id:
        session_id = args.session_id.encode()
    else:
        # Generate short 8-character hex ID
        session_id = generate_short_id().encode()

    if args.mode == "connect":
        sys.stderr.write(f"Session ID: {session_id.decode()}\n")

    # Configure MQTT client
    userdata = {
        "subscribe_topic": subscribe_topic,
        "publish_topic": publish_topic,
        "disconnected": None,
        "qos": args.qos,
        "mode": args.mode,
        "handshake": args.handshake,
        "handshake_complete": not args.handshake,  # Start as complete if no handshake needed
        "waiting_for_ack": False,
        "expect_hello": args.mode == "listen" and args.handshake,
        "session_id": session_id,
    }

    client = mqtt.Client(userdata=userdata)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

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

    # Connect to broker
    try:
        client.connect(profile["host"], int(profile["port"]), args.keepalive)
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

    # For connect mode with handshake: send hello and wait for acknowledgment
    if args.mode == "connect" and args.handshake:
        sys.stderr.write("Sending handshake hello...\n")
        userdata["waiting_for_ack"] = True
        start_time = time.time()
        last_hello_time = 0
        hello_msg = HELLO_PREFIX + session_id

        while running and not userdata["handshake_complete"]:
            current_time = time.time()
            # Send hello at intervals
            if current_time - last_hello_time > args.hello_interval:
                client.publish(publish_topic, hello_msg, qos=args.qos)
                last_hello_time = current_time
                sys.stderr.write(
                    f"Sent hello (ID: {session_id.decode()}). Waiting for acknowledgment...\n"
                )

            # Check timeout
            if current_time - start_time > args.hello_timeout:
                sys.stderr.write("Handshake timeout. No listener detected.\n")
                running = False
                break

            time.sleep(0.1)

        if not running:
            sys.exit(1)

    # Read from stdin using non-blocking I/O
    try:
        while running:
            # Check if we got an unexpected disconnect
            if userdata["disconnected"] is not None and userdata["disconnected"] != 0:
                sys.stderr.write("Disconnected from broker, exiting.\n")
                break

            # Check if there's data available to read
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)

            if rlist:
                data = sys.stdin.buffer.read1(args.chunk_size)  # Use read1 for partial reads
                if not data:  # EOF
                    break
                result = client.publish(publish_topic, data, qos=args.qos)
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
