#!/usr/bin/env python3
import argparse
import json
import select
import signal
import ssl
import sys
import time

import paho.mqtt.client as mqtt

# Default configuration
DEFAULTS = {
    "CHUNK_SIZE": 1024 * 64,
    "QOS": 0,
    "KEEPALIVE": 60,
    "MAX_PENDING": 10,
    "QOS0_DELAY_MS": 1,
}

# MQTT error mapping
MQTT_ERRORS = {
    mqtt.MQTT_ERR_CONN_LOST: "Broker connection lost",
    mqtt.MQTT_ERR_CONN_REFUSED: "Connection refused",
    mqtt.MQTT_ERR_NO_CONN: "No connection available",
}


def load_profiles(filename):
    """Load MQTT profiles from JSON file"""
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)


def setup_arg_parser():
    """Create and configure argument parser"""
    parser = argparse.ArgumentParser(description="MQTT Netcat-like Tool")

    # Positional arguments
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
        default=DEFAULTS["QOS"],
        help=f"Quality of Service level (default: {DEFAULTS['QOS']})",
    )
    parser.add_argument(
        "--keepalive",
        type=int,
        default=DEFAULTS["KEEPALIVE"],
        help=f"Keepalive interval in seconds (default: {DEFAULTS['KEEPALIVE']})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULTS["CHUNK_SIZE"],
        help=f"Chunk size for reading stdin (bytes, default: {DEFAULTS['CHUNK_SIZE']})",
    )
    parser.add_argument(
        "--max-pending",
        type=int,
        default=DEFAULTS["MAX_PENDING"],
        help=f"Max pending acknowledgments before throttling (default: {DEFAULTS['MAX_PENDING']})",
    )
    parser.add_argument(
        "--qos0-delay",
        type=float,
        default=DEFAULTS["QOS0_DELAY_MS"],
        help=f"Delay between QoS 0 sends in milliseconds (default: {DEFAULTS['QOS0_DELAY_MS']} ms)",
    )

    return parser


def validate_arguments(args):
    """Validate and warn about problematic argument values"""
    # Validate chunk size
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

    return qos0_delay_seconds


def get_topics(mode, prefix):
    """Determine publish/subscribe topics based on operation mode"""
    if mode == "listen":
        return {"subscribe": f"{prefix}/listen", "publish": f"{prefix}/connect"}
    return {"subscribe": f"{prefix}/connect", "publish": f"{prefix}/listen"}


def configure_tls(client, profile):
    """Configure TLS settings for MQTT client"""
    if not profile.get("tls", False):
        return

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


def on_connect(client, userdata, flags, rc):
    """Callback when connection to broker is established"""
    if rc == 0:
        client.subscribe(userdata["topics"]["subscribe"], qos=userdata["qos"])
        sys.stderr.write(
            f"Connected to broker, subscribed to {userdata['topics']['subscribe']} "
            f"(QoS: {userdata['qos']})\n"
        )
    else:
        sys.stderr.write(f"Connection failed with code {rc}\n")


def on_message(client, userdata, msg):
    """Callback for received messages"""
    sys.stdout.buffer.write(msg.payload)
    sys.stdout.buffer.flush()


def on_disconnect(client, userdata, rc):
    """Callback when disconnected from broker"""
    userdata["disconnected"] = rc
    if rc != 0:
        message = MQTT_ERRORS.get(rc, f"Unexpected disconnect (rc: {rc})")
        sys.stderr.write(f"{message}\n")


def on_publish(client, userdata, mid):
    """Callback when message is acknowledged by broker"""
    userdata["pending_count"] -= 1


def setup_mqtt_client(profile, args, topics, qos0_delay_seconds):
    """Create and configure MQTT client"""
    # Userdata for sharing state with callbacks
    userdata = {
        "topics": topics,
        "disconnected": None,
        "qos": args.qos,
        "pending_count": 0,
        "max_pending": args.max_pending,
        "current_chunk": None,
        "qos0_delay": qos0_delay_seconds,
    }

    client = mqtt.Client(clean_session=True, userdata=userdata)

    # Register callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    # Set credentials if available
    if "username" in profile and "password" in profile:
        client.username_pw_set(profile["username"].strip(), profile["password"].strip())

    # Configure TLS
    configure_tls(client, profile)

    # Connect to broker with error handling
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

    return client, userdata


def main_loop(client, userdata, chunk_size):
    """Main processing loop with throttling and I/O handling"""
    running = True
    last_send_time = 0

    # Signal handler for graceful shutdown
    def signal_handler(sig, frame):
        nonlocal running
        sys.stderr.write("\nDisconnecting...\n")
        running = False
        client.disconnect()
        client.loop_stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while running:
            # Check for disconnects
            if userdata["disconnected"] is not None:
                if userdata["disconnected"] != 0:
                    sys.stderr.write("Disconnected from broker, exiting.\n")
                break

            # Determine if we can send more data
            credit_available = True
            if userdata["qos"] > 0:
                # QoS 1/2: Check pending acknowledgments
                if userdata["pending_count"] >= userdata["max_pending"]:
                    credit_available = False
            else:
                # QoS 0: Rate limit using specified delay
                current_time = time.monotonic()
                if current_time - last_send_time < userdata["qos0_delay"]:
                    credit_available = False
                else:
                    last_send_time = current_time

            # Check for available data
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)

            if rlist and credit_available:
                # Read and send new data
                data = sys.stdin.buffer.read1(chunk_size)
                if not data:  # EOF
                    break
                send_data(client, userdata, data)

            # Retry any failed chunks
            elif userdata["current_chunk"] and credit_available:
                send_data(client, userdata, userdata["current_chunk"], retry=True)

            # Brief pause when throttled
            elif not credit_available:
                sleep_time = min(0.01, userdata["qos0_delay"])
                time.sleep(sleep_time)

    except BrokenPipeError:
        pass  # Output closed, normal termination
    except KeyboardInterrupt:
        pass  # User-initiated termination
    except Exception as e:
        sys.stderr.write(f"Runtime error: {str(e)}\n")
    finally:
        # Clean up resources
        client.disconnect()
        client.loop_stop()


def send_data(client, userdata, data, retry=False):
    """Publish data to MQTT broker with error handling"""
    topic = userdata["topics"]["publish"]
    qos = userdata["qos"]

    result = client.publish(topic, data, qos=qos)

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        if qos > 0:
            userdata["pending_count"] += 1
        if retry:
            userdata["current_chunk"] = None  # Clear stored chunk on successful retry
    else:
        error_msg = f"{'Retry' if retry else 'Publish'} failed (rc: {result.rc})"
        if not retry:
            error_msg += ", storing for retry"
            userdata["current_chunk"] = data
        sys.stderr.write(f"{error_msg}\n")


def main():
    """Main application entry point"""
    # Parse and validate arguments
    parser = setup_arg_parser()
    args = parser.parse_args()
    qos0_delay_seconds = validate_arguments(args)

    # Load MQTT profile
    profiles = load_profiles(args.profiles_file)
    profile = profiles.get(args.profile_name)
    if not profile:
        sys.stderr.write(f"Profile '{args.profile_name}' not found\n")
        sys.exit(1)

    # Determine topics
    topics = get_topics(args.mode, args.prefix)

    # Setup MQTT client
    client, userdata = setup_mqtt_client(profile, args, topics, qos0_delay_seconds)

    # Start MQTT network loop
    client.loop_start()

    # Run main processing loop
    main_loop(client, userdata, args.chunk_size)


if __name__ == "__main__":
    main()
