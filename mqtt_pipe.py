#!/usr/bin/env python3
import argparse
import json
import select
import signal
import ssl
import sys
import threading

import paho.mqtt.client as mqtt

CHUNK_SIZE = 1024 * 64


def load_profiles(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(userdata["subscribe_topic"], qos=0)
        sys.stderr.write(f"Connected to broker, subscribed to {userdata['subscribe_topic']}\n")
    else:
        sys.stderr.write(f"Connection failed with code {rc}\n")


def on_message(client, userdata, msg):
    sys.stdout.buffer.write(msg.payload)
    sys.stdout.buffer.flush()


def on_disconnect(client, userdata, rc):
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
    args = parser.parse_args()

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

    # Configure MQTT client
    keepalive = profile.get("keepalive", 60)
    client = mqtt.Client(userdata={"subscribe_topic": subscribe_topic})
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
        client.connect(profile["host"], int(profile["port"]), keepalive)
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

    # Read from stdin using non-blocking I/O
    try:
        while running:
            # Check if there's data available to read
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)

            if rlist:
                data = sys.stdin.buffer.read1(CHUNK_SIZE)  # Use read1 for partial reads
                if not data:  # EOF
                    break
                result = client.publish(publish_topic, data, qos=0)
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
