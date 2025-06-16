#!/usr/bin/env python3
import argparse
import json
import signal
import ssl  # Import ssl module for TLS support
import sys

import paho.mqtt.client as mqtt


def load_profiles(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(userdata["topic"], qos=1)
        sys.stderr.write(f"Subscribed to {userdata['topic']}\n")
    else:
        sys.stderr.write(f"Connection failed with code {rc}\n")


def on_message(client, userdata, msg):
    sys.stdout.buffer.write(msg.payload)
    sys.stdout.buffer.flush()


def on_disconnect(client, userdata, rc):
    if rc != 0:
        sys.stderr.write(f"Unexpected disconnect (rc: {rc})\n")


def main():
    parser = argparse.ArgumentParser(description="MQTT Pipe Listener")
    parser.add_argument("topic", help="MQTT topic to subscribe to")
    parser.add_argument("profiles_file", help="JSON file containing MQTT profiles")
    parser.add_argument("profile_name", help="Profile name to use from profiles file")
    args = parser.parse_args()

    profiles = load_profiles(args.profiles_file)
    profile = profiles.get(args.profile_name)

    if not profile:
        sys.stderr.write(f"Profile '{args.profile_name}' not found\n")
        sys.exit(1)

    keepalive = profile.get("keepalive", 60)
    client = mqtt.Client(userdata={"topic": args.topic})
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

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
            "tls_version": ssl.PROTOCOL_TLS_CLIENT,
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

    try:
        client.connect(profile["host"], int(profile["port"]), keepalive)
    except Exception as e:
        sys.stderr.write(f"Connection error: {str(e)}\n")
        sys.exit(1)

    def signal_handler(sig, frame):
        sys.stderr.write("\nDisconnecting...\n")
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    client.loop_forever()


if __name__ == "__main__":
    main()
