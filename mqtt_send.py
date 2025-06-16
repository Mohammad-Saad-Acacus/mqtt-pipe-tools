#!/usr/bin/env python3
import argparse
import json
import ssl  # Import ssl module for TLS support
import sys
import time  # For short delay after publish

import paho.mqtt.client as mqtt


def load_profiles(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="MQTT Pipe Sender")
    parser.add_argument("topic", help="MQTT topic to publish to")
    parser.add_argument("profiles_file", help="JSON file containing MQTT profiles")
    parser.add_argument("profile_name", help="Profile name to use from profiles file")
    args = parser.parse_args()

    profiles = load_profiles(args.profiles_file)
    profile = profiles.get(args.profile_name)

    if not profile:
        sys.stderr.write(f"Profile '{args.profile_name}' not found\n")
        sys.exit(1)

    keepalive = profile.get("keepalive", 60)
    client = mqtt.Client()

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

    if "username" in profile and "password" in profile:
        client.username_pw_set(profile["username"].strip(), profile["password"].strip())

    try:
        client.connect(profile["host"], int(profile["port"]), keepalive)
        client.loop_start()

        line_count = 0
        for line in sys.stdin.buffer:  # Use binary mode for raw data
            result = client.publish(args.topic, line, qos=1)
            # Add a small delay to ensure message delivery
            time.sleep(0.01)
            line_count += 1

        # Wait for all messages to be delivered
        time.sleep(1)
        client.loop_stop()
        client.disconnect()
        sys.stderr.write(f"Sent {line_count} messages\n")
    except Exception as e:
        sys.stderr.write(f"MQTT error: {str(e)}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
