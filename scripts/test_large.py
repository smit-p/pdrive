#!/usr/bin/env python3
"""Test streaming upload with a file larger than one 4MB chunk."""
import urllib.request
import os

WEBDAV = "http://localhost:8765"

# Create a 12 MB test payload (3 chunks worth).
size = 12 * 1024 * 1024
data = os.urandom(size)
print(f"Uploading {size / (1024*1024):.0f} MB test file...")

req = urllib.request.Request(WEBDAV + "/big-test.bin", data=data, method="PUT")
resp = urllib.request.urlopen(req)
print(f"  PUT -> {resp.status}")

print("Downloading and verifying...")
req = urllib.request.Request(WEBDAV + "/big-test.bin", method="GET")
resp = urllib.request.urlopen(req)
result = resp.read()
print(f"  GET -> {resp.status} ({len(result)} bytes)")

assert result == data, f"Content mismatch! Got {len(result)} bytes, expected {len(data)}"
print("  Content verified!")

req = urllib.request.Request(WEBDAV + "/big-test.bin", method="DELETE")
resp = urllib.request.urlopen(req)
print(f"  DELETE -> {resp.status}")
print("Multi-chunk streaming test passed!")
