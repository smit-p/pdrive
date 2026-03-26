#!/usr/bin/env python3
"""Test that browser GET requests return HTML directory listings."""
import urllib.request

WEBDAV = "http://localhost:8765"

# Upload a file so directory listing isn't empty
content = b"Hello pdrive browser test!\n"
req = urllib.request.Request(WEBDAV + "/hello.txt", data=content, method="PUT")
urllib.request.urlopen(req)
print("uploaded hello.txt")

# Request root like a browser (with text/html Accept header)
req = urllib.request.Request(WEBDAV + "/")
req.add_header("Accept", "text/html,application/xhtml+xml")
resp = urllib.request.urlopen(req)
body = resp.read().decode()
print(f"Status: {resp.status}")
print(f"Content-Type: {resp.headers['Content-Type']}")
print()
print(body[:800])
print("...")
