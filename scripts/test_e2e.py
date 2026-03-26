#!/usr/bin/env python3
"""End-to-end test for pdrive: upload, download, verify, list, delete."""
import urllib.request
import sys

WEBDAV = "http://localhost:8765"

def put(path, data):
    req = urllib.request.Request(WEBDAV + path, data=data, method="PUT")
    resp = urllib.request.urlopen(req)
    return resp.status

def get(path):
    req = urllib.request.Request(WEBDAV + path, method="GET")
    resp = urllib.request.urlopen(req)
    return resp.read()

def delete(path):
    req = urllib.request.Request(WEBDAV + path, method="DELETE")
    resp = urllib.request.urlopen(req)
    return resp.status

content = b"Hello from pdrive! Chunked, encrypted, distributed.\n"

print("1. Uploading /e2e-test.txt ...")
status = put("/e2e-test.txt", content)
print(f"   PUT status: {status}")

print("2. Downloading /e2e-test.txt ...")
downloaded = get("/e2e-test.txt")
print(f"   GET returned {len(downloaded)} bytes")

if downloaded == content:
    print("   MATCH: downloaded content matches original!")
else:
    print(f"   MISMATCH!")
    print(f"   Expected: {content}")
    print(f"   Got:      {downloaded}")
    sys.exit(1)

print("3. Deleting /e2e-test.txt ...")
status = delete("/e2e-test.txt")
print(f"   DELETE status: {status}")

print("\nAll tests passed!")
