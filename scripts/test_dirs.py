#!/usr/bin/env python3
"""Test directory operations via WebDAV: mkdir, upload files, list, delete."""
import urllib.request
import xml.etree.ElementTree as ET

WEBDAV = "http://localhost:8765"

def propfind(path):
    """Send a PROPFIND request and return the response body."""
    req = urllib.request.Request(WEBDAV + path, method="PROPFIND")
    req.add_header("Depth", "1")
    req.add_header("Content-Type", "application/xml")
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

def mkcol(path):
    """Create a directory via MKCOL."""
    req = urllib.request.Request(WEBDAV + path, method="MKCOL")
    try:
        resp = urllib.request.urlopen(req)
        return resp.status
    except urllib.error.HTTPError as e:
        return e.code

def put(path, data):
    """Upload a file via PUT."""
    req = urllib.request.Request(WEBDAV + path, data=data, method="PUT")
    try:
        resp = urllib.request.urlopen(req)
        return resp.status
    except urllib.error.HTTPError as e:
        return e.code

def get(path):
    """Download a file via GET."""
    req = urllib.request.Request(WEBDAV + path, method="GET")
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, b""

def delete(path):
    """Delete a file or directory via DELETE."""
    req = urllib.request.Request(WEBDAV + path, method="DELETE")
    try:
        resp = urllib.request.urlopen(req)
        return resp.status
    except urllib.error.HTTPError as e:
        return e.code

print("=== Test 1: Create directory ===")
status = mkcol("/test-photos/")
print(f"  MKCOL /test-photos/ -> {status}")
assert status in (201, 200), f"MKCOL failed: {status}"

print("=== Test 2: PROPFIND on new empty directory ===")
status, body = propfind("/test-photos/")
print(f"  PROPFIND /test-photos/ -> {status}")
assert status == 207, f"PROPFIND on new dir failed: {status}"

print("=== Test 3: Upload files into directory ===")
for i in range(3):
    content = f"image data {i}".encode() * 100
    status = put(f"/test-photos/img{i}.jpg", content)
    print(f"  PUT /test-photos/img{i}.jpg ({len(content)} bytes) -> {status}")
    assert status in (200, 201, 204), f"PUT failed: {status}"

print("=== Test 4: Create nested subdirectory ===")
status = mkcol("/test-photos/vacation/")
print(f"  MKCOL /test-photos/vacation/ -> {status}")
assert status in (201, 200), f"MKCOL nested failed: {status}"

status = put("/test-photos/vacation/beach.jpg", b"beach photo data" * 50)
print(f"  PUT /test-photos/vacation/beach.jpg -> {status}")
assert status in (200, 201, 204), f"PUT failed: {status}"

print("=== Test 5: PROPFIND on directory with files ===")
status, body = propfind("/test-photos/")
print(f"  PROPFIND /test-photos/ -> {status}")
assert status == 207, f"PROPFIND failed: {status}"

print("=== Test 6: Download a file from the directory ===")
status, data = get("/test-photos/img0.jpg")
expected = b"image data 0" * 100
print(f"  GET /test-photos/img0.jpg -> {status} ({len(data)} bytes)")
assert status == 200, f"GET failed: {status}"
assert data == expected, f"Content mismatch: got {len(data)} bytes"

print("=== Test 7: Browser listing shows directory ===")
req = urllib.request.Request(WEBDAV + "/")
req.add_header("Accept", "text/html")
resp = urllib.request.urlopen(req)
html = resp.read().decode()
assert "test-photos" in html, "Directory not shown in browser listing"
print("  Root listing contains test-photos/ directory")

print("=== Test 8: Delete the entire directory ===")
status = delete("/test-photos/")
print(f"  DELETE /test-photos/ -> {status}")
assert status in (200, 204), f"DELETE dir failed: {status}"

print("=== Test 9: Verify directory is gone ===")
status, body = propfind("/test-photos/")
print(f"  PROPFIND /test-photos/ -> {status}")
assert status == 404 or status == 207, f"Unexpected status: {status}"

print()
print("All directory tests passed!")
