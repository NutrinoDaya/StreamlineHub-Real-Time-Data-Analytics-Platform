#!/usr/bin/env python3
"""
Module: app.py

This FastAPI application serves the Elasticsearch Metrics API.
Configuration (server, CORS, router prefixes, timeouts) is loaded from XML.
"""
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Adjust PYTHONPATH to include project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import routers
from pipeline.routers import (
    auth,
    layout,
    elastic_pre_processing_metrics,
    elastic_post_processing_metrics,
    elastic_traffic_metrics,
    elastic_traffic_metricsV2,
    vpc_metrics,
    messageHub,
    ax_router
)

# ------------------------------------------------------------------------------
# Read XML config into dict
# ------------------------------------------------------------------------------
def readConfig(cfg_path: Path) -> dict:
    tree = ET.parse(str(cfg_path))
    root = tree.getroot()
    def xml_to_dict(elem):
        if len(elem) == 0:
            return elem.text.strip() if elem.text else None
        return {child.tag: xml_to_dict(child) for child in elem}
    return xml_to_dict(root)

# Load application config
cfg_dir = Path(__file__).parent.parent / "config"
app_cfg = readConfig(cfg_dir / "App.xml")

# Server settings
srv = app_cfg.get("Server", {})
host = srv.get("host", "0.0.0.0")
port = int(srv.get("port", 8002))
reload_flag = str(srv.get("reload", False)).lower() == "true"
timeout_keep_alive = int(srv.get("timeout_keep_alive", 6000))
timeout_graceful_shutdown = int(srv.get("timeout_graceful_shutdown", 600))

# CORS settings
cors = app_cfg.get("CORS", {})
origins = cors.get("allow_origins", "*")
if isinstance(origins, str):
    origins = [o.strip() for o in origins.split(",")]
cred = str(cors.get("allow_credentials", False)).lower() == "true"
methods = cors.get("allow_methods", "*")
if isinstance(methods, str):
    methods = [m.strip() for m in methods.split(",")]
headers = cors.get("allow_headers", "*")
if isinstance(headers, str):
    headers = [h.strip() for h in headers.split(",")]

# Router prefixes
rt = app_cfg.get("Routers", {})
prefixes = {
    'auth': rt.get('auth_prefix', '/api'),
    'layout': rt.get('layout_prefix', '/api/LayoutList'),
    'pre': rt.get('pre_processing_prefix', '/IncidentPreProccessing'),
    'post': rt.get('post_processing_prefix', '/IPPTimeline'),
    'traffic': rt.get('traffic_prefix', '/Traffic'),
    'trafficV2': '/trafficV2',
    'vpc': rt.get('vpc_prefix', '/Vpc'),
    'hub': rt.get('messagehub_prefix', '/messageHub'),
    'ax': rt.get('ax_prefox', '/api/PLEntity'),
}

# ------------------------------------------------------------------------------
# Create FastAPI app
# ------------------------------------------------------------------------------
app = FastAPI(title=app_cfg.get("Title", "Elasticsearch Metrics API"))

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=cred,
    allow_methods=methods,
    allow_headers=headers,
)


# Include routers with configured prefixes
app.include_router(auth.router, prefix=prefixes['auth'])
app.include_router(layout.router, prefix=prefixes['layout'])
app.include_router(elastic_pre_processing_metrics.router, prefix=prefixes['pre'])
app.include_router(elastic_post_processing_metrics.router, prefix=prefixes['post'])
app.include_router(elastic_traffic_metrics.router, prefix=prefixes['traffic'])
app.include_router(elastic_traffic_metricsV2.router, prefix=prefixes['trafficV2'])
app.include_router(vpc_metrics.router, prefix=prefixes['vpc'])
app.include_router(messageHub.router, prefix=prefixes['hub'])
app.include_router(ax_router, prefix=prefixes['ax'])

# ------------------------------------------------------------------------------
# Main (when run directly)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=reload_flag,
        timeout_keep_alive=timeout_keep_alive,
        timeout_graceful_shutdown=timeout_graceful_shutdown,
    )
