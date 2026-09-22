"""Service entrypoint.

Startup is unchanged from the original skeleton:

    uvicorn main:app --reload

Configuration is read from environment variables (see elreg/config.py and
README); every new variable has a documented default.
"""

from __future__ import annotations

import atexit

from fastapi import FastAPI

from elreg.api import create_app
from elreg.config import Settings, load_settings
from elreg.logging_setup import configure_logging
from elreg.service import RegistryService

settings: Settings = load_settings()
configure_logging(settings.log_level)

service = RegistryService(settings)
service.start()
atexit.register(service.close)

app: FastAPI = create_app(service)
