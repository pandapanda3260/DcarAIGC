"""Workbook input adapter for the shared account intake service.

Legacy schema readers keep the original offline import contract. Schema 22
submits durable preparation requests through the same service as the web API.
"""
from .account_intake import (HEADERS, DISPLAY_ID, PLATFORMS, STATUSES, CONTRACT,
                             _uid, _display, _input, import_account_summary)
