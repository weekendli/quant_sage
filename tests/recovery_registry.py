"""Fault injection for process loss after artifact publication, before ledger success."""

import os
from pathlib import Path

from quantsage_discovery.core import Registry, StageStatus


class ExitAfterPublicationRegistry(Registry):
    def __init__(self, path, marker):
        super().__init__(path)
        self.marker = Path(marker)

    def set_stage_complete(self, stage_run_id, status, **kwargs):
        if status == StageStatus.SUCCEEDED and not self.marker.exists():
            self.marker.write_text("published before ledger success", encoding="utf-8")
            os._exit(24)
        return super().set_stage_complete(stage_run_id, status, **kwargs)
