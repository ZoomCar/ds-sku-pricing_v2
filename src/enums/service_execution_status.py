from enum import Enum


class ServiceExecutionStatus(Enum):
    STARTED = "STARTED"
    FINISHED = "FINISHED"
    ABORTED = "ABORTED"
    OTHER = "OTHER"
