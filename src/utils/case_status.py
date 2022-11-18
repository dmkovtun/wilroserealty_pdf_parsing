from enum import Enum


class CaseStatus(Enum):

    dismissed = "DISMISSED"
    active = "ACTIVE"
    processing_failed = "PROCESSING_FAILED"
    not_processed = "NOT_PROCESSED"
