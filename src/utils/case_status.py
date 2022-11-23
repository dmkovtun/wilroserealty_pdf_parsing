from enum import Enum


class CaseStatus(Enum):

    dismissed = "Dismissed"
    active = "Active"
    processing_failed = "PROCESSING_FAILED"
    not_processed = "NOT_PROCESSED"
    possible_failure = "POSSIBLE FAILURE"
