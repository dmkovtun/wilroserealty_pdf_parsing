from enum import Enum


class CaseEnrichmentStatus(Enum):

    processing_failed = "PROCESSING_FAILED"
    not_processed = "NOT_PROCESSED"
    possible_failure = "POSSIBLE FAILURE"
    success = "SUCCESS"
