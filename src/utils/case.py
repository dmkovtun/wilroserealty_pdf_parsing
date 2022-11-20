from .case_status import CaseStatus


class Case:
    case_row_number: int
    case_number: str
    url_case_link: str
    url_attorney: str
    url_petition: str
    url_schedule_a_b: str
    url_schedule_d: str
    url_schedule_e_f: str
    url_top_twenty: str
    # Enrichable values
    case_status: CaseStatus
    files: dict
    # cookies: dict

    enrichable_values: dict

    # TODO add case numbers I suppose

    def __init__(
        self,
        case_row_number,
        case_number,
        url_case_link,
        url_attorney,
        url_petition,
        url_schedule_a_b,
        url_schedule_d,
        url_schedule_e_f,
        url_top_twenty,
    ) -> None:
        self.case_row_number = case_row_number
        self.case_number = case_number
        self.url_case_link = url_case_link
        self.url_attorney = url_attorney
        self.url_petition = url_petition
        self.url_schedule_a_b = url_schedule_a_b
        self.url_schedule_d = url_schedule_d
        self.url_schedule_e_f = url_schedule_e_f
        self.url_top_twenty = url_top_twenty

        # NOTE: initialization of these fields is required
        self.files = {}
        self.case_status = CaseStatus.not_processed
        self.enrichable_values = {}