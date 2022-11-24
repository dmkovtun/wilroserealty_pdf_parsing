from os.path import exists, join
from os import makedirs

from utils.get_url_hash import get_url_hash
from scrapy.utils.project import get_project_settings


file_field_type_mapping = {
    "url_attorney": "csv",
    # TODO url_petition
    "url_schedule_a_b": "pdf",
    "url_schedule_d": "pdf",
    # TODO (not required right now)
    # "url_schedule_e_f": "pdf",
    # "url_top_twenty": "pdf",
}


settings = get_project_settings()

def get_full_filename(case, file_field):
    file_storage: str = str(settings.get("TEMP_DIR_PATH"))

    full_file_type_dirname = join(file_storage, file_field)
    if not exists(full_file_type_dirname):
        makedirs(full_file_type_dirname)

    _filename = "".join(letter for letter in getattr(case, file_field) if letter.isalnum())
    filename = _filename.split(" ", maxsplit=1)[-1]
    filename = get_url_hash(filename)
    full_path = join(full_file_type_dirname, str(case.case_number).replace(':', '-') + "_" + file_field)
    file_type = file_field_type_mapping[file_field]
    return f"{full_path}.{file_type}"
