import logging
import re

from usaddress import RepeatedLabelError, tag

logger = logging.getLogger(__name__.split(".")[-1])


def _substring_from_address_text(line_with_address: str) -> list:
    tmp = line_with_address.replace(",", " ")
    line_with_address = " ".join(list(dict.fromkeys(tmp.split(" "))))

    new_line = line_with_address
    line_with_address = line_with_address.replace("\n", " ")

    all_parts = []
    _split_by = [" located at ", "-", "(", ")", ":"]
    for _part in _split_by:
        _splt = new_line.split(_part)
        if len(_splt) > 1:
            all_parts.extend(_splt)

    # NOTE: Remove duplicate words
    new_line = new_line.replace(",", " ")
    new_line = " ".join(list(dict.fromkeys(new_line.split(" "))))
    new_line = new_line.strip()

    # Try to match these regexes
    # TODO optimize code?
    pattern = re.compile(r"(.+\d{5,6}).+apartment units and approx", re.IGNORECASE)
    is_found = re.search(pattern, new_line)
    if is_found:
        return list(is_found.groups())

    pattern = re.compile(r"(.+\d{5,6}).+Value is an estimate", re.IGNORECASE)
    is_found = re.search(pattern, new_line)
    if is_found:
        return list(is_found.groups())

    pattern = re.compile(r"(.+\d{5,6}).+\d{1,3}.+acres", re.IGNORECASE)
    is_found = re.search(pattern, new_line)
    if is_found:
        return list(is_found.groups())

    if all_parts:
        return all_parts

    return [new_line.strip()]


def _fix_artifacts(line: str) -> str:
    line = line.strip()
    if line == "PIN":
        return ""
    if line.endswith("#"):
        return line[:-1]

    return line.strip()


def _parse_address(line: str) -> str:
    try:
        parsed, _ = tag(line)
        # Will not include these tags to final text
        skip_list = ["Recipient", "SubaddressType", "OccupancyIdentifier"]
        text_addr = " ".join([v for k, v in parsed.items() if k not in skip_list])
        # Add comma before place name
        try:
            place_name_value = parsed.get("PlaceName", "")
            if place_name_value:
                text_addr = text_addr.replace(place_name_value, f", {place_name_value}")
                text_addr = ", ".join([t.strip() for t in text_addr.split(",")])
        except Exception as err:
            logger.debug(f"Failed to add comma to address: {text_addr}, error: {str(err)}")

        return _fix_artifacts(text_addr)
    except RepeatedLabelError as err:
        logger.error(f"Failed to tag address: {line}, error: {str(err)[:128]}")
    return ""


def _is_valid_address(line: str) -> bool:
    _skip_list = ["See Attached Rider"]
    for p in _skip_list:
        if p in line:
            return False

    return True


def get_parsed_address(line_with_address: str) -> str:
    if not line_with_address.strip():
        return ""

    is_valid = _is_valid_address(line_with_address)
    if not is_valid:
        logger.debug(f"Address is not valid, will skip parsing: {line_with_address}")
        return ""
    processed_addr = _substring_from_address_text(line_with_address)

    lines = [_parse_address(p) for p in processed_addr]
    lines = [l for l in lines if l.strip()]

    # Will return most frequent text
    counts = {item: lines.count(item) for item in lines}
    max_frequency = max([v for _, v in counts.items()])
    most_frequent = [k for k, v in counts.items() if v == max_frequency][0]
    return most_frequent


if __name__ == "__main__":
    res = get_parsed_address("5755 Bayport Blvd. 5755 Bayport Blvd., Seabrook, TX 77586")
    print(res)
