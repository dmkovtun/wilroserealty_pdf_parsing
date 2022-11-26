from usaddress import tag, RepeatedLabelError
import logging

logger = logging.getLogger(__name__.split(".")[-1])
import re

# TODO AVINOR (K184451) FY21 - 28308
# TODO MENGUS (FS1 HYRA) - 28640

# TODO
# Single Family Residence 6501 Brad Drive Huntington Beach, CA 92642 Value is an estimate from Ticor Online Pro
# Single Family Residence 770 Paularnio Ave Costa Mesa, CA 92626 Value is an estimate from Appraisal
# Single Family Residence 778 Paularino Ave Costa Mesa, CA 92626 Value is an estimate from Ticor Online Pro
# 366 Gin Lane Southampton, NY 11968 (This is a residential single-family guest house with its own legal description deed, but which is part of a four-acre residential ocean-front estate property compound consisting of a mansion as the main house) fee simple;
# 022-23-00 1701 20th St W, Rosamond, CA 93560 16 acres with sfh and clubhouse. Loan is cross-collateralized with 830-50 Palm Canyon Dr. Borrego Springs, CA and 818 Pal Canyon Dr. Borrego Springs, CA Value based on development Value with approved improvments



def _substring_from_address_text(line_with_address: str) -> list:
    tmp = line_with_address.replace(",", " ")
    line_with_address = " ".join(list(dict.fromkeys(tmp.split(" "))))

    new_line = line_with_address
    line_with_address = line_with_address.replace("\n", " ")

    all_parts = []
    _split_by = [" located at ", "-", "(", ")", ':']
    for _part in _split_by:
        _splt = new_line.split(_part)
        if len(_splt) > 1:
            all_parts.extend(_splt)
    if all_parts:
        return all_parts

    # NOTE: Remove duplicate words
    new_line = new_line.replace(",", " ")
    new_line = " ".join(list(dict.fromkeys(new_line.split(" "))))
    new_line = new_line.strip()

    pattern = re.compile(r"(.+\d{5,6}).+apartment units and approx")
    is_found = re.search(pattern, new_line)
    if is_found:
        return list(is_found.groups())

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


def _is_valid_address(line:str) -> bool:
    _skip_list = ['See Attached Rider']
    for p in _skip_list:
        if p in line:
            return False

    return True

def get_parsed_address(line_with_address: str) -> str:
    is_valid = _is_valid_address(line_with_address)
    if not is_valid:
        logger.debug(f'Address is not valid, will skip parsing: {line_with_address}')
        return ''
    processed_addr = _substring_from_address_text(line_with_address)
    # TODO parse address like '5755 Bayport Blvd. 5755 Bayport Blvd., Seabrook, TX 77586'
    lines = [_parse_address(p) for p in processed_addr]
    return max(lines)


if __name__ == "__main__":
    tmp = "Real Estate and improvements located at 175 NE 55th Street, Miami, FL 33137- valuation is an appraisal estimated aggregate number for 14"
    # TODO
    # A/B Schedule A/B Assets Debtor 1300U SPE, LLC Name; 47,300 square foot office building located at 1300 U Street, 1330 U Street and 1329 V Street, Sacramento, CA 95814 - APNs 009-0144-001-0000, 009-0144-002-0000, 009-0144-003-0000. The Debtor values the property at $10,250,000 based upon an appraisal report dated July 22, 2022. The property is managed by Clippinger Investment Properties, Inc. for a 4% management fee with $2,500 base. The Debtor acquired the property for $9,850,000 on January 26, 2021. The Debtor financed the purchase of the property with a $6,122,000 loan from Fox Capital Mortgage Fund, LP. The building is currently vacant. However, since acquiring the property, the Debtor negotiated and entered into (on March 9, 2022) a 15-year lease with the California Highway Patrol which will commence on August 1, 2023.;
    tmp = "2019 Hillcrest Street, Mesquite, TX 75149 352 apartment units and approx. 18.3 acres of land"
    tmp = 'See Attached Rider'
    res = get_parsed_address(tmp)

    print(res)
