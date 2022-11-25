from usaddress import tag, RepeatedLabelError
import logging

logger = logging.getLogger(__name__.split(".")[-1])


# TODO AVINOR (K184451) FY21 - 28308
# TODO MENGUS (FS1 HYRA) - 28640

# TODO
# Single Family Residence 6501 Brad Drive Huntington Beach, CA 92642 Value is an estimate from Ticor Online Pro
# Single Family Residence 770 Paularnio Ave Costa Mesa, CA 92626 Value is an estimate from Appraisal
# Single Family Residence 778 Paularino Ave Costa Mesa, CA 92626 Value is an estimate from Ticor Online Pro
# 366 Gin Lane Southampton, NY 11968 (This is a residential single-family guest house with its own legal description deed, but which is part of a four-acre residential ocean-front estate property compound consisting of a mansion as the main house) fee simple;
# 022-23-00 1701 20th St W, Rosamond, CA 93560 16 acres with sfh and clubhouse. Loan is cross-collateralized with 830-50 Palm Canyon Dr. Borrego Springs, CA and 818 Pal Canyon Dr. Borrego Springs, CA Value based on development Value with approved improvments
def _substring_from_address_text(line_with_address: str) -> str:
    new_line = line_with_address
    line_with_address = line_with_address.replace('\n', ' ')

    parts = new_line.split(' located at ', maxsplit=1)
    if len(parts) > 1:
        new_line = str(parts[1])

    parts = new_line.split('-', maxsplit=1)
    if len(parts) > 1:
        new_line = str(parts[0]) if len(parts[0]) > len(parts[1]) else str(parts[1])

    # NOTE: Remove duplicate words
    new_line = new_line.replace(',', ' ')
    new_line = ' '.join(list(dict.fromkeys(new_line.split(' '))))

    return new_line.strip()

def get_parsed_address(line_with_address: str) -> str:
    line_with_address = _substring_from_address_text(line_with_address)
    # TODO parse address like '5755 Bayport Blvd. 5755 Bayport Blvd., Seabrook, TX 77586'
    try:
        parsed, _ = tag(line_with_address)
        # Will not include these tags to final text
        skip_list = ["Recipient", "SubaddressType", "OccupancyIdentifier"]
        text_addr = " ".join([v for k, v in parsed.items() if k not in skip_list])
        # Add comma before place name
        try:
            place_name_value = parsed.get('PlaceName', '')
            if place_name_value:
                text_addr = text_addr.replace(place_name_value, f", {place_name_value}")
                text_addr = ", ".join([t.strip() for t in text_addr.split(",")])
        except Exception as err:
            logger.error(f"Failed to add comma to address: {text_addr}, error: {str(err)}")
        return text_addr
    except RepeatedLabelError as err:
        logger.error(f"Failed to tag address: {line_with_address}, error: {str(err)}")

    return line_with_address


if __name__ == '__main__':
    add = '5755 Bayport Blvd. 5755 Bayport Blvd., Seabrook, TX 77586'
    add = add.replace(',', ' ')
    unique_words = ' '.join(list(dict.fromkeys(add.split(' '))))
    res = get_parsed_address(unique_words)
    from usaddress import parse
    #parsed = parse(unique_words)
    print(res)