from usaddress import tag, RepeatedLabelError
import logging

logger = logging.getLogger(__name__.split(".")[-1])


def get_parsed_address(line_with_address: str) -> str:
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
