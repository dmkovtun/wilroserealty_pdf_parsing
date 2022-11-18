from hashlib import sha256


def get_url_hash(url: str):
    """
    Returns SHA-256 hash from passed string
    """
    image_url_hash = sha256(url.encode()).hexdigest()
    return image_url_hash
