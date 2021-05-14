from urllib.request import getproxies
from typing import Dict


def get_proxies() -> Dict[str, str]:
    proxies = getproxies()

    return {k: v.split('//')[1] for k, v in proxies.items()}
