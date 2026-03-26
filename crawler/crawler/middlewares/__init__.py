"""
Middlewares Package
"""

from crawler.middlewares.drission_middleware import DrissionPageMiddleware
from crawler.middlewares.captcha_middleware import CaptchaDetectionMiddleware

__all__ = [
    'DrissionPageMiddleware',
    'CaptchaDetectionMiddleware',
]
