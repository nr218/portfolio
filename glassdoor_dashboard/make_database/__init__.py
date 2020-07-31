from pkg_resources import get_distribution, DistributionNotFound

try:  # pragma: no cover
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    pass
