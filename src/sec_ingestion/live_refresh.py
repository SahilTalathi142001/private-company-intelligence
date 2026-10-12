"""Compatibility entry point for the SEC live-refresh controller."""

from company_platform.sec_detection.live_refresh import (
    SEC_SUBMISSIONS,
    VERSION,
    SECLiveRefresh,
    main,
)

__all__ = [
    "SEC_SUBMISSIONS",
    "VERSION",
    "SECLiveRefresh",
    "main",
]


if __name__ == "__main__":
    main()
