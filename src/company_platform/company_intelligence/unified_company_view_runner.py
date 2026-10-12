from __future__ import annotations

import argparse
from pathlib import Path

from company_platform.company_intelligence.unified_company_view import (
    UnifiedCompanyViewBuilder,
)


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Build the Unified Company View "
            "from financial, investment and "
            "event-driven intelligence."
        )
    )

    parser.add_argument(
        "--step16-profile",
        type=Path,
    )

    parser.add_argument(
        "--step20-profile",
        type=Path,
    )

    parser.add_argument(
        "--company-master",
        type=Path,
    )

    args = parser.parse_args()

    builder = (
        UnifiedCompanyViewBuilder()
    )

    kwargs = {}

    if args.step16_profile:
        kwargs["step16_path"] = (
            args.step16_profile
        )

    if args.step20_profile:
        kwargs["step20_path"] = (
            args.step20_profile
        )

    if args.company_master:
        kwargs["company_master_path"] = (
            args.company_master
        )

    frame, output = (
        builder.build_and_write(
            **kwargs
        )
    )

    print(
        "UNIFIED COMPANY VIEW"
    )
    print(
        f"Rows       : {len(frame)}"
    )
    print(
        f"Companies  : {frame['cik'].nunique()}"
    )
    print(
        f"Output     : {output}"
    )


if __name__ == "__main__":
    main()